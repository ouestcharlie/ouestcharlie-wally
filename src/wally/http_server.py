"""Wally media middleware — thumbnail, preview and video serving via the backend abstraction.

URL scheme:
  GET /thumbnail/{backend_name}/{partition}/{avif_hash}
  GET /previews/{backend_name}/{partition}/{content_hash}.jpg
  GET /video/{backend_name}/{partition}/{content_hash}.{mp4,mov}

Implemented as a pure-ASGI middleware that wraps the MCP app.  All I/O
runs in the main asyncio event loop via the backend abstraction, which
makes it trivial to switch from local filesystem to a remote backend.
asyncio.Event deduplicates concurrent preview generation requests so
generation runs exactly once per cache miss.

Unlike thumbnails/previews (small, pre-generated, served whole-body), video
streams the *original* file straight from the backend and honours HTTP Range
requests (RFC 7233, single-range only) so ``<video>`` seeking works (OEC-39a
§1). The body is streamed in bounded chunks rather than buffered, so a
GB-scale file never lands in memory.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import Any
from urllib.parse import unquote

from ouestcharlie_imageproc.image_proc import PersistentImageProc
from ouestcharlie_toolkit import VIDEO_SUFFIXES, Backend
from ouestcharlie_toolkit.backend import backend_from_config
from ouestcharlie_toolkit.lance_index import PHOTO_TABLE_NAME, LanceIndex, _esc, row_to_photo_entry
from ouestcharlie_toolkit.preview_builder import generate_preview_jpeg
from ouestcharlie_toolkit.schema import PhotoEntry, preview_jpeg_path, thumbnail_avif_path

_log = logging.getLogger(__name__)

# Container suffix → MIME type. Keyed off VIDEO_SUFFIXES (OEC-39 §1) rather than
# mimetypes.guess_type(), which is unreliable cross-platform for ".mov". The bare
# container type is intentional — the codec-precise "codecs=" parameter is not
# built (OEC-39a §1); the browser probes the actual bytes for playability.
_VIDEO_CONTENT_TYPES = {".mp4": b"video/mp4", ".mov": b"video/quicktime"}

# Bounded read size for streaming video bodies (neither Wally nor the proxy ever
# holds a whole file in memory).
_VIDEO_CHUNK_BYTES = 512 * 1024


class MediaMiddleware:
    """ASGI middleware: handles /thumbnails/… and /previews/… in-process.

    All file access goes through the backend abstraction so the storage
    layer can be swapped (local → remote) without touching this class.

    A single :class:`PersistentImageProc` instance is kept alive for the
    lifetime of this middleware and reused across all preview requests,
    eliminating per-request subprocess startup overhead.
    """

    def __init__(
        self,
        app: Any,
        *,
        backend_config: dict,
        backend_name: str,
    ) -> None:
        self._app = app
        self._backend = backend_from_config(backend_config)
        self._backend_name = backend_name
        self._image_proc = PersistentImageProc()
        # asyncio.Lock() is safe to construct without a running loop in Python ≥ 3.11.
        self._lock = asyncio.Lock()
        self._in_progress: dict[str, asyncio.Event] = {}

    async def close(self) -> None:
        """Shut down the persistent image-proc process."""
        await self._image_proc.close()

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = unquote(scope.get("path", ""))
            if path.startswith("/previews/"):
                await self._handle_preview(path, send)
                return
            if path.startswith("/thumbnail/"):
                await self._handle_thumbnail(path, send)
                return
            if path.startswith("/video/"):
                await self._handle_video(path, scope, send)
                return
        await self._app(scope, receive, send)

    async def _handle_preview(self, path: str, send: Any) -> None:
        # path = "/previews/{backend_name}/{partition}/{content_hash}.jpg"
        parts = path.lstrip("/").split("/", 2)
        if len(parts) < 3:
            await _send_error(send, 404)
            return
        _, url_backend, rest = parts
        if url_backend != self._backend_name:
            await _send_error(send, 404)
            return
        rest_parts = rest.rsplit("/", 1)
        if len(rest_parts) != 2 or not rest_parts[1].endswith(".jpg"):
            await _send_error(send, 404)
            return
        partition, hash_file = rest_parts
        content_hash = hash_file[:-4]  # strip ".jpg"

        backend_path = preview_jpeg_path(partition, content_hash)

        if not await self._backend.exists(backend_path):
            _log.info(
                "Preview cache miss — generating: partition=%r hash=%r", partition, content_hash
            )
            await self._ensure_preview(partition, content_hash)
        else:
            _log.debug("Preview cache hit: partition=%r hash=%r", partition, content_hash)

        try:
            data, _ = await self._backend.read(backend_path)
        except FileNotFoundError:
            _log.error("Preview not available after generation: %s", backend_path)
            await _send_error(send, 503)
            return

        _log.info(
            "Serving preview: hash=%r size=%d bytes path=%s", content_hash, len(data), backend_path
        )
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"image/jpeg"),
                    (b"content-length", str(len(data)).encode()),
                    (b"access-control-allow-origin", b"*"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": data})

    async def _ensure_preview(self, partition: str, content_hash: str) -> None:
        """Generate and cache the preview JPEG, deduplicating concurrent requests."""
        key = f"{partition}:{content_hash}"
        async with self._lock:
            if key in self._in_progress:
                event = self._in_progress[key]
                wait = True
            else:
                event = asyncio.Event()
                self._in_progress[key] = event
                wait = False
        if wait:
            _log.debug("Preview already in progress, waiting: hash=%r", content_hash)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout=120.0)
            _log.debug("Preview wait complete: hash=%r", content_hash)
            return
        try:
            await _generate_preview(self._backend, partition, content_hash, self._image_proc)
        except Exception as exc:
            _log.error(
                "Preview generation failed — partition=%r hash=%r: %s",
                partition,
                content_hash,
                exc,
                exc_info=True,
            )
        finally:
            async with self._lock:
                del self._in_progress[key]
            event.set()

    async def _handle_thumbnail(self, path: str, send: Any) -> None:
        # path = "/thumbnail/{backend_name}/{partition}/{avif_hash}"
        # partition may contain slashes; avif_hash is the last segment.
        parts = path.lstrip("/").split("/", 2)
        if len(parts) < 3:
            await _send_error(send, 404)
            return
        _, url_backend, rest = parts
        if url_backend != self._backend_name:
            await _send_error(send, 404)
            return
        rest_parts = rest.rsplit("/", 1)
        if len(rest_parts) != 2:
            await _send_error(send, 404)
            return
        partition, avif_hash = rest_parts
        backend_path = thumbnail_avif_path(partition, avif_hash)
        try:
            data, _ = await self._backend.read(backend_path)
        except FileNotFoundError:
            await _send_error(send, 404)
            return
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"image/avif"),
                    (b"content-length", str(len(data)).encode()),
                    (b"access-control-allow-origin", b"*"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": data})

    async def _handle_video(self, path: str, scope: Any, send: Any) -> None:
        # path = "/video/{backend_name}/{partition}/{content_hash}.{ext}"
        # The URL extension is cosmetic (the frontend defaults it, OEC-39a §3);
        # the authoritative container comes from the resolved file's own suffix.
        parts = path.lstrip("/").split("/", 2)
        if len(parts) < 3:
            await _send_error(send, 404)
            return
        _, url_backend, rest = parts
        if url_backend != self._backend_name:
            await _send_error(send, 404)
            return
        rest_parts = rest.rsplit("/", 1)
        if len(rest_parts) != 2:
            await _send_error(send, 404)
            return
        partition, hash_file = rest_parts
        content_hash = os.path.splitext(hash_file)[0]

        try:
            entry = await _lookup_entry(self._backend, partition, content_hash)
        except FileNotFoundError:
            _log.info("Video not found in index: partition=%r hash=%r", partition, content_hash)
            await _send_error(send, 404)
            return

        ext = os.path.splitext(entry.filename)[1].lower()
        if ext not in VIDEO_SUFFIXES:
            _log.warning(
                "Video request for non-video entry: filename=%r hash=%r",
                entry.filename,
                content_hash,
            )
            await _send_error(send, 404)
            return
        content_type = _VIDEO_CONTENT_TYPES.get(ext, b"application/octet-stream")

        prefix = partition.rstrip("/") + "/" if partition else ""
        source_path = f"{prefix}{entry.filename}"
        try:
            local = await self._backend.local_path(source_path)
            file_size = await asyncio.to_thread(os.path.getsize, local)
        except OSError as exc:
            _log.error(
                "Video source unavailable — hash=%r path=%r: %s", content_hash, source_path, exc
            )
            await _send_error(send, 404)
            return

        range_header = _header_value(scope, b"range")
        span = _parse_range(range_header, file_size) if range_header else None

        if span == "unsatisfiable":
            await send(
                {
                    "type": "http.response.start",
                    "status": 416,
                    "headers": [
                        (b"content-range", f"bytes */{file_size}".encode()),
                        (b"access-control-allow-origin", b"*"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": b""})
            return

        if span is None:
            start, end, status = 0, file_size - 1, 200
        else:
            start, end = span
            status = 206

        length = end - start + 1
        headers = [
            (b"content-type", content_type),
            (b"content-length", str(length).encode()),
            (b"accept-ranges", b"bytes"),
            (b"access-control-allow-origin", b"*"),
        ]
        if status == 206:
            headers.append((b"content-range", f"bytes {start}-{end}/{file_size}".encode()))

        _log.info(
            "Serving video: hash=%r status=%d range=%s-%s/%d type=%s",
            content_hash,
            status,
            start,
            end,
            file_size,
            content_type.decode(),
        )
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await _stream_file(local, start, length, send)


async def _stream_file(local: Any, start: int, length: int, send: Any) -> None:
    """Stream ``length`` bytes from ``local`` starting at ``start`` in bounded chunks."""

    def _open() -> Any:
        handle = open(local, "rb")  # noqa: SIM115 — closed in the finally below
        handle.seek(start)
        return handle

    handle = await asyncio.to_thread(_open)
    try:
        remaining = length
        while remaining > 0:
            chunk = await asyncio.to_thread(handle.read, min(_VIDEO_CHUNK_BYTES, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})
    finally:
        await asyncio.to_thread(handle.close)


def _header_value(scope: Any, name: bytes) -> str | None:
    """Return a request header value (lowercased-name match) from the ASGI scope."""
    for key, value in scope.get("headers", []):
        if key == name:
            return value.decode("latin-1")
    return None


def _parse_range(header: str, size: int) -> tuple[int, int] | str | None:
    """Parse a single-range ``Range`` header against ``size``.

    Returns ``(start, end)`` inclusive for a satisfiable range, ``"unsatisfiable"``
    when the range falls outside the file (→ 416), or ``None`` when the header is
    absent/malformed/multi-range (caller serves the full body as 200). Only the
    single-range form is supported — browsers never request multipart ranges for
    ``<video>`` (OEC-39a §1).
    """
    if not header or not header.startswith("bytes=") or "," in header:
        return None
    spec = header[len("bytes=") :].strip()
    start_s, sep, end_s = spec.partition("-")
    if not sep:
        return None
    try:
        if not start_s:
            # Suffix form: bytes=-N → last N bytes.
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(0, size - suffix)
            return (start, size - 1)
        start = int(start_s)
        end = int(end_s) if end_s else size - 1
    except ValueError:
        return None
    if start >= size:
        return "unsatisfiable"
    if end < start:
        return None
    return (start, min(end, size - 1))


async def _send_error(send: Any, status: int) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b""})


async def _lookup_entry(backend: Backend, partition: str, content_hash: str) -> PhotoEntry:
    """Resolve a media entry from the LanceDB index by content hash + partition.

    Raises:
        FileNotFoundError: if no matching entry exists.
    """
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    query = f"content_hash = '{_esc(content_hash)}' AND partition = '{_esc(partition)}'"
    matches, _ = await lance_index.search_where(query, page_size=1)
    if not matches:
        raise FileNotFoundError(
            f"Media with content_hash={content_hash!r} not found in partition {partition!r}"
        )
    return row_to_photo_entry(matches[0])


async def _generate_preview(
    backend: Backend,
    partition: str,
    content_hash: str,
    image_proc: PersistentImageProc,
) -> None:
    """Look up the photo entry in the LanceDB index and generate its JPEG preview."""
    entry = await _lookup_entry(backend, partition, content_hash)
    _log.debug(
        "Generating preview: hash=%r filename=%r partition=%r",
        content_hash,
        entry.filename,
        partition,
    )
    await generate_preview_jpeg(image_proc, backend, partition, entry)
