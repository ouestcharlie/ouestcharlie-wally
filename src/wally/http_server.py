"""Wally media middleware — thumbnail and preview serving via the backend abstraction.

URL scheme:
  GET /thumbnails/{backend_name}/{partition}/thumbnails.avif
  GET /previews/{backend_name}/{partition}/{content_hash}.jpg

Implemented as a pure-ASGI middleware that wraps the MCP app.  All I/O
runs in the main asyncio event loop via the backend abstraction, which
makes it trivial to switch from local filesystem to a remote backend.
asyncio.Event deduplicates concurrent preview generation requests so
generation runs exactly once per cache miss.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any
from urllib.parse import unquote

from ouestcharlie_toolkit.schema import preview_jpeg_path

_log = logging.getLogger(__name__)


class MediaMiddleware:
    """ASGI middleware: handles /thumbnails/… and /previews/… in-process.


    All file access goes through the backend abstraction so the storage
    layer can be swapped (local → remote) without touching this class.
    """

    def __init__(
        self,
        app: Any,
        *,
        backend_config: dict,
        backend_name: str,
    ) -> None:
        from ouestcharlie_toolkit.backend import backend_from_config

        self._app = app
        self._backend = backend_from_config(backend_config)
        self._backend_name = backend_name
        # asyncio.Lock() is safe to construct without a running loop in Python ≥ 3.11.
        self._lock = asyncio.Lock()
        self._in_progress: dict[str, asyncio.Event] = {}

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = unquote(scope.get("path", ""))
            if path.startswith("/previews/"):
                await self._handle_preview(path, send)
                return
            if path.startswith("/thumbnails/"):
                await self._handle_thumbnail(path, send)
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
            await self._ensure_preview(partition, content_hash)

        try:
            data, _ = await self._backend.read(backend_path)
        except FileNotFoundError:
            _log.error("Preview not available after generation: %s", backend_path)
            await _send_error(send, 503)
            return

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
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout=120.0)
            return
        try:
            await _generate_preview(self._backend, partition, content_hash)
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
        # path = "/thumbnails/{backend_name}/{avif_path}"
        # where avif_path is the backend-relative path, e.g.:
        #   ".ouestcharlie/2024/Jul/thumbnails-Kf3QzA2_nBcR8xYvLm1P9w.avif"
        parts = path.lstrip("/").split("/", 2)
        if len(parts) < 3:
            await _send_error(send, 404)
            return
        _, url_backend, backend_path = parts
        if url_backend != self._backend_name:
            await _send_error(send, 404)
            return
        filename = backend_path.rsplit("/", 1)[-1]
        if not (
            filename.startswith("thumbnails-") or filename.startswith("previews-")
        ) or not filename.endswith(".avif"):
            await _send_error(send, 404)
            return
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


async def _send_error(send: Any, status: int) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b""})


async def _generate_preview(
    backend: Any,
    partition: str,
    content_hash: str,
) -> None:
    """Find the photo entry in the leaf manifest and generate its JPEG preview."""
    from ouestcharlie_toolkit.manifest import ManifestStore
    from ouestcharlie_toolkit.thumbnail_builder import generate_preview_jpeg

    manifest_store = ManifestStore(backend)

    leaf, _ = await manifest_store.read_leaf(partition)
    entry = next((e for e in leaf.photos if e.content_hash == content_hash), None)
    if entry is None:
        raise FileNotFoundError(
            f"Photo with content_hash={content_hash!r} not found in partition {partition!r}"
        )

    await generate_preview_jpeg(backend, partition, entry)
