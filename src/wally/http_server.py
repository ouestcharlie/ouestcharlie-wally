"""Wally preview middleware — on-demand JPEG preview generation and serving.

URL scheme:
  GET /previews/{backend_name}/{partition}/{content_hash}.jpg

Implemented as a pure-ASGI middleware that wraps the MCP app.  All I/O
runs in the main asyncio event loop.  asyncio.Event deduplicates concurrent
requests for the same photo so generation runs exactly once per cache miss.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from urllib.parse import unquote
from typing import Any

_log = logging.getLogger(__name__)


class PreviewMiddleware:
    """ASGI middleware: handles /previews/… in-process; passes everything else through.

    Sits between the outer auth guard and the MCP Starlette app so that
    preview requests bypass Bearer authentication (loopback-only, proxied
    by Woof which already holds the token).
    """

    def __init__(
        self,
        app: Any,
        *,
        backend_config: dict,
        backend_name: str,
    ) -> None:
        self._app = app
        self._backend_config = backend_config
        self._backend_name = backend_name
        self._backend_root = Path(backend_config["root"])
        # asyncio.Lock() is safe to construct without a running loop in Python ≥ 3.11.
        self._lock = asyncio.Lock()
        self._in_progress: dict[str, asyncio.Event] = {}

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = unquote(scope.get("path", ""))
            if path.startswith("/previews/"):
                await self._handle_preview(path, send)
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

        cache_path = (
            self._backend_root / partition / ".ouestcharlie" / "previews" / hash_file
        )

        if not cache_path.exists():
            await self._ensure_preview(partition, content_hash)

        if not cache_path.exists():
            _log.error("Preview not available after generation: %s", cache_path)
            await _send_error(send, 503)
            return

        data = await asyncio.to_thread(cache_path.read_bytes)
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"image/jpeg"),
                (b"content-length", str(len(data)).encode()),
                (b"access-control-allow-origin", b"*"),
            ],
        })
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
            try:
                await asyncio.wait_for(event.wait(), timeout=120.0)
            except asyncio.TimeoutError:
                pass
            return
        try:
            await _generate_preview(self._backend_config, partition, content_hash)
        except Exception as exc:
            _log.error(
                "Preview generation failed — partition=%r hash=%r: %s",
                partition, content_hash, exc, exc_info=True,
            )
        finally:
            async with self._lock:
                del self._in_progress[key]
            event.set()


async def _send_error(send: Any, status: int) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b""})


async def _generate_preview(
    backend_config: dict,
    partition: str,
    content_hash: str,
) -> None:
    """Find the photo entry in the leaf manifest and generate its JPEG preview."""
    from ouestcharlie_toolkit.backend import backend_from_config
    from ouestcharlie_toolkit.manifest import ManifestStore
    from ouestcharlie_toolkit.thumbnail_builder import generate_preview_jpeg

    backend = backend_from_config(backend_config)
    manifest_store = ManifestStore(backend)

    leaf, _ = await manifest_store.read_leaf(partition)
    entry = next(
        (e for e in leaf.photos if e.content_hash == content_hash), None
    )
    if entry is None:
        raise FileNotFoundError(
            f"Photo with content_hash={content_hash!r} not found in partition {partition!r}"
        )

    await generate_preview_jpeg(backend, partition, entry)
