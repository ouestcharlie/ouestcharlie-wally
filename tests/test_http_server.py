"""Tests for MediaMiddleware media serving."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from wally.http_server import MediaMiddleware

BACKEND_NAME = "testlib"
FAKE_AVIF = b"AVIF_FAKE_DATA"


def _make_app(backend_root: Path) -> MediaMiddleware:
    """Wrap a no-op ASGI app in MediaMiddleware pointed at backend_root."""

    async def _fallback(scope, receive, send):  # type: ignore[no-untyped-def]
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    return MediaMiddleware(
        _fallback,
        backend_config={"type": "filesystem", "root": str(backend_root)},
        backend_name=BACKEND_NAME,
    )


_CHUNK_HASH = "Kf3QzA2_nBcR8xYvLm1P9w"
_AVIF_FILENAME = f"thumbnails-{_CHUNK_HASH}.avif"


@pytest.fixture()
def backend_root(tmp_path: Path) -> Path:
    partition = tmp_path / "2024" / "2024-07" / ".ouestcharlie"
    partition.mkdir(parents=True)
    (partition / _AVIF_FILENAME).write_bytes(FAKE_AVIF)
    return tmp_path


@pytest.mark.asyncio
async def test_thumbnail_served(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            f"/thumbnails/{BACKEND_NAME}/2024/2024-07/.ouestcharlie/{_AVIF_FILENAME}"
        )
    assert resp.status_code == 200
    assert resp.content == FAKE_AVIF
    assert resp.headers["content-type"] == "image/avif"


@pytest.mark.asyncio
async def test_thumbnail_wrong_backend_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnails/wronglib/2024/2024-07/.ouestcharlie/{_AVIF_FILENAME}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_thumbnail_missing_file_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            f"/thumbnails/{BACKEND_NAME}/2024/2024-08/.ouestcharlie/{_AVIF_FILENAME}"
        )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Preview — PersistentImageProc integration
# ---------------------------------------------------------------------------

_CONTENT_HASH = "KfAbc123A2nBcR8xYvLm1P"
_PREVIEW_PATH = f".ouestcharlie/previews/{_CONTENT_HASH}.jpg"


@pytest.fixture()
def backend_with_preview(tmp_path: Path) -> Path:
    """Backend with a pre-cached preview JPEG."""
    preview_dir = tmp_path / ".ouestcharlie" / "previews"
    preview_dir.mkdir(parents=True)
    (preview_dir / f"{_CONTENT_HASH}.jpg").write_bytes(b"CACHED_PREVIEW_JPEG")
    return tmp_path


@pytest.mark.asyncio
async def test_preview_served_from_cache(backend_with_preview: Path) -> None:
    """A pre-cached preview is served directly without calling image-proc."""
    app = _make_app(backend_with_preview)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/previews/{BACKEND_NAME}//{_CONTENT_HASH}.jpg")
    assert resp.status_code == 200
    assert resp.content == b"CACHED_PREVIEW_JPEG"
    assert resp.headers["content-type"] == "image/jpeg"


@pytest.mark.asyncio
async def test_preview_wrong_backend_returns_404(backend_with_preview: Path) -> None:
    app = _make_app(backend_with_preview)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/previews/wronglib//{_CONTENT_HASH}.jpg")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_preview_generation_uses_persistent_image_proc(tmp_path: Path) -> None:
    """On a cache miss, _ensure_preview passes PersistentImageProc to generate_preview_jpeg."""
    app = _make_app(tmp_path)

    async def fake_generate(backend, partition, content_hash, image_proc):
        # Verify that image_proc is the middleware's instance.
        assert image_proc is app._image_proc
        # Write the cache file so the handler can read it back.
        cache = tmp_path / ".ouestcharlie" / "previews" / f"{content_hash}.jpg"
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(b"GENERATED_PREVIEW")

    with patch("wally.http_server._generate_preview", side_effect=fake_generate):
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/previews/{BACKEND_NAME}//{_CONTENT_HASH}.jpg")

    assert resp.status_code == 200
    assert resp.content == b"GENERATED_PREVIEW"


@pytest.mark.asyncio
async def test_media_middleware_close_shuts_down_image_proc(tmp_path: Path) -> None:
    """MediaMiddleware.close() delegates to PersistentImageProc.close()."""
    app = _make_app(tmp_path)
    app._image_proc.close = AsyncMock()
    await app.close()
    app._image_proc.close.assert_awaited_once()
