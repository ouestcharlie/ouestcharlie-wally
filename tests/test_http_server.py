"""Tests for MediaMiddleware media serving."""

from __future__ import annotations

from pathlib import Path

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


@pytest.fixture()
def backend_root(tmp_path: Path) -> Path:
    partition = tmp_path / "2024" / "2024-07" / ".ouestcharlie"
    partition.mkdir(parents=True)
    (partition / "thumbnails.avif").write_bytes(FAKE_AVIF)
    return tmp_path


@pytest.mark.asyncio
async def test_thumbnail_served(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnails/{BACKEND_NAME}/2024/2024-07/thumbnails.avif")
    assert resp.status_code == 200
    assert resp.content == FAKE_AVIF
    assert resp.headers["content-type"] == "image/avif"


@pytest.mark.asyncio
async def test_thumbnail_wrong_backend_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/thumbnails/wronglib/2024/2024-07/thumbnails.avif")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_thumbnail_missing_file_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnails/{BACKEND_NAME}/2024/2024-08/thumbnails.avif")
    assert resp.status_code == 404
