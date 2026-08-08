"""Tests for MediaMiddleware media serving."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from ouestcharlie_toolkit.schema import PhotoEntry

from wally.http_server import MediaMiddleware, _generate_preview, _parse_range

BACKEND_NAME = "testlib"
FAKE_AVIF = b"AVIF_FAKE_DATA"


def _make_app(backend_root: Path) -> MediaMiddleware:
    """Wrap a no-op ASGI app in MediaMiddleware pointed at backend_root."""

    async def _fallback(scope, receive, send):  # type: ignore[no-untyped-def]
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    return MediaMiddleware(
        _fallback,
        backend_config={"type": "filesystem", "path": str(backend_root)},
        backend_name=BACKEND_NAME,
    )


_CHUNK_HASH = "Kf3QzA2_nBcR8xYvLm1P9w"
_AVIF_FILENAME = f"thumbnails-{_CHUNK_HASH}.avif"


@pytest.fixture()
def backend_root(tmp_path: Path) -> Path:
    avif_dir = tmp_path / ".ouestcharlie" / "2024" / "2024-07"
    avif_dir.mkdir(parents=True)
    (avif_dir / _AVIF_FILENAME).write_bytes(FAKE_AVIF)
    return tmp_path


@pytest.mark.asyncio
async def test_thumbnail_served(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnail/{BACKEND_NAME}/2024/2024-07/{_CHUNK_HASH}")
    assert resp.status_code == 200
    assert resp.content == FAKE_AVIF
    assert resp.headers["content-type"] == "image/avif"


@pytest.mark.asyncio
async def test_thumbnail_wrong_backend_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnail/wronglib/2024/2024-07/{_CHUNK_HASH}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_thumbnail_missing_file_returns_404(backend_root: Path) -> None:
    app = _make_app(backend_root)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/thumbnail/{BACKEND_NAME}/2024/2024-08/{_CHUNK_HASH}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Preview — PersistentImageProc integration
# ---------------------------------------------------------------------------

_CONTENT_HASH = "KfAbc123A2nBcR8xYvLm1P"


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


# ---------------------------------------------------------------------------
# _generate_preview — LanceIndex.search_where result handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_preview_no_match_raises_not_found(tmp_path: Path) -> None:
    """search_where returns a 2-tuple (rows, total_count); an empty result must
    raise FileNotFoundError rather than crash or silently do nothing."""
    fake_index = AsyncMock()
    fake_index.search_where.return_value = ([], 0)

    with (
        patch("wally.http_server.LanceIndex.open", AsyncMock(return_value=fake_index)),
        patch("wally.http_server.generate_preview_jpeg", AsyncMock()) as fake_generate_jpeg,
        pytest.raises(FileNotFoundError),
    ):
        await _generate_preview(
            backend=object(),
            partition="2024/2024-07",
            content_hash="doesnotexist",
            image_proc=object(),
        )
    fake_generate_jpeg.assert_not_awaited()


# ---------------------------------------------------------------------------
# Video — Range streaming
# ---------------------------------------------------------------------------

_VIDEO_HASH = "VidAbc123A2nBcR8xYvLm1P"
_VIDEO_BODY = bytes(range(256)) * 40  # 10240 bytes of deterministic content


@pytest.fixture()
def backend_with_video(tmp_path: Path) -> Path:
    """Backend holding an original MP4 at 2024/2024-07/CLIP.mp4."""
    vid_dir = tmp_path / "2024" / "2024-07"
    vid_dir.mkdir(parents=True)
    (vid_dir / "CLIP.mp4").write_bytes(_VIDEO_BODY)
    return tmp_path


def _patch_lookup(filename: str) -> Any:
    """Patch _lookup_entry to resolve to a video PhotoEntry with `filename`."""
    entry = PhotoEntry(
        filename=filename,
        content_hash=_VIDEO_HASH,
        searchable={"media_type": "video"},
    )
    return patch("wally.http_server._lookup_entry", AsyncMock(return_value=entry))


@pytest.mark.asyncio
async def test_video_full_body_200(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mp4"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mp4")
    assert resp.status_code == 200
    assert resp.content == _VIDEO_BODY
    assert resp.headers["content-type"] == "video/mp4"
    assert resp.headers["accept-ranges"] == "bytes"
    assert resp.headers["content-length"] == str(len(_VIDEO_BODY))


@pytest.mark.asyncio
async def test_video_range_206(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mp4"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mp4",
                headers={"Range": "bytes=100-199"},
            )
    assert resp.status_code == 206
    assert resp.content == _VIDEO_BODY[100:200]
    assert resp.headers["content-range"] == f"bytes 100-199/{len(_VIDEO_BODY)}"
    assert resp.headers["content-length"] == "100"


@pytest.mark.asyncio
async def test_video_open_ended_range_206(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mp4"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mp4",
                headers={"Range": "bytes=10200-"},
            )
    assert resp.status_code == 206
    assert resp.content == _VIDEO_BODY[10200:]
    assert resp.headers["content-range"] == f"bytes 10200-10239/{len(_VIDEO_BODY)}"


@pytest.mark.asyncio
async def test_video_unsatisfiable_range_416(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mp4"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mp4",
                headers={"Range": "bytes=99999-100000"},
            )
    assert resp.status_code == 416
    assert resp.headers["content-range"] == f"bytes */{len(_VIDEO_BODY)}"


@pytest.mark.asyncio
async def test_video_mov_content_type(tmp_path: Path) -> None:
    vid_dir = tmp_path / "2024" / "2024-07"
    vid_dir.mkdir(parents=True)
    (vid_dir / "CLIP.mov").write_bytes(_VIDEO_BODY)
    app = _make_app(tmp_path)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mov"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mov")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "video/quicktime"


@pytest.mark.asyncio
async def test_video_wrong_backend_404(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with _patch_lookup("CLIP.mp4"):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/video/wronglib/2024/2024-07/{_VIDEO_HASH}.mp4")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_video_not_in_index_404(backend_with_video: Path) -> None:
    app = _make_app(backend_with_video)
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    with patch("wally.http_server._lookup_entry", AsyncMock(side_effect=FileNotFoundError)):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/video/{BACKEND_NAME}/2024/2024-07/{_VIDEO_HASH}.mp4")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    ("header", "size", "expected"),
    [
        ("bytes=0-99", 1000, (0, 99)),
        ("bytes=100-", 1000, (100, 999)),
        ("bytes=-50", 1000, (950, 999)),
        ("bytes=500-99999", 1000, (500, 999)),  # end clamped to size-1
        ("bytes=1000-", 1000, "unsatisfiable"),  # start past EOF
        ("bytes=0-0,50-60", 1000, None),  # multi-range → full body
        ("bytes=abc-def", 1000, None),  # malformed → full body
        ("kilobytes=0-1", 1000, None),  # wrong unit → full body
        ("bytes=200-100", 1000, None),  # end < start → full body
    ],
)
def test_parse_range(header: str, size: int, expected: Any) -> None:
    assert _parse_range(header, size) == expected


@pytest.mark.asyncio
async def test_media_middleware_close_shuts_down_image_proc(tmp_path: Path) -> None:
    """MediaMiddleware.close() delegates to PersistentImageProc.close()."""
    app = _make_app(tmp_path)
    app._image_proc.close = AsyncMock()
    await app.close()
    app._image_proc.close.assert_awaited_once()
