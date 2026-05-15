"""Tests for GPS bounding box filter — matching and partition filtering."""

from __future__ import annotations

from pathlib import Path

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.lance_index import PHOTO_TABLE_NAME, LanceIndex
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import ManifestSummary, PhotoEntry

from wally.searcher import GpsBoxFilter, SearchPredicate, search_photos

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=tmp_path)


def _entry(filename: str, lat: float | None, lon: float | None) -> PhotoEntry:
    searchable: dict = {}
    if lat is not None and lon is not None:
        searchable["gps"] = (lat, lon)
    return PhotoEntry(filename=filename, content_hash=f"hash_{filename}", searchable=searchable)


async def _leaf(
    backend: LocalBackend,
    partition: str,
    photos: list[PhotoEntry],
    summary: ManifestSummary | None = None,
) -> None:
    lance_index = await LanceIndex.open_or_create(backend, PHOTO_TABLE_NAME)
    await lance_index.upsert_partition(partition, photos, None)

    store = ManifestStore(backend)
    ps = (
        summary if summary is not None else ManifestSummary(path=partition, photo_count=len(photos))
    )
    await store.upsert_partition_in_summary(ps)


# ---------------------------------------------------------------------------
# Leaf-level matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_photo_inside_bbox_matches(backend: LocalBackend) -> None:
    """Photo inside bounding box appears in results."""
    await _leaf(backend, "", [_entry("paris.jpg", lat=48.85, lon=2.35)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "paris.jpg"


@pytest.mark.asyncio
async def test_photo_outside_lat_excluded(backend: LocalBackend) -> None:
    """Photo south of min_lat is excluded."""
    await _leaf(backend, "", [_entry("marseille.jpg", lat=43.3, lon=5.37)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_photo_outside_lon_excluded(backend: LocalBackend) -> None:
    """Photo east of max_lon is excluded."""
    await _leaf(backend, "", [_entry("strasbourg.jpg", lat=48.57, lon=7.75)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_photo_without_gps_excluded(backend: LocalBackend) -> None:
    """Photo with no GPS data is excluded by a GPS filter."""
    await _leaf(backend, "", [_entry("nogps.jpg", lat=None, lon=None)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_open_ended_bbox_matches_any_gps(backend: LocalBackend) -> None:
    """GpsBoxFilter with all None bounds matches any photo with GPS data."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", lat=48.85, lon=2.35),
            _entry("b.jpg", lat=-33.86, lon=151.2),
            _entry("nogps.jpg", lat=None, lon=None),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"gps": GpsBoxFilter()}),
    )
    assert len(result.matches) == 2


# ---------------------------------------------------------------------------
# Multi-partition results
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disjoint_partition_returns_no_results(backend: LocalBackend) -> None:
    """Photo outside the filter bbox produces no results."""
    await _leaf(backend, "south", [_entry("marseille.jpg", lat=43.3, lon=5.37)])

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0
    assert result.partitions_scanned == 0


@pytest.mark.asyncio
async def test_overlapping_partition_returns_match(backend: LocalBackend) -> None:
    """Photo inside the filter bbox is returned."""
    await _leaf(backend, "idf", [_entry("paris.jpg", lat=48.85, lon=2.35)])

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.5, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_no_gps_summary_still_searched(backend: LocalBackend) -> None:
    """Partition without GPS bbox summary is still searched."""
    no_gps_summary = ManifestSummary(path="legacy", _stats={})
    await _leaf(
        backend, "legacy", [_entry("photo.jpg", lat=48.85, lon=2.35)], summary=no_gps_summary
    )

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1


# ---------------------------------------------------------------------------
# Boundary conditions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_photo_exactly_on_min_lat_boundary_matches(backend: LocalBackend) -> None:
    """Photo exactly at min_lat matches (bounds are inclusive)."""
    await _leaf(backend, "", [_entry("edge.jpg", lat=48.0, lon=2.35)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_photo_exactly_on_max_lon_boundary_matches(backend: LocalBackend) -> None:
    """Photo exactly at max_lon matches (bounds are inclusive)."""
    await _leaf(backend, "", [_entry("edge.jpg", lat=48.5, lon=3.0)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_photo_just_outside_max_lat_excluded(backend: LocalBackend) -> None:
    """Photo north of max_lat is excluded."""
    await _leaf(backend, "", [_entry("over.jpg", lat=49.0001, lon=2.35)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Single-bound filters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_min_lat_set(backend: LocalBackend) -> None:
    """GpsBoxFilter with only min_lat rejects photos south of threshold."""
    await _leaf(
        backend,
        "",
        [
            _entry("north.jpg", lat=50.0, lon=10.0),
            _entry("south.jpg", lat=40.0, lon=10.0),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"gps": GpsBoxFilter(min_lat=45.0)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "north.jpg"


@pytest.mark.asyncio
async def test_only_max_lon_set(backend: LocalBackend) -> None:
    """GpsBoxFilter with only max_lon rejects photos east of threshold."""
    await _leaf(
        backend,
        "",
        [
            _entry("west.jpg", lat=48.0, lon=-5.0),
            _entry("east.jpg", lat=48.0, lon=10.0),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"gps": GpsBoxFilter(max_lon=0.0)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "west.jpg"


# ---------------------------------------------------------------------------
# Mixed GPS / no-GPS in same partition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mixed_gps_no_gps_in_same_leaf(backend: LocalBackend) -> None:
    """Photos without GPS are excluded; photos with GPS inside bbox are included."""
    await _leaf(
        backend,
        "",
        [
            _entry("gps_in.jpg", lat=48.85, lon=2.35),
            _entry("gps_out.jpg", lat=43.3, lon=5.37),
            _entry("no_gps_1.jpg", lat=None, lon=None),
            _entry("no_gps_2.jpg", lat=None, lon=None),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "gps_in.jpg"


# ---------------------------------------------------------------------------
# Partition bbox exactly touching filter box — still searched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partition_bbox_touches_filter_box_photo_matches(backend: LocalBackend) -> None:
    """Photo at bbox boundary is included (inclusive bounds)."""
    await _leaf(backend, "touching", [_entry("border.jpg", lat=48.0, lon=2.0)])

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1
