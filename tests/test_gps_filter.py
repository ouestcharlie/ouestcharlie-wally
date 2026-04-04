"""Tests for GPS bounding box filter — matching and partition pruning."""

from __future__ import annotations

from pathlib import Path

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    LeafManifest,
    ManifestSummary,
    PhotoEntry,
)

from wally.searcher import GpsBoxFilter, SearchPredicate, search_photos

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path))


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _entry(filename: str, lat: float | None, lon: float | None) -> PhotoEntry:
    searchable: dict = {}
    if lat is not None and lon is not None:
        searchable["gps"] = [lat, lon]
    return PhotoEntry(filename=filename, content_hash=f"sha256:{filename}", searchable=searchable)


def _gps_summary(
    path: str, min_lat: float, max_lat: float, min_lon: float, max_lon: float
) -> ManifestSummary:
    stats: dict = {
        "gps": {
            "type": "gps_bbox",
            "lat": {"min": min_lat, "max": max_lat},
            "lon": {"min": min_lon, "max": max_lon},
        }
    }
    return ManifestSummary(path=path, _stats=stats)


async def _leaf(
    store: ManifestStore,
    partition: str,
    photos: list[PhotoEntry],
    summary: ManifestSummary | None = None,
) -> None:
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photos,
    )
    await store.create_leaf(manifest)
    ps = (
        summary if summary is not None else ManifestSummary(path=partition, photo_count=len(photos))
    )
    await store.upsert_partition_in_summary(ps)


# ---------------------------------------------------------------------------
# Leaf-level matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_photo_inside_bbox_matches(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo inside bounding box appears in results."""
    await _leaf(store, "", [_entry("paris.jpg", lat=48.85, lon=2.35)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "paris.jpg"


@pytest.mark.asyncio
async def test_photo_outside_lat_excluded(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo south of min_lat is excluded."""
    await _leaf(store, "", [_entry("marseille.jpg", lat=43.3, lon=5.37)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_photo_outside_lon_excluded(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo east of max_lon is excluded."""
    await _leaf(store, "", [_entry("strasbourg.jpg", lat=48.57, lon=7.75)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_photo_without_gps_excluded(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo with no GPS data is excluded by a GPS filter."""
    await _leaf(store, "", [_entry("nogps.jpg", lat=None, lon=None)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_open_ended_bbox_matches_any_gps(store: ManifestStore, backend: LocalBackend) -> None:
    """GpsBoxFilter with all None bounds matches any photo with GPS data."""
    await _leaf(
        store,
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
    # All photos with GPS data match; the one without GPS is excluded
    assert len(result.matches) == 2


# ---------------------------------------------------------------------------
# Partition-level pruning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disjoint_partition_pruned(store: ManifestStore, backend: LocalBackend) -> None:
    """Partition whose GPS bbox is fully outside the filter box is pruned."""
    # Child partition: Marseille area (south of France)
    child_summary = _gps_summary("south", min_lat=43.0, max_lat=44.0, min_lon=5.0, max_lon=6.0)
    await _leaf(
        store, "south", [_entry("marseille.jpg", lat=43.3, lon=5.37)], summary=child_summary
    )

    result = await search_photos(
        backend,
        # Filter for Paris area — disjoint from Marseille partition
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 0
    assert result.partitions_pruned == 1
    assert result.partitions_scanned == 0


@pytest.mark.asyncio
async def test_overlapping_partition_not_pruned(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Partition whose GPS bbox overlaps the filter box is not pruned."""
    # Child partition covers both Paris and Île-de-France broadly
    child_summary = _gps_summary("idf", min_lat=48.0, max_lat=49.5, min_lon=1.5, max_lon=3.5)
    await _leaf(store, "idf", [_entry("paris.jpg", lat=48.85, lon=2.35)], summary=child_summary)

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.5, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_pruned == 0
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_missing_partition_bbox_not_pruned(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Partition without GPS bbox summary is never pruned (conservative)."""
    no_gps_summary = ManifestSummary(path="legacy", _stats={})
    await _leaf(store, "legacy", [_entry("photo.jpg", lat=48.85, lon=2.35)], summary=no_gps_summary)

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_pruned == 0
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1


# ---------------------------------------------------------------------------
# Boundary conditions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_photo_exactly_on_min_lat_boundary_matches(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo exactly at min_lat matches (bounds are inclusive)."""
    await _leaf(store, "", [_entry("edge.jpg", lat=48.0, lon=2.35)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_photo_exactly_on_max_lon_boundary_matches(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo exactly at max_lon matches (bounds are inclusive)."""
    await _leaf(store, "", [_entry("edge.jpg", lat=48.5, lon=3.0)])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_photo_just_outside_max_lat_excluded(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo infinitesimally north of max_lat is excluded."""
    await _leaf(store, "", [_entry("over.jpg", lat=49.0001, lon=2.35)])
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
async def test_only_min_lat_set(store: ManifestStore, backend: LocalBackend) -> None:
    """GpsBoxFilter with only min_lat rejects photos south of threshold."""
    await _leaf(
        store,
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
async def test_only_max_lon_set(store: ManifestStore, backend: LocalBackend) -> None:
    """GpsBoxFilter with only max_lon rejects photos east of threshold."""
    await _leaf(
        store,
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
async def test_mixed_gps_no_gps_in_same_leaf(store: ManifestStore, backend: LocalBackend) -> None:
    """Photos without GPS are excluded; photos with GPS inside bbox are included."""
    await _leaf(
        store,
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
# Partition bbox exactly touching filter box — not pruned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partition_bbox_touches_filter_box_not_pruned(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Partition bbox sharing a boundary with filter box is not pruned."""
    # Partition: lat [47, 48], lon [1, 2] — touches filter at lat=48, lon=2
    child_summary = _gps_summary("touching", min_lat=47.0, max_lat=48.0, min_lon=1.0, max_lon=2.0)
    await _leaf(store, "touching", [_entry("border.jpg", lat=48.0, lon=2.0)], summary=child_summary)

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)}
        ),
    )
    assert result.partitions_pruned == 0
    assert result.partitions_scanned == 1
    assert len(result.matches) == 1
