"""Tests for Wally searcher — core search logic."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    SCHEMA_VERSION,
    LeafManifest,
    ParentManifest,
    PartitionSummary,
    PhotoEntry,
    ThumbnailGridLayout,
    manifest_path,
)

from wally.searcher import PhotoMatch, SearchPredicate, SearchResult, search_photos

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path))


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _entry(
    filename: str = "photo.jpg",
    content_hash: str = "sha256:aabbcc",
    date_taken: datetime | None = datetime(2024, 7, 14, 10, 0, 0),
    tags: list[str] | None = None,
    rating: int | None = None,
    make: str | None = None,
    model: str | None = None,
) -> PhotoEntry:
    return PhotoEntry(
        filename=filename,
        content_hash=content_hash,
        date_taken=date_taken,
        tags=tags or [],
        rating=rating,
        make=make,
        model=model,
    )


def _summary(
    path: str,
    date_min: datetime | None = None,
    date_max: datetime | None = None,
    rating_min: int | None = None,
    rating_max: int | None = None,
) -> PartitionSummary:
    return PartitionSummary(
        path=path,
        date_min=date_min,
        date_max=date_max,
        rating_min=rating_min,
        rating_max=rating_max,
    )


async def _leaf(store: ManifestStore, partition: str, photos: list[PhotoEntry],
                grid: ThumbnailGridLayout | None = None) -> None:
    """Write a leaf manifest with the given photos."""
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photos,
        thumbnail_grid=grid,
    )
    await store.create_leaf(manifest)


async def _parent(store: ManifestStore, path: str, children: list[PartitionSummary]) -> None:
    """Write a parent manifest with the given child summaries."""
    manifest = ParentManifest(
        schema_version=SCHEMA_VERSION,
        path=path,
        children=children,
    )
    await store.create_parent(manifest)


# ---------------------------------------------------------------------------
# Basic matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_date_range_matches(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo within date range appears in results."""
    await _leaf(store, "", [_entry(date_taken=datetime(2024, 7, 14))])
    result = await search_photos(
        backend,
        SearchPredicate(date_min=datetime(2024, 1, 1), date_max=datetime(2024, 12, 31)),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "photo.jpg"


@pytest.mark.asyncio
async def test_date_range_excludes(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo outside date range is excluded."""
    await _leaf(store, "", [_entry(date_taken=datetime(2023, 6, 1))])
    result = await search_photos(
        backend,
        SearchPredicate(date_min=datetime(2024, 1, 1), date_max=datetime(2024, 12, 31)),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_empty_predicate_returns_all(store: ManifestStore, backend: LocalBackend) -> None:
    """Empty predicate matches all photos."""
    photos = [
        _entry("a.jpg", "sha256:aa", datetime(2022, 1, 1)),
        _entry("b.jpg", "sha256:bb", datetime(2023, 5, 15)),
        _entry("c.jpg", "sha256:cc", None),
    ]
    await _leaf(store, "", photos)
    result = await search_photos(backend, SearchPredicate())
    assert len(result.matches) == 3


@pytest.mark.asyncio
async def test_photo_without_date_excluded_by_date_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with date_taken=None is excluded when predicate has date_min."""
    await _leaf(store, "", [_entry(date_taken=None)])
    result = await search_photos(
        backend, SearchPredicate(date_min=datetime(2024, 1, 1))
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_photo_without_date_included_by_empty_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with date_taken=None is included by a predicate with no date bounds."""
    await _leaf(store, "", [_entry(date_taken=None)])
    result = await search_photos(backend, SearchPredicate())
    assert len(result.matches) == 1


# ---------------------------------------------------------------------------
# Tag filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tag_filter_matches(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo with matching tag is returned."""
    await _leaf(store, "", [_entry(tags=["travel", "france"])])
    result = await search_photos(backend, SearchPredicate(tags=["travel"]))
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_tag_filter_and_semantics(store: ManifestStore, backend: LocalBackend) -> None:
    """All predicate tags must be present (AND semantics)."""
    await _leaf(store, "", [
        _entry("a.jpg", "sha256:aa", tags=["travel", "france"]),
        _entry("b.jpg", "sha256:bb", tags=["travel"]),
    ])
    result = await search_photos(backend, SearchPredicate(tags=["travel", "france"]))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "a.jpg"


@pytest.mark.asyncio
async def test_tag_filter_no_match(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo without required tag is excluded."""
    await _leaf(store, "", [_entry(tags=["france"])])
    result = await search_photos(backend, SearchPredicate(tags=["travel"]))
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Rating filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rating_min_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """rating_min=4 excludes photos with rating 3."""
    await _leaf(store, "", [
        _entry("high.jpg", "sha256:hh", rating=4),
        _entry("low.jpg", "sha256:ll", rating=3),
    ])
    result = await search_photos(backend, SearchPredicate(rating_min=4))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "high.jpg"


@pytest.mark.asyncio
async def test_rating_max_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """rating_max=2 excludes photos with rating 3."""
    await _leaf(store, "", [
        _entry("low.jpg", "sha256:ll", rating=2),
        _entry("high.jpg", "sha256:hh", rating=3),
    ])
    result = await search_photos(backend, SearchPredicate(rating_max=2))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "low.jpg"


@pytest.mark.asyncio
async def test_rating_none_excluded_by_rating_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with rating=None is excluded when rating_min is set."""
    await _leaf(store, "", [_entry(rating=None)])
    result = await search_photos(backend, SearchPredicate(rating_min=1))
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Camera make / model filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_substring_case_insensitive(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """make filter is a case-insensitive substring match."""
    await _leaf(store, "", [
        _entry("nikon.jpg", "sha256:nn", make="Nikon Corporation"),
        _entry("canon.jpg", "sha256:cc", make="Canon"),
    ])
    result = await search_photos(backend, SearchPredicate(make="nikon"))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "nikon.jpg"


@pytest.mark.asyncio
async def test_model_substring_case_insensitive(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """model filter is a case-insensitive substring match."""
    await _leaf(store, "", [
        _entry("d850.jpg", "sha256:aa", model="NIKON D850"),
        _entry("other.jpg", "sha256:bb", model="Canon EOS R5"),
    ])
    result = await search_photos(backend, SearchPredicate(model="d850"))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "d850.jpg"


@pytest.mark.asyncio
async def test_make_none_excluded_by_make_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with make=None is excluded when make predicate is set."""
    await _leaf(store, "", [_entry(make=None)])
    result = await search_photos(backend, SearchPredicate(make="nikon"))
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Two-level pruning — parent manifests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parent_prunes_by_date_summary(
    store: ManifestStore, backend: LocalBackend, tmp_path: Path
) -> None:
    """Child partition whose date_max < date_min is pruned without reading its leaf."""
    # Write two child leaves: one in 2022 (out of range), one in 2024 (in range).
    await _leaf(store, "old", [_entry("old.jpg", "sha256:oo", datetime(2022, 6, 1))])
    await _leaf(store, "new", [_entry("new.jpg", "sha256:nn", datetime(2024, 6, 1))])

    # Write root parent manifest pointing to both children.
    await _parent(store, "", [
        _summary("old", date_min=datetime(2022, 1, 1), date_max=datetime(2022, 12, 31)),
        _summary("new", date_min=datetime(2024, 1, 1), date_max=datetime(2024, 12, 31)),
    ])

    result = await search_photos(
        backend, SearchPredicate(date_min=datetime(2024, 1, 1))
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "new.jpg"
    assert result.partitions_pruned == 1
    assert result.partitions_scanned == 1


@pytest.mark.asyncio
async def test_parent_prunes_by_rating_summary(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Child partition whose rating_max < rating_min is pruned."""
    await _leaf(store, "low", [_entry("low.jpg", "sha256:ll", rating=2)])
    await _leaf(store, "high", [_entry("high.jpg", "sha256:hh", rating=5)])

    await _parent(store, "", [
        _summary("low", rating_min=2, rating_max=2),
        _summary("high", rating_min=5, rating_max=5),
    ])

    result = await search_photos(backend, SearchPredicate(rating_min=4))
    assert len(result.matches) == 1
    assert result.matches[0].filename == "high.jpg"
    assert result.partitions_pruned == 1


@pytest.mark.asyncio
async def test_parent_conservative_with_none_summary_dates(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """A child with date_min=None/date_max=None is never pruned on date."""
    await _leaf(store, "p", [_entry(date_taken=None)])
    await _parent(store, "", [
        _summary("p", date_min=None, date_max=None),
    ])

    result = await search_photos(
        backend, SearchPredicate(date_min=datetime(2024, 1, 1))
    )
    # The photo itself has no date, so it's excluded at leaf scan — but the
    # partition is NOT pruned at the parent level.
    assert result.partitions_pruned == 0
    assert result.partitions_scanned == 1
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Tile index computation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tile_index_computed_correctly(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """tile_index reflects the photo's position in thumbnail_grid.photo_order."""
    photos = [
        _entry("a.jpg", "sha256:aaa", datetime(2024, 1, 1)),
        _entry("b.jpg", "sha256:bbb", datetime(2024, 1, 2)),
        _entry("c.jpg", "sha256:ccc", datetime(2024, 1, 3)),
    ]
    grid = ThumbnailGridLayout(
        cols=3, rows=1, tile_size=256,
        # Sorted by content_hash ascending — matching stable ordering
        photo_order=["sha256:aaa", "sha256:bbb", "sha256:ccc"],
    )
    await _leaf(store, "", photos, grid=grid)

    result = await search_photos(
        backend,
        SearchPredicate(date_min=datetime(2024, 1, 2), date_max=datetime(2024, 1, 2)),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "b.jpg"
    assert result.matches[0].tile_index == 1  # position in photo_order


@pytest.mark.asyncio
async def test_tile_index_none_when_no_thumbnail_grid(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """tile_index is None when the leaf manifest has no thumbnail_grid."""
    await _leaf(store, "", [_entry()])
    result = await search_photos(backend, SearchPredicate())
    assert result.matches[0].tile_index is None


# ---------------------------------------------------------------------------
# Path construction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_thumbnails_path_formed_correctly(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """thumbnails_path points to the AVIF container inside the metadata dir."""
    grid = ThumbnailGridLayout(cols=1, rows=1, tile_size=256, photo_order=["sha256:aa"])
    await _leaf(store, "2024/07", [_entry("photo.jpg", "sha256:aa")], grid=grid)
    result = await search_photos(backend, SearchPredicate(), root="2024/07")
    assert len(result.matches) == 1
    assert result.matches[0].thumbnails_path == f"2024/07/{METADATA_DIR}/thumbnails.avif"


@pytest.mark.asyncio
async def test_file_path_root_partition(store: ManifestStore, backend: LocalBackend) -> None:
    """file_path for the root partition is just the filename."""
    await _leaf(store, "", [_entry("photo.jpg", "sha256:xx")])
    result = await search_photos(backend, SearchPredicate())
    assert result.matches[0].file_path == "photo.jpg"


@pytest.mark.asyncio
async def test_file_path_nested_partition(store: ManifestStore, backend: LocalBackend) -> None:
    """file_path for a nested partition includes the partition prefix."""
    await _leaf(store, "Vacations/Italy", [_entry("DSC_001.jpg", "sha256:xx")])
    result = await search_photos(backend, SearchPredicate(), root="Vacations/Italy")
    assert result.matches[0].file_path == "Vacations/Italy/DSC_001.jpg"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_root_manifest_returns_empty_result(backend: LocalBackend) -> None:
    """No manifest at root → empty result, no errors (unindexed library)."""
    result = await search_photos(backend, SearchPredicate())
    assert result.matches == []
    assert result.errors == 0


@pytest.mark.asyncio
async def test_corrupt_manifest_increments_error_count(
    backend: LocalBackend, tmp_path: Path
) -> None:
    """Corrupt JSON at a manifest path increments result.errors."""
    meta_dir = tmp_path / METADATA_DIR
    meta_dir.mkdir(parents=True)
    (meta_dir / "manifest.json").write_bytes(b"not valid json }{")

    result = await search_photos(backend, SearchPredicate())
    assert result.errors == 1
    assert len(result.error_details) == 1


# ---------------------------------------------------------------------------
# Multi-partition search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_partition_search(store: ManifestStore, backend: LocalBackend) -> None:
    """Results are aggregated across multiple leaf partitions."""
    await _leaf(store, "2024/01", [
        _entry("jan.jpg", "sha256:j1", datetime(2024, 1, 15)),
    ])
    await _leaf(store, "2024/07", [
        _entry("jul1.jpg", "sha256:j2", datetime(2024, 7, 4)),
        _entry("jul2.jpg", "sha256:j3", datetime(2024, 7, 20)),
    ])
    await _leaf(store, "2023/12", [
        _entry("dec.jpg", "sha256:d1", datetime(2023, 12, 25)),
    ])
    # Build a root parent pointing to the three partitions (simplified).
    await _parent(store, "", [
        _summary("2024/01", date_min=datetime(2024, 1, 1), date_max=datetime(2024, 1, 31)),
        _summary("2024/07", date_min=datetime(2024, 7, 1), date_max=datetime(2024, 7, 31)),
        _summary("2023/12", date_min=datetime(2023, 12, 1), date_max=datetime(2023, 12, 31)),
    ])

    result = await search_photos(
        backend,
        SearchPredicate(date_min=datetime(2024, 1, 1)),
    )
    filenames = {m.filename for m in result.matches}
    assert filenames == {"jan.jpg", "jul1.jpg", "jul2.jpg"}
    assert result.partitions_scanned == 2
    assert result.partitions_pruned == 1


@pytest.mark.asyncio
async def test_partitions_scanned_counter(store: ManifestStore, backend: LocalBackend) -> None:
    """partitions_scanned increments once per leaf manifest read."""
    await _leaf(store, "p1", [_entry("a.jpg", "sha256:aa")])
    await _leaf(store, "p2", [_entry("b.jpg", "sha256:bb")])
    await _parent(store, "", [
        _summary("p1"),
        _summary("p2"),
    ])
    result = await search_photos(backend, SearchPredicate())
    assert result.partitions_scanned == 2
