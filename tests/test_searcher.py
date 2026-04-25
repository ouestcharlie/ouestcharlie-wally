"""Tests for Wally searcher — core search logic."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    SCHEMA_VERSION,
    LeafManifest,
    ManifestSummary,
    PhotoEntry,
    ThumbnailChunk,
    ThumbnailGridLayout,
)

from wally.searcher import (
    CollectionFilter,
    RangeFilter,
    SearchPredicate,
    StringFilter,
    search_photos,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=tmp_path)


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _entry(
    filename: str = "photo.jpg",
    content_hash: str = "aabbcc",
    date_taken: datetime | None = datetime(2024, 7, 14, 10, 0, 0),
    tags: list[str] | None = None,
    rating: int | None = None,
    make: str | None = None,
    model: str | None = None,
    width: int | None = None,
    height: int | None = None,
) -> PhotoEntry:
    searchable: dict = {}
    if date_taken is not None:
        searchable["date_taken"] = date_taken
    if tags is not None:
        searchable["tags"] = tags
    if rating is not None:
        searchable["rating"] = rating
    if make is not None:
        searchable["make"] = make
    if model is not None:
        searchable["model"] = model
    if width is not None:
        searchable["width"] = width
    if height is not None:
        searchable["height"] = height
    return PhotoEntry(filename=filename, content_hash=content_hash, searchable=searchable)


def _summary(
    path: str,
    date_min: datetime | None = None,
    date_max: datetime | None = None,
    rating_min: int | None = None,
    rating_max: int | None = None,
) -> ManifestSummary:
    stats: dict = {}
    if date_min is not None or date_max is not None:
        stats["dateTaken"] = {"type": "date_range", "min": date_min, "max": date_max}
    if rating_min is not None or rating_max is not None:
        stats["rating"] = {"type": "int_range", "min": rating_min, "max": rating_max}
    return ManifestSummary(path=path, _stats=stats)


async def _leaf(
    store: ManifestStore,
    partition: str,
    photos: list[PhotoEntry],
    chunks: list[ThumbnailChunk] | None = None,
    summary: ManifestSummary | None = None,
) -> None:
    """Write a leaf manifest and register the partition in summary.json.

    If ``summary`` is given its stats are used for pruning tests; otherwise a
    minimal summary (path + photoCount) is written.
    """
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition=partition,
        photos=photos,
        thumbnail_chunks=chunks or [],
    )
    await store.create_leaf(manifest)
    ps = (
        summary if summary is not None else ManifestSummary(path=partition, photo_count=len(photos))
    )
    await store.upsert_partition_in_summary(ps)


async def _write_summaries(store: ManifestStore, summaries: list[ManifestSummary]) -> None:
    """Write partition summaries into summary.json (replaces _parent for pruning tests)."""
    for s in summaries:
        await store.upsert_partition_in_summary(s)


# ---------------------------------------------------------------------------
# Basic matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_date_range_matches(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo within date range appears in results."""
    await _leaf(store, "", [_entry(date_taken=datetime(2024, 7, 14))])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=datetime(2024, 12, 31))}
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "photo.jpg"


@pytest.mark.asyncio
async def test_date_range_excludes(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo outside date range is excluded."""
    await _leaf(store, "", [_entry(date_taken=datetime(2023, 6, 1))])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=datetime(2024, 12, 31))}
        ),
    )
    assert len(result.matches) == 0


@pytest.mark.asyncio
async def test_empty_predicate_returns_all(store: ManifestStore, backend: LocalBackend) -> None:
    """Empty predicate matches all photos."""
    photos = [
        _entry("a.jpg", "aa", datetime(2022, 1, 1)),
        _entry("b.jpg", "bb", datetime(2023, 5, 15)),
        _entry("c.jpg", "cc", None),
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
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
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
    result = await search_photos(
        backend,
        SearchPredicate(filters={"tags": CollectionFilter(values=("travel",))}),
    )
    assert len(result.matches) == 1


@pytest.mark.asyncio
async def test_tag_filter_and_semantics(store: ManifestStore, backend: LocalBackend) -> None:
    """All predicate tags must be present (AND semantics)."""
    await _leaf(
        store,
        "",
        [
            _entry("a.jpg", "aa", tags=["travel", "france"]),
            _entry("b.jpg", "bb", tags=["travel"]),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"tags": CollectionFilter(values=("travel", "france"))}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "a.jpg"


@pytest.mark.asyncio
async def test_tag_filter_no_match(store: ManifestStore, backend: LocalBackend) -> None:
    """Photo without required tag is excluded."""
    await _leaf(store, "", [_entry(tags=["france"])])
    result = await search_photos(
        backend,
        SearchPredicate(filters={"tags": CollectionFilter(values=("travel",))}),
    )
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Rating filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rating_min_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """rating_min=4 excludes photos with rating 3."""
    await _leaf(
        store,
        "",
        [
            _entry("high.jpg", "hh", rating=4),
            _entry("low.jpg", "ll", rating=3),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"rating": RangeFilter(lo=4, hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "high.jpg"


@pytest.mark.asyncio
async def test_rating_max_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """rating_max=2 excludes photos with rating 3."""
    await _leaf(
        store,
        "",
        [
            _entry("low.jpg", "ll", rating=2),
            _entry("high.jpg", "hh", rating=3),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"rating": RangeFilter(lo=None, hi=2)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "low.jpg"


@pytest.mark.asyncio
async def test_rating_none_excluded_by_rating_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with rating=None is excluded when rating_min is set."""
    await _leaf(store, "", [_entry(rating=None)])
    result = await search_photos(
        backend,
        SearchPredicate(filters={"rating": RangeFilter(lo=1, hi=None)}),
    )
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Camera make / model filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_substring_case_insensitive(store: ManifestStore, backend: LocalBackend) -> None:
    """make filter is a case-insensitive substring match."""
    await _leaf(
        store,
        "",
        [
            _entry("nikon.jpg", "nn", make="Nikon Corporation"),
            _entry("canon.jpg", "cc", make="Canon"),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"make": StringFilter(value="nikon")}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "nikon.jpg"


@pytest.mark.asyncio
async def test_model_substring_case_insensitive(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """model filter is a case-insensitive substring match."""
    await _leaf(
        store,
        "",
        [
            _entry("d850.jpg", "aa", model="NIKON D850"),
            _entry("other.jpg", "bb", model="Canon EOS R5"),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"model": StringFilter(value="d850")}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "d850.jpg"


@pytest.mark.asyncio
async def test_make_none_excluded_by_make_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with make=None is excluded when make predicate is set."""
    await _leaf(store, "", [_entry(make=None)])
    result = await search_photos(
        backend,
        SearchPredicate(filters={"make": StringFilter(value="nikon")}),
    )
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Two-level pruning — parent manifests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parent_prunes_by_date_summary(
    store: ManifestStore, backend: LocalBackend, tmp_path: Path
) -> None:
    """Child partition whose date_max < date_min is pruned without reading its leaf."""
    # Write two leaf partitions with explicit date summaries for pruning.
    await _leaf(
        store,
        "old",
        [_entry("old.jpg", "oo", datetime(2022, 6, 1))],
        summary=_summary("old", date_min=datetime(2022, 1, 1), date_max=datetime(2022, 12, 31)),
    )
    await _leaf(
        store,
        "new",
        [_entry("new.jpg", "nn", datetime(2024, 6, 1))],
        summary=_summary("new", date_min=datetime(2024, 1, 1), date_max=datetime(2024, 12, 31)),
    )

    result = await search_photos(
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "new.jpg"
    assert result.partitions_pruned == 1
    assert result.partitions_scanned == 1


@pytest.mark.asyncio
async def test_parent_prunes_by_rating_summary(store: ManifestStore, backend: LocalBackend) -> None:
    """Partition whose rating_max < rating_min filter is pruned."""
    await _leaf(
        store,
        "low",
        [_entry("low.jpg", "ll", rating=2)],
        summary=_summary("low", rating_min=2, rating_max=2),
    )
    await _leaf(
        store,
        "high",
        [_entry("high.jpg", "hh", rating=5)],
        summary=_summary("high", rating_min=5, rating_max=5),
    )

    result = await search_photos(
        backend,
        SearchPredicate(filters={"rating": RangeFilter(lo=4, hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "high.jpg"
    assert result.partitions_pruned == 1


@pytest.mark.asyncio
async def test_parent_conservative_with_none_summary_dates(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """A child with date_min=None/date_max=None is never pruned on date."""
    await _leaf(
        store, "p", [_entry(date_taken=None)], summary=_summary("p", date_min=None, date_max=None)
    )

    result = await search_photos(
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
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
async def test_tile_index_computed_correctly(store: ManifestStore, backend: LocalBackend) -> None:
    """tile_index reflects the photo's position in the chunk's grid.photo_order."""
    photos = [
        _entry("a.jpg", "aaa", datetime(2024, 1, 1)),
        _entry("b.jpg", "bbb", datetime(2024, 1, 2)),
        _entry("c.jpg", "ccc", datetime(2024, 1, 3)),
    ]
    chunk = ThumbnailChunk(
        avif_hash="HASH22CHARSEXAMPLE" + "XXXX",
        grid=ThumbnailGridLayout(
            cols=3,
            rows=1,
            tile_size=256,
            photo_order=["aaa", "bbb", "ccc"],
        ),
    )
    await _leaf(store, "", photos, chunks=[chunk])

    result = await search_photos(
        backend,
        SearchPredicate(
            filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 2), hi=datetime(2024, 1, 2))}
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "b.jpg"
    assert result.matches[0].tile_index == 1  # position in photo_order


@pytest.mark.asyncio
async def test_tile_index_none_when_no_thumbnail_chunks(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """tile_index is None when the leaf manifest has no thumbnail_chunks."""
    await _leaf(store, "", [_entry()])
    result = await search_photos(backend, SearchPredicate())
    assert result.matches[0].tile_index is None


# ---------------------------------------------------------------------------
# Path construction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_avif_path_propagated_to_match(store: ManifestStore, backend: LocalBackend) -> None:
    """avif_path on PhotoMatch points to the specific chunk AVIF file."""
    avif_path = f"{METADATA_DIR}/2024/07/thumbnails-Kf3QzA2_nBcR8xYvLm1P9w.avif"
    chunk = ThumbnailChunk(
        avif_hash="Kf3QzA2_nBcR8xYvLm1P9w",
        grid=ThumbnailGridLayout(cols=1, rows=1, tile_size=256, photo_order=["aa"]),
    )
    await _leaf(store, "2024/07", [_entry("photo.jpg", "aa")], chunks=[chunk])
    result = await search_photos(backend, SearchPredicate(), root="2024/07")
    assert len(result.matches) == 1
    assert result.matches[0].avif_path == avif_path


@pytest.mark.asyncio
async def test_file_path_root_partition(store: ManifestStore, backend: LocalBackend) -> None:
    """file_path for the root partition is just the filename."""
    await _leaf(store, "", [_entry("photo.jpg", "xx")])
    result = await search_photos(backend, SearchPredicate())
    assert result.matches[0].file_path == "photo.jpg"


@pytest.mark.asyncio
async def test_file_path_nested_partition(store: ManifestStore, backend: LocalBackend) -> None:
    """file_path for a nested partition includes the partition prefix."""
    await _leaf(store, "Vacations/Italy", [_entry("DSC_001.jpg", "xx")])
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
    store: ManifestStore, backend: LocalBackend, tmp_path: Path
) -> None:
    """Corrupt manifest.json for a partition listed in summary.json increments errors."""
    # Register partition "" in summary.json so the searcher tries to read its manifest.
    await store.upsert_partition_in_summary(ManifestSummary(path="", photo_count=1))
    # Now overwrite manifest.json with corrupt data.
    meta_dir = tmp_path / METADATA_DIR
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
    await _leaf(
        store,
        "2024/01",
        [_entry("jan.jpg", "j1", datetime(2024, 1, 15))],
        summary=_summary("2024/01", date_min=datetime(2024, 1, 1), date_max=datetime(2024, 1, 31)),
    )
    await _leaf(
        store,
        "2024/07",
        [
            _entry("jul1.jpg", "j2", datetime(2024, 7, 4)),
            _entry("jul2.jpg", "j3", datetime(2024, 7, 20)),
        ],
        summary=_summary("2024/07", date_min=datetime(2024, 7, 1), date_max=datetime(2024, 7, 31)),
    )
    await _leaf(
        store,
        "2023/12",
        [_entry("dec.jpg", "d1", datetime(2023, 12, 25))],
        summary=_summary(
            "2023/12", date_min=datetime(2023, 12, 1), date_max=datetime(2023, 12, 31)
        ),
    )

    result = await search_photos(
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
    )
    filenames = {m.filename for m in result.matches}
    assert filenames == {"jan.jpg", "jul1.jpg", "jul2.jpg"}
    assert result.partitions_scanned == 2
    assert result.partitions_pruned == 1


@pytest.mark.asyncio
async def test_partitions_scanned_counter(store: ManifestStore, backend: LocalBackend) -> None:
    """partitions_scanned increments once per leaf manifest read."""
    await _leaf(store, "p1", [_entry("a.jpg", "aa")])
    await _leaf(store, "p2", [_entry("b.jpg", "bb")])
    result = await search_photos(backend, SearchPredicate())
    assert result.partitions_scanned == 2


# ---------------------------------------------------------------------------
# Combined / multi-field predicates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_combined_date_and_rating_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """Only photos matching ALL constraints are returned (AND semantics)."""
    await _leaf(
        store,
        "",
        [
            _entry("match.jpg", "m1", date_taken=datetime(2024, 6, 1), rating=5),
            _entry("wrong_date.jpg", "m2", date_taken=datetime(2023, 1, 1), rating=5),
            _entry("wrong_rating.jpg", "m3", date_taken=datetime(2024, 6, 1), rating=2),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={
                "dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None),
                "rating": RangeFilter(lo=4, hi=None),
            }
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "match.jpg"


@pytest.mark.asyncio
async def test_combined_tag_and_make_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """tag AND make must both match."""
    await _leaf(
        store,
        "",
        [
            _entry("both.jpg", "b", tags=["travel"], make="Nikon"),
            _entry("tag_only.jpg", "t", tags=["travel"], make="Canon"),
            _entry("make_only.jpg", "k", tags=["portrait"], make="Nikon"),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={
                "tags": CollectionFilter(values=("travel",)),
                "make": StringFilter(value="nikon"),
            }
        ),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "both.jpg"


# ---------------------------------------------------------------------------
# Width / height filtering (INT_RANGE)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_width_min_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """width_min filters out narrower photos."""
    await _leaf(
        store,
        "",
        [
            _entry("wide.jpg", "w", width=3840),
            _entry("narrow.jpg", "n", width=1920),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"width": RangeFilter(lo=3840, hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "wide.jpg"


@pytest.mark.asyncio
async def test_height_max_filter(store: ManifestStore, backend: LocalBackend) -> None:
    """height_max filters out taller photos."""
    await _leaf(
        store,
        "",
        [
            _entry("short.jpg", "s", height=1080),
            _entry("tall.jpg", "t", height=2160),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"height": RangeFilter(lo=None, hi=1080)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "short.jpg"


@pytest.mark.asyncio
async def test_width_none_excluded_by_width_predicate(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with no width is excluded when a width filter is set."""
    await _leaf(store, "", [_entry(width=None)])
    result = await search_photos(
        backend,
        SearchPredicate(filters={"width": RangeFilter(lo=1, hi=None)}),
    )
    assert len(result.matches) == 0


# ---------------------------------------------------------------------------
# Date with timezone — _naive() stripping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_date_filter_strips_timezone(store: ManifestStore, backend: LocalBackend) -> None:
    """Timezone-aware dates in entries are compared naively (strip tzinfo)."""
    tz_entry = PhotoEntry(
        filename="tz.jpg",
        content_hash="tz",
        searchable={"date_taken": datetime(2024, 7, 14, 10, 0, 0, tzinfo=UTC)},
    )
    await _leaf(store, "", [tz_entry])
    result = await search_photos(
        backend,
        SearchPredicate(
            filters={
                "dateTaken": RangeFilter(
                    lo=datetime(2024, 7, 1),
                    hi=datetime(2024, 7, 31),
                )
            }
        ),
    )
    assert len(result.matches) == 1


# ---------------------------------------------------------------------------
# Preview grid fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_chunk_tile_lookup(store: ManifestStore, backend: LocalBackend) -> None:
    """Photos in different chunks get the correct avif_path and tile_index."""
    chunk_a = ThumbnailChunk(
        avif_hash="AAAA",
        grid=ThumbnailGridLayout(cols=1, rows=1, tile_size=256, photo_order=["a"]),
    )
    chunk_b = ThumbnailChunk(
        avif_hash="BBBB",
        grid=ThumbnailGridLayout(cols=1, rows=1, tile_size=256, photo_order=["b"]),
    )
    manifest = LeafManifest(
        schema_version=SCHEMA_VERSION,
        partition="",
        photos=[_entry("a.jpg", "a"), _entry("b.jpg", "b")],
        thumbnail_chunks=[chunk_a, chunk_b],
    )
    await store.create_leaf(manifest)
    await store.upsert_partition_in_summary(ManifestSummary(path="", photo_count=2))

    result = await search_photos(backend, SearchPredicate())
    ma = next(x for x in result.matches if x.filename == "a.jpg")
    mb = next(x for x in result.matches if x.filename == "b.jpg")
    assert ma.avif_path == f"{METADATA_DIR}/thumbnails-AAAA.avif"
    assert ma.tile_index == 0
    assert mb.avif_path == f"{METADATA_DIR}/thumbnails-BBBB.avif"
    assert mb.tile_index == 0


# ---------------------------------------------------------------------------
# Subtree search (root parameter)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_root_parameter_limits_search_to_subtree(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """root= restricts search to the specified subtree, ignoring sibling partitions."""
    await _leaf(store, "2024/07", [_entry("july.jpg", "j1")])
    await _leaf(store, "2023/12", [_entry("dec.jpg", "d1")])

    result = await search_photos(backend, SearchPredicate(), root="2024/07")
    assert len(result.matches) == 1
    assert result.matches[0].filename == "july.jpg"


# ---------------------------------------------------------------------------
# Deep nesting (parent → parent → leaf)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deep_nesting_traversal(store: ManifestStore, backend: LocalBackend) -> None:
    """A deeply nested partition is found via summary.json."""
    await _leaf(store, "A/B/C", [_entry("deep.jpg", "d1", date_taken=datetime(2024, 3, 1))])

    result = await search_photos(
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "deep.jpg"
    assert result.partitions_scanned == 1
    assert result.partitions_pruned == 0


@pytest.mark.asyncio
async def test_deep_nesting_prunes_intermediate_parent(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """A partition whose summary is out-of-range is pruned directly via summary.json."""
    await _leaf(
        store,
        "recent/sub",
        [_entry("new.jpg", "n", date_taken=datetime(2024, 6, 1))],
        summary=_summary(
            "recent/sub", date_min=datetime(2024, 1, 1), date_max=datetime(2024, 12, 31)
        ),
    )
    await _leaf(
        store,
        "old/sub",
        [_entry("old.jpg", "o", date_taken=datetime(2020, 6, 1))],
        summary=_summary("old/sub", date_min=datetime(2020, 1, 1), date_max=datetime(2020, 12, 31)),
    )

    result = await search_photos(
        backend,
        SearchPredicate(filters={"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=None)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "new.jpg"
    assert result.partitions_pruned == 1
    assert result.partitions_scanned == 1


# ---------------------------------------------------------------------------
# on_progress callback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_progress_called_for_each_leaf(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """on_progress is called once per leaf manifest scanned."""
    await _leaf(store, "p1", [_entry("a.jpg", "a1")])
    await _leaf(store, "p2", [_entry("b.jpg", "b1")])

    calls: list[tuple[int, str]] = []

    async def _cb(count: int, partition: str) -> None:
        calls.append((count, partition))

    await search_photos(backend, SearchPredicate(), on_progress=_cb)

    assert len(calls) == 2
    # counts must be monotonically increasing
    assert calls[0][0] == 1
    assert calls[1][0] == 2
    assert {p for _, p in calls} == {"p1", "p2"}


# ---------------------------------------------------------------------------
# Rating exact match (lo == hi)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rating_exact_match(store: ManifestStore, backend: LocalBackend) -> None:
    """RangeFilter with lo==hi matches only photos with exactly that rating."""
    await _leaf(
        store,
        "",
        [
            _entry("three.jpg", "3", rating=3),
            _entry("four.jpg", "4", rating=4),
            _entry("five.jpg", "5", rating=5),
        ],
    )
    result = await search_photos(
        backend,
        SearchPredicate(filters={"rating": RangeFilter(lo=4, hi=4)}),
    )
    assert len(result.matches) == 1
    assert result.matches[0].filename == "four.jpg"


# ---------------------------------------------------------------------------
# Tags — empty collection vs no collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tag_filter_empty_photo_tags_excluded(
    store: ManifestStore, backend: LocalBackend
) -> None:
    """Photo with an empty tag list is excluded when a tag filter is set."""
    await _leaf(store, "", [_entry(tags=[])])
    result = await search_photos(
        backend,
        SearchPredicate(filters={"tags": CollectionFilter(values=("travel",))}),
    )
    assert len(result.matches) == 0
