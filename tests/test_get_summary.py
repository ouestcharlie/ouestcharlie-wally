"""Tests for the get_summary runtime aggregation.

On-demand aggregate computed from the LanceDB index, scoped by the filter predicates.
"""

from __future__ import annotations

import contextlib
from datetime import datetime
from pathlib import Path

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.lance_index import PHOTO_TABLE_NAME, FtsFilter, LanceIndex
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    PhotoEntry,
    RootSummary,
    deserialize_summary,
    serialize_summary,
)

from wally.searcher import FilterGroup, FilterLeaf, RangeFilter, SearchPredicate, get_summary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=tmp_path)


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _entry(
    filename: str,
    content_hash: str,
    date_taken: datetime | None = None,
    rating: int | None = None,
    tags: list[str] | None = None,
    description: str | None = None,
) -> PhotoEntry:
    searchable: dict = {}
    if date_taken is not None:
        searchable["date_taken"] = date_taken
    if rating is not None:
        searchable["rating"] = rating
    if tags is not None:
        searchable["tags"] = tags
    if description is not None:
        searchable["description"] = description
    return PhotoEntry(filename=filename, content_hash=content_hash, searchable=searchable)


async def _leaf(backend: LocalBackend, partition: str, photos: list[PhotoEntry]) -> None:
    """Write photos to LanceDB and ensure the thin summary.json marker exists."""
    lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME, create_if_missing=True)
    await lance_index.upsert_partition(partition, photos, None)

    store = ManifestStore(backend)
    with contextlib.suppress(FileExistsError):
        await store.create_summary(RootSummary(schema_version=SCHEMA_VERSION))


# ---------------------------------------------------------------------------
# RootSummary — thin marker shape
# ---------------------------------------------------------------------------


def test_serialize_thin_summary_has_no_partitions_key() -> None:
    """The thin RootSummary shape serializes to just schemaVersion (+ optional lastIndexedAt)."""
    summary = RootSummary(schema_version=SCHEMA_VERSION, last_indexed_at=datetime(2026, 1, 1))
    d = serialize_summary(summary)
    assert d == {"schemaVersion": SCHEMA_VERSION, "lastIndexedAt": "2026-01-01T00:00:00"}


def test_deserialize_legacy_bulky_summary_ignores_partitions() -> None:
    """A pre-redesign summary.json with a `partitions` list is readable — the field is dropped."""
    legacy = {
        "schemaVersion": SCHEMA_VERSION,
        "partitions": [{"path": "2024", "photoCount": 10}],
    }
    summary = deserialize_summary(legacy)
    assert summary.schema_version == SCHEMA_VERSION
    assert not hasattr(summary, "partitions")


@pytest.mark.asyncio
async def test_write_full_summary_creates_then_overwrites(store: ManifestStore) -> None:
    """write_full_summary creates summary.json if absent, overwrites if present."""
    await store.write_full_summary(RootSummary(schema_version=SCHEMA_VERSION))
    first, _ = await store.read_summary()
    assert first.schema_version == SCHEMA_VERSION

    await store.write_full_summary(RootSummary(schema_version=SCHEMA_VERSION + 1))
    second, _ = await store.read_summary()
    assert second.schema_version == SCHEMA_VERSION + 1


# ---------------------------------------------------------------------------
# get_summary — unindexed / error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unindexed_backend_raises(backend: LocalBackend) -> None:
    """Missing summary.json → ValueError suggesting a full index (same as search_photos)."""
    with pytest.raises(ValueError, match="full index"):
        await get_summary(backend, SearchPredicate())


# ---------------------------------------------------------------------------
# get_summary — aggregation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_predicate_summarizes_whole_library(backend: LocalBackend) -> None:
    """An empty predicate aggregates over every indexed photo."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", datetime(2022, 1, 1), rating=3),
            _entry("b.jpg", "bb", datetime(2024, 6, 15), rating=5),
        ],
    )
    summary = await get_summary(backend, SearchPredicate())
    assert summary.media_count == 2
    assert summary.dateTaken["min"] == datetime(2022, 1, 1)
    assert summary.dateTaken["max"] == datetime(2024, 6, 15)
    assert summary.rating["min"] == 3
    assert summary.rating["max"] == 5


@pytest.mark.asyncio
async def test_filtered_predicate_narrows_aggregate(backend: LocalBackend) -> None:
    """A filter predicate narrows the aggregate to matching photos only."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", datetime(2022, 1, 1), rating=3),
            _entry("b.jpg", "bb", datetime(2024, 6, 15), rating=5),
        ],
    )
    predicate = SearchPredicate(
        root=FilterGroup(
            children=[FilterLeaf("dateTaken", RangeFilter(lo=datetime(2024, 1, 1), hi=None))]
        )
    )
    summary = await get_summary(backend, predicate)
    assert summary.media_count == 1
    assert summary.dateTaken["min"] == datetime(2024, 6, 15)
    assert summary.dateTaken["max"] == datetime(2024, 6, 15)


@pytest.mark.asyncio
async def test_no_match_returns_zero_count(backend: LocalBackend) -> None:
    """A predicate matching nothing returns media_count=0 with no range stats."""
    await _leaf(backend, "", [_entry("a.jpg", "aa", datetime(2022, 1, 1))])
    predicate = SearchPredicate(
        root=FilterGroup(
            children=[FilterLeaf("dateTaken", RangeFilter(lo=datetime(2030, 1, 1), hi=None))]
        )
    )
    summary = await get_summary(backend, predicate)
    assert summary.media_count == 0
    assert summary.dateTaken is None
    assert summary.tags is None


# ---------------------------------------------------------------------------
# get_summary — tag facets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tag_facets_over_whole_library(backend: LocalBackend) -> None:
    """An empty predicate returns tag counts over every indexed photo."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", tags=["travel", "france"]),
            _entry("b.jpg", "bb", tags=["travel"]),
        ],
    )
    summary = await get_summary(backend, SearchPredicate())
    assert summary.tags["counts"] == {"travel": 2, "france": 1}


@pytest.mark.asyncio
async def test_tag_facets_scoped_by_filter(backend: LocalBackend) -> None:
    """Tag facets are scoped to the same filtered set as the aggregate stats."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", rating=5, tags=["travel"]),
            _entry("b.jpg", "bb", rating=1, tags=["work"]),
        ],
    )
    predicate = SearchPredicate(
        root=FilterGroup(children=[FilterLeaf("rating", RangeFilter(lo=5, hi=None))])
    )
    summary = await get_summary(backend, predicate)
    assert summary.tags["counts"] == {"travel": 1}


# ---------------------------------------------------------------------------
# get_summary — full-text filter scoping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_summary_fts_filter_scopes_aggregate(backend: LocalBackend) -> None:
    """An fts_filter narrows the aggregate to photos matching the FTS query."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", rating=3, description="Red Canyon sunset"),
            _entry("b.jpg", "bb", rating=5, description="Sandy beach waves"),
        ],
    )
    summary = await get_summary(
        backend,
        SearchPredicate(),
        fts_filter=FtsFilter(query="Canyon", columns=["description"]),
    )
    assert summary.media_count == 1
    assert summary.rating["min"] == 3
    assert summary.rating["max"] == 3


@pytest.mark.asyncio
async def test_get_summary_fts_filter_combined_with_predicate(backend: LocalBackend) -> None:
    """fts_filter and the SQL predicate both apply: only matching-both rows are aggregated."""
    await _leaf(
        backend,
        "",
        [
            _entry("a.jpg", "aa", rating=5, tags=["travel"], description="Canyon sunset"),
            _entry("b.jpg", "bb", rating=1, tags=["work"], description="Canyon sunrise"),
            _entry("c.jpg", "cc", rating=5, tags=["family"], description="Beach waves"),
        ],
    )
    predicate = SearchPredicate(
        root=FilterGroup(children=[FilterLeaf("rating", RangeFilter(lo=4, hi=None))])
    )
    summary = await get_summary(
        backend, predicate, fts_filter=FtsFilter(query="Canyon", columns=["description"])
    )
    assert summary.media_count == 1
    assert summary.tags["counts"] == {"travel": 1}


# ---------------------------------------------------------------------------
# get_summary — video fields (media_type / duration / codec / has_audio)
# ---------------------------------------------------------------------------


def _video_entry(name: str, h: str, **video: object) -> PhotoEntry:
    return PhotoEntry(
        filename=name,
        content_hash=h,
        searchable={"media_type": "video", **video},
    )


@pytest.mark.asyncio
async def test_video_stats_end_to_end(backend: LocalBackend) -> None:
    """get_summary reports media-type facets, duration range, codec facets, audio counts."""
    await _leaf(
        backend,
        "",
        [
            # Real photos carry media_type="photo" (set by PhotoEntry.from_sidecar).
            PhotoEntry(filename="p.jpg", content_hash="p1", searchable={"media_type": "photo"}),
            _video_entry("v1.mp4", "v1", duration_seconds=15.0, video_codec="h264", has_audio=True),
            _video_entry(
                "v2.mov", "v2", duration_seconds=90.0, video_codec="hevc", has_audio=False
            ),
        ],
    )
    summary = await get_summary(backend, SearchPredicate())
    assert summary.mediaType["counts"] == {"photo": 1, "video": 2}
    assert summary.durationSeconds["min"] == 15.0
    assert summary.durationSeconds["max"] == 90.0
    assert summary.videoCodec["counts"] == {"h264": 1, "hevc": 1}
    assert summary.hasAudio == {"type": "bool_counts", "true": 1, "false": 1}


@pytest.mark.asyncio
async def test_has_audio_filter_scopes_summary(backend: LocalBackend) -> None:
    """Filtering hasAudio=true scopes the aggregate to videos that carry audio."""
    from wally.searcher import BoolFilter

    await _leaf(
        backend,
        "",
        [
            _video_entry("v1.mp4", "v1", video_codec="h264", has_audio=True),
            _video_entry("v2.mov", "v2", video_codec="hevc", has_audio=False),
        ],
    )
    predicate = SearchPredicate(
        root=FilterGroup(children=[FilterLeaf(field="hasAudio", value=BoolFilter(value=True))])
    )
    summary = await get_summary(backend, predicate)
    assert summary.media_count == 1
    assert summary.videoCodec["counts"] == {"h264": 1}
