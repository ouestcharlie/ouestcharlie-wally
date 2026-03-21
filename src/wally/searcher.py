"""Core photo search logic for Wally.

Pure async module — no MCP dependency. Independently testable.

The search algorithm uses two-level pruning (per query_design.md):
  1. summary.json pruning: skip partitions whose range statistics cannot contain
     any match (date, rating, GPS bbox).
  2. Leaf manifest scan: evaluate the full predicate per photo entry.

The matching and pruning logic is driven by a field configuration (list[FieldDef])
rather than hardwired field checks. Adding a new searchable field only requires
adding a FieldDef to PHOTO_FIELDS in ouestcharlie_toolkit.fields — no changes
here needed.

Wally is read-only — it never writes to manifests or XMP sidecars.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Union

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldDef, FieldType
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    LeafManifest,
    ManifestSummary,
    thumbnail_avif_path,
)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filter value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RangeFilter:
    """Inclusive min/max bounds for a range field (date or int).

    Either bound may be None (open-ended).
    A None entry value is always excluded when any bound is set.
    """

    lo: Any = None  # inclusive lower bound (datetime for DATE_RANGE, int for INT_RANGE)
    hi: Any = None  # inclusive upper bound


@dataclass(frozen=True)
class CollectionFilter:
    """AND-match filter for a string collection field (e.g. tags).

    All values in `values` must be present in the entry's collection.
    """

    values: tuple[str, ...]


@dataclass(frozen=True)
class StringFilter:
    """Case-insensitive substring match for a string field (e.g. make, model)."""

    value: str


@dataclass(frozen=True)
class GpsBoxFilter:
    """Bounding box filter for GPS_BOX fields.

    Only photos whose GPS point falls inside the box match.
    All bounds are in decimal degrees. None means open-ended on that side.
    A photo with no GPS data is always excluded when any bound is set.
    """

    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None


FilterValue = Union[RangeFilter, CollectionFilter, StringFilter, GpsBoxFilter]


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass
class SearchPredicate:
    """Generic search predicate.

    `filters` maps field names (matching FieldDef.name in the active field config)
    to filter values. An absent key is a wildcard (matches anything).

    Example:
        SearchPredicate(filters={
            "date":   RangeFilter(lo=datetime(2024, 1, 1), hi=datetime(2024, 12, 31)),
            "rating": RangeFilter(lo=4, hi=None),
            "tags":   CollectionFilter(values=("travel",)),
            "make":   StringFilter(value="nikon"),
        })
    """

    filters: dict[str, FilterValue] = field(default_factory=dict)


@dataclass
class PhotoMatch:
    """A single photo that matched the search predicate.

    Contains all information Woof needs to render a gallery entry and
    route thumbnail/preview requests to the AVIF grid tile.

    ``searchable`` mirrors the PhotoEntry.searchable dict (keyed by
    FieldDef.entry_attr) so Woof can serialise any field without knowing
    the field list at compile time.
    """

    partition: str
    filename: str
    content_hash: str
    searchable: dict[str, Any]  # keyed by FieldDef.entry_attr

    # Thumbnail tile location (None when no thumbnails exist for this photo)
    tile_index: int | None
    avif_path: str | None            # backend-relative path to the chunk AVIF file
    thumbnail_cols: int | None       # columns in the AVIF grid
    thumbnail_tile_size: int | None  # tile edge in pixels (tiles are square)

    # Path for "open with Finder / file system"
    file_path: str  # partition + "/" + filename, relative to backend root


@dataclass
class SearchResult:
    """Aggregated result of a search_photos call."""

    matches: list[PhotoMatch] = field(default_factory=list)
    partitions_scanned: int = 0   # leaf manifests fully evaluated
    partitions_pruned: int = 0    # subtrees skipped by summary stats
    errors: int = 0
    error_details: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def search_photos(
    backend: Backend,
    predicate: SearchPredicate,
    root: str = "",
    on_progress: Callable[[int, str], Awaitable[None]] | None = None,
    field_config: list[FieldDef] | None = None,
) -> SearchResult:
    """Search all photos matching predicate.

    Reads summary.json from the backend root to get all partition summaries,
    prunes partitions whose statistics exclude any possible match, then scans
    surviving leaf manifests entry by entry.

    A missing summary.json is treated as an unindexed library and returns an
    empty result (not an error).

    Args:
        backend:      Backend to search (read-only).
        predicate:    Filter to apply. An empty predicate matches all photos.
        root:         Subtree to search (default "" = entire backend).
        on_progress:  Optional async callback(partitions_scanned, partition)
                      invoked after each leaf manifest is scanned.
        field_config: Field definitions driving match and prune logic.
                      Defaults to PHOTO_FIELDS from ouestcharlie_toolkit.fields.

    Returns:
        SearchResult with all matching PhotoMatch entries.
    """
    if field_config is None:
        field_config = PHOTO_FIELDS
    result = SearchResult()
    store = ManifestStore(backend)

    # Read the flat summary to get all partition summaries.
    try:
        summary, _ = await store.read_summary()
    except FileNotFoundError:
        _log.info("No summary.json — library is unindexed, returning empty result")
        return result
    except Exception as exc:
        _log.error("Failed to read summary.json: %s", exc)
        result.errors += 1
        result.error_details.append(f"summary.json: {exc}")
        return result

    # Filter to the requested subtree if root is specified.
    partitions_to_scan = summary.partitions
    if root:
        root_prefix = root.rstrip("/") + "/"
        partitions_to_scan = [
            p for p in summary.partitions
            if p.path == root or p.path.startswith(root_prefix)
        ]

    # Prune by summary stats, then scan surviving leaf manifests.
    for partition_summary in partitions_to_scan:
        if _can_prune(partition_summary, predicate, field_config):
            result.partitions_pruned += 1
            _log.debug("Pruned partition %r (summary out of range)", partition_summary.path)
            continue

        try:
            manifest, _ = await store.read_leaf(partition_summary.path)
        except FileNotFoundError:
            _log.warning(
                "Partition %r in summary.json but manifest.json missing — skipping",
                partition_summary.path,
            )
            result.errors += 1
            result.error_details.append(f"{partition_summary.path}: manifest.json missing")
            continue
        except Exception as exc:
            _log.error("Failed to read manifest for %r: %s", partition_summary.path, exc)
            result.errors += 1
            result.error_details.append(f"{partition_summary.path}: {exc}")
            continue

        await _handle_leaf(manifest, predicate, result, on_progress, field_config)

    return result


async def _handle_leaf(
    manifest: LeafManifest,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
    field_config: list[FieldDef],
) -> None:
    """Scan a leaf manifest, appending each matching entry to result."""
    result.partitions_scanned += 1

    # Build O(1) chunk-aware lookup: content_hash → (avif_path, tile_index, cols, tile_size)
    thumb_lookup: dict[str, tuple[str, int, int, int]] = {}
    for chunk in manifest.thumbnail_chunks:
        chunk_path = thumbnail_avif_path(manifest.partition, chunk.avif_hash)
        for i, h in enumerate(chunk.grid.photo_order):
            thumb_lookup[h] = (chunk_path, i, chunk.grid.cols, chunk.grid.tile_size)

    for entry in manifest.photos:
        if not _matches(entry, predicate, field_config):
            continue
        thumb = thumb_lookup.get(entry.content_hash)
        result.matches.append(
            PhotoMatch(
                partition=manifest.partition,
                filename=entry.filename,
                content_hash=entry.content_hash,
                searchable=dict(entry.searchable),
                tile_index=thumb[1] if thumb else None,
                avif_path=thumb[0] if thumb else None,
                thumbnail_cols=thumb[2] if thumb else None,
                thumbnail_tile_size=thumb[3] if thumb else None,
                file_path=_file_path(manifest.partition, entry.filename),
            )
        )

    if on_progress is not None:
        await on_progress(result.partitions_scanned, manifest.partition)


# ---------------------------------------------------------------------------
# Pruning and matching — config-driven
# ---------------------------------------------------------------------------


def _naive(dt: datetime) -> datetime:
    """Return timezone-naive datetime for safe min/max comparison.

    Mirrors the same helper in whitebeard/indexer.py.
    """
    return dt.replace(tzinfo=None)


def _can_prune(
    summary: ManifestSummary,
    predicate: SearchPredicate,
    field_config: list[FieldDef],
) -> bool:
    """Return True if this partition's summary proves no photo can match.

    Dispatches on field type:
    - DATE_RANGE / INT_RANGE: checks whether the filter range is disjoint from
      the partition's min/max stats.
    - GPS_BOX: checks whether the filter bounding box is disjoint from the
      partition's GPS bbox stats.

    Conservative: if a summary bound is None (unknown), never prune on
    that dimension. STRING_COLLECTION and STRING_MATCH require full leaf scan.
    """
    for fdef in field_config:
        fv = predicate.filters.get(fdef.name)
        if fv is None:
            continue

        if fdef.type in (FieldType.DATE_RANGE, FieldType.INT_RANGE) and isinstance(fv, RangeFilter):
            field_stat = getattr(summary, fdef.name) if fdef.summary_range else None
            s_max = field_stat["max"] if field_stat else None
            s_min = field_stat["min"] if field_stat else None
            use_naive = fdef.type == FieldType.DATE_RANGE

            if fv.lo is not None and s_max is not None:
                cmp_s_max = _naive(s_max) if use_naive else s_max
                cmp_lo = _naive(fv.lo) if use_naive else fv.lo
                if cmp_s_max < cmp_lo:
                    return True

            if fv.hi is not None and s_min is not None:
                cmp_s_min = _naive(s_min) if use_naive else s_min
                cmp_hi = _naive(fv.hi) if use_naive else fv.hi
                if cmp_s_min > cmp_hi:
                    return True

        elif fdef.type is FieldType.GPS_BOX and isinstance(fv, GpsBoxFilter):
            field_stat = getattr(summary, fdef.name, None)
            if field_stat is None:
                continue  # no bbox in summary — conservative, don't prune
            p_min_lat = field_stat.get("minLat")
            p_max_lat = field_stat.get("maxLat")
            p_min_lon = field_stat.get("minLon")
            p_max_lon = field_stat.get("maxLon")
            if None in (p_min_lat, p_max_lat, p_min_lon, p_max_lon):
                continue  # incomplete summary — conservative, don't prune
            if fv.min_lat is not None and p_max_lat < fv.min_lat:
                return True  # partition fully south of filter box
            if fv.max_lat is not None and p_min_lat > fv.max_lat:
                return True  # partition fully north of filter box
            if fv.min_lon is not None and p_max_lon < fv.min_lon:
                return True  # partition fully west of filter box
            if fv.max_lon is not None and p_min_lon > fv.max_lon:
                return True  # partition fully east of filter box

    return False


def _matches(
    entry: object,
    predicate: SearchPredicate,
    field_config: list[FieldDef],
) -> bool:
    """Return True if entry satisfies all predicate constraints.

    Iterates over field_config, looking up each field's filter in the
    predicate. An absent filter is a wildcard. Dispatch is by filter
    value type (RangeFilter, CollectionFilter, StringFilter).

    entry is typed as object to avoid circular imports at call site;
    duck-typed attribute access is safe because callers always pass
    PhotoEntry instances.
    """
    for fdef in field_config:
        fv = predicate.filters.get(fdef.name)
        if fv is None:
            continue  # field not constrained — wildcard

        entry_val = entry.searchable.get(fdef.entry_attr)  # type: ignore[attr-defined]

        if isinstance(fv, RangeFilter):
            # None entry value is excluded by any range bound.
            if entry_val is None:
                return False
            if fdef.type == FieldType.DATE_RANGE:
                v = _naive(entry_val)
                if fv.lo is not None and v < _naive(fv.lo):
                    return False
                if fv.hi is not None and v > _naive(fv.hi):
                    return False
            else:  # INT_RANGE
                if fv.lo is not None and entry_val < fv.lo:
                    return False
                if fv.hi is not None and entry_val > fv.hi:
                    return False

        elif isinstance(fv, CollectionFilter):
            # All required values must be present (AND semantics).
            collection = entry_val or []
            for required in fv.values:
                if required not in collection:
                    return False

        elif isinstance(fv, StringFilter):
            # Case-insensitive substring; None entry is excluded.
            if entry_val is None or fv.value.lower() not in entry_val.lower():
                return False

        elif isinstance(fv, GpsBoxFilter):
            # Point-in-bbox match; None entry (no GPS data) is excluded.
            if entry_val is None:
                return False
            lat, lon = entry_val
            if fv.min_lat is not None and lat < fv.min_lat:
                return False
            if fv.max_lat is not None and lat > fv.max_lat:
                return False
            if fv.min_lon is not None and lon < fv.min_lon:
                return False
            if fv.max_lon is not None and lon > fv.max_lon:
                return False

    return True


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------



def _file_path(partition: str, filename: str) -> str:
    """Relative path to the original photo file."""
    prefix = partition.rstrip("/") + "/" if partition else ""
    return f"{prefix}{filename}"
