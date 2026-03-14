"""Core photo search logic for Wally.

Pure async module — no MCP dependency. Independently testable.

The search algorithm uses two-level pruning (per query_design.md):
  1. Parent manifest summary pruning: skip subtrees whose range statistics
     cannot contain any match (date, rating).
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
    METADATA_DIR,
    LeafManifest,
    ParentManifest,
    PartitionSummary,
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

    # Thumbnail grid location (None when no grid exists for this partition)
    tile_index: int | None
    thumbnails_path: str | None      # relative path to thumbnails.avif
    thumbnail_cols: int | None       # columns in the thumbnail AVIF grid
    thumbnail_tile_size: int | None  # tile edge in pixels (tiles are square)
    previews_path: str | None        # relative path to previews.avif
    preview_cols: int | None         # columns in the preview AVIF grid
    preview_tile_size: int | None    # tile edge in pixels (tiles are square)

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
    """Search all photos matching predicate, traversing from root.

    Reads the manifest tree starting at root, pruning parent subtrees
    whose summary statistics exclude any possible match, then scanning
    surviving leaf manifests entry by entry.

    A missing manifest at root is treated as an unindexed library and
    returns an empty result (not an error).

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
    await _traverse(store, root, predicate, result, on_progress, field_config)
    return result


# ---------------------------------------------------------------------------
# Internal traversal
# ---------------------------------------------------------------------------


async def _traverse(
    store: ManifestStore,
    partition: str,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
    field_config: list[FieldDef],
) -> None:
    """Recursive descent through the manifest tree."""
    try:
        manifest, _version = await store.read_any(partition)
    except FileNotFoundError:
        _log.error("No manifest at %r", partition)
        return
    except Exception as exc:
        _log.error("Failed to read manifest at %r: %s", partition, exc)
        result.errors += 1
        result.error_details.append(f"{partition}: {exc}")
        return

    if isinstance(manifest, ParentManifest):
        await _handle_parent(manifest, store, predicate, result, on_progress, field_config)
    else:
        await _handle_leaf(manifest, predicate, result, on_progress, field_config)


async def _handle_parent(
    manifest: ParentManifest,
    store: ManifestStore,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
    field_config: list[FieldDef],
) -> None:
    """Process a parent manifest: prune children, recurse into survivors."""
    for child in manifest.children:
        if _can_prune(child, predicate, field_config):
            result.partitions_pruned += 1
            _log.debug("Pruned partition %r (summary out of range)", child.path)
        else:
            await _traverse(store, child.path, predicate, result, on_progress, field_config)


async def _handle_leaf(
    manifest: LeafManifest,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
    field_config: list[FieldDef],
) -> None:
    """Scan a leaf manifest, appending each matching entry to result."""
    result.partitions_scanned += 1

    # Build O(1) tile-index lookups by inverting photo_order once per manifest.
    thumb_index: dict[str, int] = {}
    if manifest.thumbnail_grid is not None:
        thumb_index = {h: i for i, h in enumerate(manifest.thumbnail_grid.photo_order)}

    preview_index: dict[str, int] = {}
    if manifest.preview_grid is not None:
        preview_index = {h: i for i, h in enumerate(manifest.preview_grid.photo_order)}

    thumbnails_path = _avif_path(manifest.partition, "thumbnails.avif") \
        if manifest.thumbnail_grid is not None else None
    thumbnail_cols = manifest.thumbnail_grid.cols if manifest.thumbnail_grid is not None else None
    thumbnail_tile_size = manifest.thumbnail_grid.tile_size if manifest.thumbnail_grid is not None else None
    previews_path = _avif_path(manifest.partition, "previews.avif") \
        if manifest.preview_grid is not None else None
    preview_cols = manifest.preview_grid.cols if manifest.preview_grid is not None else None
    preview_tile_size = manifest.preview_grid.tile_size if manifest.preview_grid is not None else None

    for entry in manifest.photos:
        if not _matches(entry, predicate, field_config):
            continue
        result.matches.append(
            PhotoMatch(
                partition=manifest.partition,
                filename=entry.filename,
                content_hash=entry.content_hash,
                searchable=dict(entry.searchable),
                tile_index=thumb_index.get(entry.content_hash),
                thumbnails_path=thumbnails_path,
                thumbnail_cols=thumbnail_cols,
                thumbnail_tile_size=thumbnail_tile_size,
                previews_path=previews_path,
                preview_cols=preview_cols,
                preview_tile_size=preview_tile_size,
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
    summary: PartitionSummary,
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


def _avif_path(partition: str, filename: str) -> str:
    """Relative path to an AVIF container inside a partition's metadata dir."""
    prefix = partition.rstrip("/") + "/" if partition else ""
    return f"{prefix}{METADATA_DIR}/{filename}"


def _file_path(partition: str, filename: str) -> str:
    """Relative path to the original photo file."""
    prefix = partition.rstrip("/") + "/" if partition else ""
    return f"{prefix}{filename}"
