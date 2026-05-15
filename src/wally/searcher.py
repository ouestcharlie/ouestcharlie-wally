"""Core photo search logic for Wally.

Pure async module — no MCP dependency. Independently testable.

The search algorithm queries the LanceDB columnar index at
.ouestcharlie/index.lance/ — a single SQL predicate replaces the two-level
JSON pruning + manifest scan approach of schema v2.

Wally is read-only — it never writes to manifests or XMP sidecars.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldDef, FieldType
from ouestcharlie_toolkit.lance_index import PHOTO_TABLE_NAME, LanceIndex, _esc
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    ManifestSummary,
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


FilterValue = RangeFilter | CollectionFilter | StringFilter | GpsBoxFilter


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
    avif_hash: str | None  # hash of the AVIF chunk file (identifies the grid)


@dataclass
class SearchResult:
    """Aggregated result of a search_photos call."""

    matches: list[PhotoMatch] = field(default_factory=list)
    partitions_scanned: int = 0  # leaf manifests fully evaluated
    partitions_pruned: int = 0  # subtrees skipped by summary stats
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
    """Search all photos matching predicate using the LanceDB columnar index.

    Reads summary.json to verify the schema version, then executes a single
    SQL query against the LanceDB table at .ouestcharlie/index.lance/.

    A missing summary.json is treated as an unindexed library and returns an
    empty result (not an error).

    Args:
        backend:      Backend to search (read-only).
        predicate:    Filter to apply. An empty predicate matches all photos.
        root:         Subtree to search (default "" = entire backend).
        on_progress:  Optional async callback(partitions_scanned, partition)
                      invoked once after the query completes.
        field_config: Field definitions driving match and filter logic.
                      Defaults to PHOTO_FIELDS from ouestcharlie_toolkit.fields.

    Returns:
        SearchResult with all matching PhotoMatch entries.
    """
    if field_config is None:
        field_config = PHOTO_FIELDS
    result = SearchResult()
    store = ManifestStore(backend)

    try:
        summary, _ = await store.read_summary()
    except FileNotFoundError:
        _log.info("No summary.json — library is unindexed, returning empty result")
        return result
    except Exception as exc:
        _log.error("Failed to read summary.json: %s", exc)
        raise Exception(f"summary.json: {exc}") from exc

    if summary.schema_version != SCHEMA_VERSION:
        msg = (
            f"Library index schema version {summary.schema_version} does not match "
            f"expected version {SCHEMA_VERSION}. Run a full index to upgrade."
        )
        _log.error(msg)
        raise ValueError(msg)

    try:
        lance_index = await LanceIndex.open(backend, PHOTO_TABLE_NAME)
    except FileNotFoundError as err:
        _log.error("LanceDB index missing")
        raise ValueError("LanceDB index missing for backend. Run a full index.") from err

    where_clause = _build_where_clause(predicate, field_config)
    try:
        rows = await lance_index.search_where(where_clause, root)
    except Exception as exc:
        _log.error("LanceDB search failed: %s", exc, exc_info=True)
        result.errors += 1
        result.error_details.append(str(exc))
        return result

    for row in rows:
        avif_hash = row.get("thumbnail_avif_hash") or None
        tile_index_raw = row.get("thumbnail_tile_index")
        result.matches.append(
            PhotoMatch(
                partition=row["partition"],
                filename=row["filename"],
                content_hash=row["content_hash"],
                searchable=_row_to_searchable(row, field_config),
                tile_index=int(tile_index_raw)
                if avif_hash is not None and tile_index_raw is not None
                else None,
                avif_hash=avif_hash,
            )
        )

    unique_partitions = {m.partition for m in result.matches}
    result.partitions_scanned = len(unique_partitions)

    if on_progress is not None and unique_partitions:
        await on_progress(result.partitions_scanned, "")

    return result


# ---------------------------------------------------------------------------
# LanceDB WHERE clause builder
# ---------------------------------------------------------------------------


def _build_where_clause(
    predicate: SearchPredicate,
    field_config: list[FieldDef],
) -> str | None:
    """Build a SQL WHERE clause string from a SearchPredicate.

    Returns None if the predicate has no active filters (match all).
    """
    clauses: list[str] = []

    for fdef in field_config:
        fv = predicate.filters.get(fdef.name)
        if fv is None:
            continue

        if isinstance(fv, RangeFilter) and fdef.type is FieldType.DATE_RANGE:
            if fv.lo is not None:
                ts = fv.lo.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                clauses.append(f"date_taken >= TIMESTAMP '{ts}'")
            if fv.hi is not None:
                ts = fv.hi.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                clauses.append(f"date_taken <= TIMESTAMP '{ts}'")

        elif isinstance(fv, RangeFilter) and fdef.type is FieldType.INT_RANGE:
            col = fdef.entry_attr
            if fv.lo is not None:
                clauses.append(f"{col} >= {fv.lo}")
            if fv.hi is not None:
                clauses.append(f"{col} <= {fv.hi}")

        elif isinstance(fv, CollectionFilter) and fdef.type is FieldType.STRING_COLLECTION:
            for tag in fv.values:
                clauses.append(f"array_has(tags, '{_esc(tag)}')")

        elif isinstance(fv, StringFilter) and fdef.type is FieldType.STRING_MATCH:
            col = fdef.entry_attr
            clauses.append(f"lower({col}) LIKE '%{_esc(fv.value.lower())}%'")

        elif isinstance(fv, GpsBoxFilter) and fdef.type is FieldType.GPS_BOX:
            # Always require non-null GPS when this filter is present.
            clauses.append("gps_lat IS NOT NULL AND gps_lon IS NOT NULL")
            if fv.min_lat is not None:
                clauses.append(f"gps_lat >= {fv.min_lat}")
            if fv.max_lat is not None:
                clauses.append(f"gps_lat <= {fv.max_lat}")
            if fv.min_lon is not None:
                clauses.append(f"gps_lon >= {fv.min_lon}")
            if fv.max_lon is not None:
                clauses.append(f"gps_lon <= {fv.max_lon}")

    return " AND ".join(clauses) if clauses else None


def _row_to_searchable(row: dict[str, Any], field_config: list[FieldDef]) -> dict[str, Any]:
    """Build a PhotoEntry-style searchable dict from a LanceDB row."""
    searchable: dict[str, Any] = {}
    for fdef in field_config:
        if fdef.type is FieldType.GPS_BOX:
            lat = row.get("gps_lat")
            lon = row.get("gps_lon")
            searchable[fdef.entry_attr] = (lat, lon) if lat is not None or lon is not None else None
        elif fdef.type is FieldType.DATE_RANGE:
            searchable[fdef.entry_attr] = row.get("date_taken")
        elif fdef.type is FieldType.STRING_COLLECTION:
            searchable[fdef.entry_attr] = list(row.get("tags") or [])
        else:
            searchable[fdef.entry_attr] = row.get(fdef.entry_attr)
    return searchable


# ---------------------------------------------------------------------------
# Pruning and matching — config-driven (kept for tests and future use)
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
            lat_stat = field_stat.get("lat", {})
            lon_stat = field_stat.get("lon", {})
            p_min_lat = lat_stat.get("min")
            p_max_lat = lat_stat.get("max")
            p_min_lon = lon_stat.get("min")
            p_max_lon = lon_stat.get("max")
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
