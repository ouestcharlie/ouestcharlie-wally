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
from typing import Any

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldDef, FieldType
from ouestcharlie_toolkit.lance_index import (
    PAGE_SIZE,
    PHOTO_TABLE_NAME,
    FtsFilter,
    LanceIndex,
    _esc,
)
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import SCHEMA_VERSION

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
    """Case-insensitive partial match for a string field (e.g. make, model, directory).

    ``mode`` controls the match style:
    - ``"contains"`` (default): value appears anywhere in the field
    - ``"startswith"``: field starts with value (path prefix match)
    - ``"exact"``: case-insensitive exact match
    """

    value: str
    mode: str = "contains"  # "contains" | "startswith" | "exact"


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
# Filter tree (groups and leaves)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FilterLeaf:
    """A single field predicate: one field name mapped to a filter value."""

    field: str  # FieldDef.name
    value: FilterValue


@dataclass(frozen=True)
class FilterGroup:
    """Boolean combination of filter leaves and/or nested groups.

    ``logic`` controls how children are combined:
    - ``"AND"`` (default): all children must match
    - ``"OR"``: at least one child must match

    Wire-format shorthand: ``{"all": [...]}`` → AND group, ``{"any": [...]}`` → OR group.
    A flat field dict (no ``all``/``any`` key) is treated as an implicit AND group.
    """

    logic: str = "AND"  # "AND" | "OR"
    children: list[FilterLeaf | FilterGroup] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


class SearchPredicate:
    """Generic search predicate expressed as a filter group tree.

    ``root`` is the top-level ``FilterGroup``. Build it directly or use
    ``FilterGroup`` / ``FilterLeaf`` helpers. The agent layer parses the MCP
    wire format into this structure.

    Example (AND of date + rating):
        SearchPredicate(root=FilterGroup(logic="AND", children=[
            FilterLeaf("dateTaken",
                RangeFilter(lo=datetime(2024, 1, 1), hi=datetime(2024, 12, 31))),
            FilterLeaf("rating", RangeFilter(lo=4, hi=None)),
        ]))
    """

    def __init__(self, root: FilterGroup | None = None) -> None:
        self.root = root if root is not None else FilterGroup()


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

    # FTS relevance score — present only when a description full-text search was performed
    score: float | None = None


@dataclass
class SearchResult:
    """Aggregated result of a search_photos call."""

    matches: list[PhotoMatch] = field(default_factory=list)
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    total_count: int = 0
    page: int = 1
    page_size: int = PAGE_SIZE
    has_more: bool = False
    tag_facets: dict[str, int] = field(default_factory=dict)  # {tag: count} over full result set


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def search_photos(
    backend: Backend,
    predicate: SearchPredicate,
    fts_filter: FtsFilter | None = None,
    on_progress: Callable[[int, str], Awaitable[None]] | None = None,
    field_config: list[FieldDef] | None = None,
    sort_by: str = "date_taken",
    sort_order: str = "desc",
    page: int = 0,
) -> SearchResult:
    """Search all photos matching predicate using the LanceDB columnar index.

    Reads summary.json to verify the schema version, then executes a single
    SQL query against the LanceDB table at .ouestcharlie/index.lance/.

    A missing summary.json is treated as an unindexed library and returns an
    empty result (not an error).

    Directory/partition scoping is expressed via the ``directory`` field in
    ``predicate`` (e.g. ``StringFilter(value="2024", mode="startswith")``).

    Args:
        backend:      Backend to search (read-only).
        predicate:    Filter to apply. An empty predicate matches all photos.
        on_progress:  Optional async callback(1, partition)
                      invoked once after the query completes.
        field_config: Field definitions driving match and filter logic.
                      Defaults to PHOTO_FIELDS from ouestcharlie_toolkit.fields.
        sort_by:      Column to sort by (default "date_taken").
        sort_order:   "asc" or "desc" (default "desc").
        page:         0-indexed page number (default 0).

    Returns:
        SearchResult with one page of matching PhotoMatch entries, pagination metadata,
        and tag_facets counts computed over the full (unpaginated) result set.
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
        matches, total_count, tag_facets = await lance_index.search_where(
            where_clause,
            fts_filter=fts_filter,
            order_by=sort_by,
            order_desc=(sort_order == "desc"),
            page=page,
        )
    except Exception as exc:
        _log.error("LanceDB search failed: %s", exc, exc_info=True)
        result.errors += 1
        result.error_details.append(str(exc))
        return result

    result.tag_facets = tag_facets

    for row in matches:
        avif_hash = row.get("thumbnail_avif_hash") or None
        tile_index_raw = row.get("thumbnail_tile_index")
        score_raw = row.get("_score")
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
                score=float(score_raw) if score_raw is not None else None,
            )
        )

    unique_partitions = {m.partition for m in result.matches}
    result.total_count = total_count
    result.page = page
    result.page_size = PAGE_SIZE
    result.has_more = ((page + 1) * PAGE_SIZE) < total_count

    if on_progress is not None and unique_partitions:
        await on_progress(1, "")

    return result


# ---------------------------------------------------------------------------
# LanceDB WHERE clause builder
# ---------------------------------------------------------------------------


def _build_leaf(leaf: FilterLeaf, field_config: list[FieldDef]) -> list[str]:
    """Return SQL clause fragments for a single FilterLeaf.

    Returns a list because some filter types (DATE_RANGE, GPS_BOX) emit
    multiple clauses that must always be AND-joined regardless of the parent
    group's logic.
    """
    fdef = next((f for f in field_config if f.name == leaf.field), None)
    if fdef is None:
        return []
    fv = leaf.value

    if isinstance(fv, RangeFilter) and fdef.type is FieldType.DATE_RANGE:
        parts: list[str] = []
        if fv.lo is not None:
            ts = fv.lo.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
            parts.append(f"date_taken >= TIMESTAMP '{ts}'")
        if fv.hi is not None:
            ts = fv.hi.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
            parts.append(f"date_taken <= TIMESTAMP '{ts}'")
        return parts

    if isinstance(fv, RangeFilter) and fdef.type in (FieldType.INT_RANGE, FieldType.FLOAT_RANGE):
        col = fdef.entry_attr
        parts = []
        if fv.lo is not None:
            parts.append(f"{col} >= {fv.lo}")
        if fv.hi is not None:
            parts.append(f"{col} <= {fv.hi}")
        return parts

    if isinstance(fv, CollectionFilter) and fdef.type is FieldType.STRING_COLLECTION:
        return [f"array_has(tags, '{_esc(tag)}')" for tag in fv.values]

    if isinstance(fv, StringFilter) and fdef.type is FieldType.STRING_MATCH:
        col = fdef.entry_attr
        escaped = _esc(fv.value.lower())
        if fv.mode == "startswith":
            return [f"lower({col}) LIKE '{escaped}%'"]
        if fv.mode == "exact":
            return [f"lower({col}) = '{escaped}'"]
        return [f"lower({col}) LIKE '%{escaped}%'"]

    if fdef.type is FieldType.TEXT:
        _log.warning("Attempt to filter on a full text field '%s', skipped", fdef.entry_attr)
        return []

    if isinstance(fv, GpsBoxFilter) and fdef.type is FieldType.GPS_BOX:
        parts = ["gps_lat IS NOT NULL AND gps_lon IS NOT NULL"]
        if fv.min_lat is not None:
            parts.append(f"gps_lat >= {fv.min_lat}")
        if fv.max_lat is not None:
            parts.append(f"gps_lat <= {fv.max_lat}")
        if fv.min_lon is not None:
            parts.append(f"gps_lon >= {fv.min_lon}")
        if fv.max_lon is not None:
            parts.append(f"gps_lon <= {fv.max_lon}")
        return parts

    return []


def _build_group(group: FilterGroup, field_config: list[FieldDef]) -> str | None:
    """Recursively build a SQL expression from a FilterGroup.

    Returns a parenthesised expression for OR groups (when they contain more
    than one clause) so they compose correctly inside a parent AND group.
    Returns None when the group produces no clauses.
    """
    child_exprs: list[str] = []

    for child in group.children:
        if isinstance(child, FilterLeaf):
            frags = _build_leaf(child, field_config)
            if frags:
                # Multi-fragment leaves (date range, GPS) are always AND-joined internally.
                child_exprs.append(" AND ".join(frags) if len(frags) > 1 else frags[0])
        elif isinstance(child, FilterGroup):
            expr = _build_group(child, field_config)
            if expr:
                child_exprs.append(expr)

    if not child_exprs:
        return None

    joiner = " OR " if group.logic == "OR" else " AND "
    combined = joiner.join(child_exprs)
    # Wrap OR groups in parens so they compose safely inside parent AND expressions.
    if group.logic == "OR" and len(child_exprs) > 1:
        return f"({combined})"
    return combined


def _build_where_clause(predicate: SearchPredicate, field_config: list[FieldDef]) -> str | None:
    """Build a SQL WHERE clause from a SearchPredicate."""
    return _build_group(predicate.root, field_config)


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
