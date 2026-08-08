"""Wally MCP agent — photo search/consumption agent for OuEstCharlie."""

from __future__ import annotations

import calendar
import logging
from datetime import datetime

from dateutil.parser import isoparse
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.exceptions import ToolError
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldType, is_sortable
from ouestcharlie_toolkit.lance_index import FtsFilter
from ouestcharlie_toolkit.schema import _summary_to_dict
from ouestcharlie_toolkit.server import AgentBase

from .searcher import (
    BoolFilter,
    CollectionFilter,
    FilterGroup,
    FilterLeaf,
    GpsBoxFilter,
    PhotoMatch,
    RangeFilter,
    SearchPredicate,
    StringFilter,
    get_summary,
    search_photos,
)

_log = logging.getLogger(__name__)

# Human-readable filter-format description per field type, surfaced by
# list_search_fields. Must cover every FieldType member (enforced by test) —
# a missing entry makes list_search_fields raise KeyError for the whole call.
_FIELD_FORMAT: dict[FieldType, str] = {
    FieldType.DATE_RANGE: (
        'object with optional "min" and/or "max" (ISO 8601 string; '
        'partial dates supported: "2024", "2024-07", "2024-07-14"; '
        'time-of-day supported via a "T" component: '
        '"2024-07-14T18", "2024-07-14T18:30", "2024-07-14T18:30:00")'
    ),
    FieldType.INT_RANGE: 'object with optional "min" and/or "max" (integer)',
    FieldType.FLOAT_RANGE: 'object with optional "min" and/or "max" (float)',
    FieldType.STRING_COLLECTION: "list of strings (AND semantics — all must be present)",
    FieldType.STRING_MATCH: (
        "string (case-insensitive substring match) or "
        '{"value": "...", "mode": "startswith"|"contains"|"exact"}'
    ),
    FieldType.BOOL: "boolean (true/false); photos with no value are excluded",
    FieldType.TEXT: "full-text search — use full_text_filter, not filters",
    FieldType.GPS_BOX: (
        '{"minLat": float, "maxLat": float, "minLon": float, "maxLon": float} '
        "— decimal degrees bounding box; photos outside the box are excluded. "
        "All bounds optional (open-ended)."
    ),
    FieldType.DESCRIPTIVE: "not yet implemented",
}

# Shared filter-syntax documentation, embedded in both search_photos and
# get_summary's docstrings so the `all`/`any`/leaf syntax is documented once.
# Kept textually identical to Woof's copy of the same constant so the two MCP
# layers present the same filter/full-text vocabulary. Sort documentation lives
# in _SORT_SYNTAX_DOC (search_photos only) — get_summary has no sort argument.
_FILTER_SYNTAX_DOC = """\
filters: Filter expression. Three forms are accepted:

    **Single field** — one ``{"fieldName": value}`` dict::

        # media captured during an activity — full timestamps on both bounds
        {"dateTaken": {"min": "2026-07-15T07:46:41", "max": "2026-07-15T09:37:05"}}

    **``{"all": [...]}``** — AND group (all must match)::

        # 4K Nikon shots in 2024
        {"all": [
            {"dateTaken": {"min": "2024", "max": "2024"}},
            {"make": "nikon"},
            {"width": {"min": 3840}}
        ]}

    **``{"any": [...]}``** — OR group (at least one must match)::

        # Photos shot on Nikon OR Canon
        {"any": [{"make": "nikon"}, {"make": "canon"}]}

    Groups can be nested::

        # 2024 photos on Nikon OR Canon
        {"all": [
            {"dateTaken": {"min": "2024", "max": "2024"}},
            {"any": [{"make": "nikon"}, {"make": "canon"}]}
        ]}

    Tags are cumulative (AND relationship):
        # everything tagged Famille AND Vacances
        {"tags": ["Famille", "Vacances"]}
full_text_filter: Full-text search over one or more TEXT-typed
    fields. Schema::

        {"query": "Canyon", "columns": ["description"]}

    ``query`` is a single search string applied across all listed
    columns. ``columns`` must be entry_attr names of TEXT-typed
    fields (see ``list_search_fields`` → ``full_text_search.fields``).
    Results are relevance-ranked and each match includes ``_score``.
    Compatible with ``filters`` (SQL predicates applied on top of FTS)."""

# Sort documentation for search_photos only. Kept textually identical to Woof's
# copy. Not part of _FILTER_SYNTAX_DOC because get_summary shares that block and
# accepts no sort argument.
_SORT_SYNTAX_DOC = """\
sort_by: Field name to sort results by — one of the ``list_search_fields``
    names marked ``sortable`` (e.g. ``dateTaken``, ``rating``). Defaults to
    ``dateTaken``. Unknown or non-sortable names are rejected.
sort_order: ``asc`` or ``desc`` (default ``desc``)."""


class WallyAgent(AgentBase):
    """Wally: searches the photo library by traversing manifests.

    Receives ``WOOF_BACKEND_CONFIG`` from the environment (set by Woof before
    launching). Exposes MCP tools:
    - ``list_search_fields``: returns all queryable fields with types and formats.
    - ``get_summary``: aggregate stats (count, date/rating/GPS ranges, tag facets)
      over photos matching a filter expression — same filter syntax as
      ``search_photos``. Computed at query time from the LanceDB index, no
      precomputed data.
    - ``search_photos``: searches photos using a structured filter expression
      (flat AND dict, ``all``/``any`` groups, or nested combinations).

    Wally is read-only — it never writes XMP sidecars or manifests.
    """

    def __init__(self) -> None:
        super().__init__("wally", version="0.1.0")
        self._http_port: int | None = None
        self._register_tools()

    def _register_tools(self) -> None:
        mcp = self.mcp

        @mcp.tool()
        async def list_search_fields() -> dict:
            """List all searchable photo fields with their types and filter formats.

            Returns a ``fields`` list of descriptors. Use the field names and formats
            described here when constructing the ``filters`` argument for
            ``search_photos``. The same ``name`` values are used as ``sort_by`` keys —
            only fields marked ``sortable`` may be passed to ``sort_by``.

            Returns:
                ``fields`` — list of field descriptors, each with:
                    ``name`` — field name to use as key in ``filters`` and as ``sort_by``.
                    ``type`` — semantic type (DATE_RANGE, INT_RANGE, STRING_COLLECTION,
                        STRING_MATCH, GPS_BOX, DESCRIPTIVE).
                    ``filterFormat`` — description of the expected value format.
                    ``sortable`` — True if this field can be used as a ``sort_by`` key.
            """
            sql_fields = [
                {
                    "name": fdef.name,
                    "type": fdef.type.name,
                    "filterFormat": _FIELD_FORMAT[fdef.type],
                    "sortable": is_sortable(fdef),
                }
                for fdef in PHOTO_FIELDS
                if fdef.type is not FieldType.TEXT
            ]
            text_fields = [
                {"name": fdef.name, "column": fdef.entry_attr, "label": fdef.label or fdef.name}
                for fdef in PHOTO_FIELDS
                if fdef.type is FieldType.TEXT
            ]
            return {
                "fields": sql_fields,
                "full_text_search": {
                    "description": (
                        "Search across one or more text fields with a single query string. "
                        "Results are relevance-ranked and include a _score per match. "
                        'Pass via full_text_filter={"query": "...", "columns": [...]}.'
                    ),
                    "fields": text_fields,
                },
            }

        async def _get_summary_tool(
            filters: dict | None = None,
            full_text_filter: dict | None = None,
        ) -> dict:
            try:
                node = _parse_filter_node(filters or {}, PHOTO_FIELDS)
            except ValueError as exc:
                raise ToolError(str(exc)) from exc

            root_group = (
                node if isinstance(node, FilterGroup) else FilterGroup(logic="AND", children=[node])
            )
            predicate = SearchPredicate(root=root_group)

            try:
                fts = _build_fts_filter(full_text_filter)
            except ValueError as exc:
                raise ToolError(str(exc)) from exc

            try:
                summary = await get_summary(
                    self.backend,
                    predicate=predicate,
                    fts_filter=fts,
                    lance_index_path=self.lance_index_path_override,
                )
            except Exception as exc:
                raise ToolError(str(exc)) from exc

            return _summary_to_dict(summary)

        # Docstring assigned before registration — the decorator below reads
        # __doc__ immediately to build the tool description.
        _get_summary_tool.__doc__ = f"""Compute aggregate statistics for photos matching a filter.

            Returns count, per-field ranges (date, rating, width/height,
            duration, GPS bounding box), categorical facets (media type, video
            codec, tags), and boolean counts (has-audio). An empty ``filters``
            summarizes the whole library. Scope it to a query using the filter,
            optionally combined with ``full_text_filter`` — same semantics as
            ``search_photos``.

            Use ``list_search_fields`` to discover all available fields and
            their expected filter formats.

            Args:
                {_FILTER_SYNTAX_DOC}

            Returns:
                ``mediaCount`` — number of matching items.
                Per-field range stats (``dateTaken``, ``rating``, ``width``,
                ``height``, ``durationSeconds``, ``gps``), each present only if at
                least one matching item has a value for that field. Ranges carry
                ``{{"type": "date_range"|"int_range"|"float_range", "min", "max"}}``.
                Categorical facets (``mediaType``, ``videoCodec``, ``tags``) —
                ``{{"type": "string_facets"|"tag_facets", "counts": {{value: count}}}}``.
                Boolean counts (``hasAudio``) —
                ``{{"type": "bool_counts", "true": N, "false": M}}``.
                Each stat is present only when the matching set has values for it —
                e.g. a photo-only result carries no ``videoCodec``/``durationSeconds``.
            """
        mcp.tool(name="get_summary")(_get_summary_tool)

        async def _search_photos_tool(
            ctx: Context,
            filters: dict | None = None,
            full_text_filter: dict | None = None,
            sort_by: str = "dateTaken",
            sort_order: str = "desc",
            page: int = 0,
        ) -> dict:
            try:
                node = _parse_filter_node(filters or {}, PHOTO_FIELDS)
            except ValueError as exc:
                raise ToolError(str(exc)) from exc

            try:
                sort_column = _resolve_sort_column(sort_by, PHOTO_FIELDS)
            except ValueError as exc:
                raise ToolError(str(exc)) from exc

            root_group = (
                node if isinstance(node, FilterGroup) else FilterGroup(logic="AND", children=[node])
            )
            predicate = SearchPredicate(root=root_group)

            try:
                fts = _build_fts_filter(full_text_filter)
            except ValueError as exc:
                raise ToolError(str(exc)) from exc

            partitions_done = 0

            async def _on_progress(count: int, partition: str) -> None:
                nonlocal partitions_done
                partitions_done = count
                try:
                    await ctx.report_progress(
                        progress=count, total=count + 1, message=f"scanned {partition}"
                    )
                except Exception as exc:
                    _log.debug(
                        "Progress notification failed (client may have disconnected): %s", exc
                    )

            try:
                result = await search_photos(
                    self.backend,
                    predicate=predicate,
                    fts_filter=fts,
                    on_progress=_on_progress,
                    sort_by=sort_column,
                    sort_order=sort_order,
                    page=page,
                    lance_index_path=self.lance_index_path_override,
                )
            except Exception as exc:
                raise ToolError(str(exc)) from exc

            return {
                "totalCount": result.total_count,
                "page": result.page,
                "pageSize": result.page_size,
                "hasMore": result.has_more,
                "errors": result.errors,
                "errorDetails": result.error_details,
                "matches": [_match_to_dict(m) for m in result.matches],
            }

        # Docstring assigned before registration — the decorator below reads
        # __doc__ immediately to build the tool description.
        _search_photos_tool.__doc__ = f"""Search photos matching structured predicates.

            Executes a SQL query against the LanceDB columnar index.

            Use ``get_summary`` to get an overview.

            Use ``list_search_fields`` to discover all available fields and
            their expected filter formats.

            Args:
                {_FILTER_SYNTAX_DOC}
                {_SORT_SYNTAX_DOC}
                page: 0-indexed page number (default 0).

            Returns:
                ``matches`` — list of matching photo records.
                ``totalCount`` — total matches across all pages.
                ``page`` — 0-indexed page returned.
                ``pageSize`` — number of records per page.
                ``hasMore`` — True if further pages remain.
                ``errors`` — count of read failures.
                ``errorDetails`` — per-failure error messages.

                Does not include a tag-facet breakdown — call ``get_summary``
                with the same ``filters`` for aggregate stats including tags.
            """
        mcp.tool(name="search_photos")(_search_photos_tool)


# ---------------------------------------------------------------------------
# Filter validation
# ---------------------------------------------------------------------------


def _resolve_sort_column(name: str, field_config: list) -> str:
    """Map a ``sort_by`` field name to its LanceDB column, validating it.

    ``sort_by`` uses the same field names as ``list_search_fields`` / ``filters``.
    Unknown or non-sortable names raise ``ValueError`` rather than silently
    falling back to an arbitrary order.
    """
    for fdef in field_config:
        if fdef.name == name and is_sortable(fdef):
            return fdef.entry_attr
    raise ValueError(
        f"Unknown or unsortable sort field: '{name}'. "
        "Call list_search_fields to discover sortable fields (those with sortable=true)."
    )


def _parse_filter_node(raw: dict, field_config: list) -> FilterGroup | FilterLeaf:
    """Parse a raw MCP filter dict into a FilterGroup or FilterLeaf (recursive).

    Three forms are accepted:
    - ``{"all": [{"field1Name": value1}, {"field2Name": value2}...]}`` → AND group.
    - ``{"any": [{"field1Name": value1}, {"field1Name": value2}]...]}`` → OR group;.
    - single-key dict ``{"fieldName": value}`` → FilterLeaf.

    Groups are parsed recursively. i.e. "all" or "any" groups can contain other groups

    """
    # Guard against non-conformant input: a common client mistake is passing the
    # whole filter (or a nested value) as a JSON-encoded *string* rather than a JSON
    # object, e.g. filters='{"tags": "Alpinism"}'. Detect it explicitly rather than
    # letting `"all" in raw` run a substring check on a string and silently misbehave.
    if isinstance(raw, str):
        raise ValueError(
            f"filter must be a JSON object, not a string. Got {raw!r}. "
            'Pass filters as an object such as {"tags": ["Alpinism"]} — '
            "do not JSON-encode it into a string."
        )
    if not isinstance(raw, dict):
        raise ValueError(f"filter must be a JSON object, got {type(raw).__name__}: {raw!r}.")

    known = {fdef.name: fdef for fdef in field_config}

    if "all" in raw or "any" in raw:
        logic = "AND" if "all" in raw else "OR"
        key = "all" if logic == "AND" else "any"
        items = raw.get(key, [])
        if not isinstance(items, list):
            raise ValueError(f"'{key}' must be a list")
        children: list[FilterLeaf | FilterGroup] = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Each filter group child must be a dict")
            children.append(_parse_filter_node(item, field_config))
        return FilterGroup(logic=logic, children=children)

    if len(raw) == 0:
        return FilterGroup()

    if len(raw) != 1:
        raise ValueError(
            f"A filter node must be a single-field leaf (one key), "
            f'or a group using "all" or "any". Got {len(raw)} keys: {list(raw)!r}. '
            'To combine multiple conditions use {"all": [{"field1": ...}, {"field2": ...}]}.'
        )

    key, value = next(iter(raw.items()))
    if key not in known:
        raise ValueError(
            f"Unknown filter field: '{key}'. Call list_search_fields to discover available fields."
        )
    fdef = known[key]
    fv = _parse_filter_value(fdef, value)
    if fv is None:
        return FilterGroup()
    return FilterLeaf(field=key, value=fv)


_STRING_MATCH_MODES = ("contains", "startswith", "exact")
_GPS_BOX_KEYS = ("minLat", "maxLat", "minLon", "maxLon")


def _reject_unknown_subkeys(fdef, raw: dict, allowed: tuple[str, ...]) -> None:  # type: ignore[no-untyped-def]
    """Raise ValueError if a dict-valued filter carries keys outside ``allowed``.

    Without this, a misspelled sub-key (e.g. ``from``/``to`` instead of
    ``min``/``max``) is silently dropped along with the whole filter leaf, so the
    query matches everything — the caller gets a plausible-looking result with no
    indication the filter was ignored.
    """
    unknown = [k for k in raw if k not in allowed]
    if unknown:
        raise ValueError(
            f"Filter '{fdef.name}' has unknown key(s): {unknown}. Allowed key(s): {list(allowed)}."
        )


def _parse_filter_value(fdef, raw):  # type: ignore[no-untyped-def]
    """Parse a single raw filter value according to the field's FieldType."""
    if fdef.type in (FieldType.DATE_RANGE, FieldType.INT_RANGE, FieldType.FLOAT_RANGE):
        if not isinstance(raw, dict):
            raise ValueError(
                f"Filter '{fdef.name}' expects an object with 'min' and/or 'max', "
                f"got {type(raw).__name__}: {raw!r}."
            )
        _reject_unknown_subkeys(fdef, raw, ("min", "max"))
        if fdef.type == FieldType.DATE_RANGE:
            lo = _parse_date_min(raw.get("min"))
            hi = _parse_date_max(raw.get("max"))
        else:
            lo = raw.get("min")
            hi = raw.get("max")
        if lo is None and hi is None:
            raise ValueError(
                f"Filter '{fdef.name}' expects at least one of 'min'/'max' with a value."
            )
        return RangeFilter(lo=lo, hi=hi)

    if fdef.type == FieldType.STRING_COLLECTION:
        if raw is None or raw == []:
            return None
        if not isinstance(raw, list):
            raise ValueError(
                f"Filter '{fdef.name}' expects a list of strings, "
                f"got {type(raw).__name__}: {raw!r}. "
                f'Use {{"{fdef.name}": ["value1", "value2"]}}.'
            )
        return CollectionFilter(values=tuple(raw))

    if fdef.type == FieldType.STRING_MATCH:
        if isinstance(raw, str):
            return StringFilter(value=raw) if raw else None
        if isinstance(raw, dict):
            _reject_unknown_subkeys(fdef, raw, ("value", "mode"))
            val = raw.get("value")
            if val is None:
                raise ValueError(
                    f"Filter '{fdef.name}' expects a 'value' key with a string, or a bare string."
                )
            if not isinstance(val, str):
                raise ValueError(
                    f"Filter '{fdef.name}'.value must be a string, "
                    f"got {type(val).__name__}: {val!r}."
                )
            mode = raw.get("mode", "contains")
            if mode not in _STRING_MATCH_MODES:
                raise ValueError(
                    f"Filter '{fdef.name}'.mode must be one of {list(_STRING_MATCH_MODES)}, "
                    f"got {mode!r}."
                )
            return StringFilter(value=val, mode=mode) if val else None
        raise ValueError(
            f'Filter \'{fdef.name}\' expects a string or {{"value": ..., "mode": ...}}, '
            f"got {type(raw).__name__}: {raw!r}."
        )

    if fdef.type == FieldType.BOOL:
        if isinstance(raw, bool):
            return BoolFilter(value=raw)
        raise ValueError(
            f"Filter '{fdef.name}' expects a boolean (true/false), "
            f"got {type(raw).__name__}: {raw!r}."
        )

    if fdef.type == FieldType.GPS_BOX:
        if not isinstance(raw, dict):
            raise ValueError(
                f"Filter '{fdef.name}' expects an object with "
                f"{list(_GPS_BOX_KEYS)}, got {type(raw).__name__}: {raw!r}."
            )
        _reject_unknown_subkeys(fdef, raw, _GPS_BOX_KEYS)
        if all(raw.get(k) is None for k in _GPS_BOX_KEYS):
            raise ValueError(
                f"Filter '{fdef.name}' expects at least one of {list(_GPS_BOX_KEYS)} with a value."
            )
        return GpsBoxFilter(
            min_lat=raw.get("minLat"),
            max_lat=raw.get("maxLat"),
            min_lon=raw.get("minLon"),
            max_lon=raw.get("maxLon"),
        )

    # DESCRIPTIVE and TEXT: not yet implemented — silently ignored
    return None


def _build_fts_filter(full_text_filter: dict | None) -> FtsFilter | None:
    """Validate and build an FtsFilter from the raw MCP dict.

    Raises ValueError on invalid input so callers can re-raise as ToolError.
    """
    if full_text_filter is None:
        return None
    fts_query = full_text_filter.get("query")
    fts_columns = full_text_filter.get("columns")
    if not isinstance(fts_query, str) or not fts_query:
        raise ValueError("full_text_filter.query must be a non-empty string")
    if not isinstance(fts_columns, list) or not fts_columns:
        raise ValueError("full_text_filter.columns must be a non-empty list")
    text_attrs = {fdef.entry_attr for fdef in PHOTO_FIELDS if fdef.type is FieldType.TEXT}
    bad = [c for c in fts_columns if c not in text_attrs]
    if bad:
        raise ValueError(
            f"full_text_filter.columns contains non-TEXT field(s): {bad}. "
            "Use list_search_fields → full_text_search.fields for valid column names."
        )
    return FtsFilter(query=fts_query, columns=fts_columns)


# ---------------------------------------------------------------------------
# Date parsing helpers (MCP interface concern — kept out of searcher.py)
# ---------------------------------------------------------------------------


def _parse_date_min(s: str | None) -> datetime | None:
    """Parse an optional date/datetime string as an inclusive lower bound.

    Partial dates are expanded to their earliest instant:
      "2024"                → datetime(2024, 1, 1, 0, 0, 0)
      "2024-07"             → datetime(2024, 7, 1, 0, 0, 0)
      "2024-07-14"          → datetime(2024, 7, 14, 0, 0, 0)
    A time component (``T``-separated) is applied as-is, with missing
    minute/second fields defaulting to 0:
      "2024-07-14T18"       → datetime(2024, 7, 14, 18, 0, 0)
      "2024-07-14T18:30"    → datetime(2024, 7, 14, 18, 30, 0)
      "2024-07-14T18:30:15" → datetime(2024, 7, 14, 18, 30, 15)
    Any timezone designator is stripped (naive datetime returned).
    """
    if s is None:
        return None
    s = s.strip()
    try:
        if "T" in s:
            # Delegate full/partial time, fractional-second and timezone-offset
            # parsing to isoparse; strip tz so the result is naive (matching how
            # date_taken is stored). Missing minute/second fields default to 0.
            return isoparse(s).replace(tzinfo=None)
        parts = s.split("-")
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 else 1
        day = int(parts[2]) if len(parts) > 2 else 1
        return datetime(year, month, day, 0, 0, 0)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Invalid date_min {s!r}: {exc}") from exc


def _parse_date_max(s: str | None) -> datetime | None:
    """Parse an optional date/datetime string as an inclusive upper bound.

    Partial dates are expanded to their latest instant:
      "2024"                → datetime(2024, 12, 31, 23, 59, 59)
      "2024-07"             → datetime(2024, 7, 31, 23, 59, 59)
      "2024-07-14"          → datetime(2024, 7, 14, 23, 59, 59)
    A time component (``T``-separated) is applied as-is, with missing
    minute/second fields defaulting to 0 (no end-of-unit expansion):
      "2024-07-14T20"       → datetime(2024, 7, 14, 20, 0, 0)
      "2024-07-14T20:00"    → datetime(2024, 7, 14, 20, 0, 0)
    Any timezone designator is stripped (naive datetime returned).
    """
    if s is None:
        return None
    s = s.strip()
    try:
        # A precise timestamp resolves to the same instant as a lower bound.
        if "T" in s:
            return _parse_date_min(s)
        parts = s.split("-")
        if len(parts) == 1:
            return datetime(int(parts[0]), 12, 31, 23, 59, 59)
        elif len(parts) == 2:
            year, month = int(parts[0]), int(parts[1])
            last_day = calendar.monthrange(year, month)[1]
            return datetime(year, month, last_day, 23, 59, 59)
        else:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]), 23, 59, 59)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Invalid date_max {s!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Result serialization
# ---------------------------------------------------------------------------


def _match_to_dict(m: PhotoMatch) -> dict:
    d: dict = {
        "partition": m.partition,
        "filename": m.filename,
        "contentHash": m.content_hash,
    }
    for fdef in PHOTO_FIELDS:
        value = m.searchable.get(fdef.entry_attr)
        if value is None:
            continue
        if fdef.type is FieldType.DATE_RANGE:
            d[fdef.name] = value.isoformat()
        elif fdef.type is FieldType.GPS_BOX:
            d[fdef.name] = list(value)
        elif fdef.type is FieldType.STRING_COLLECTION:
            if value:
                d[fdef.name] = value
        else:
            d[fdef.name] = value
    if m.tile_index is not None:
        d["tileIndex"] = m.tile_index
    if m.avif_hash is not None:
        d["avifHash"] = m.avif_hash
    if m.score is not None:
        d["score"] = m.score
    return d
