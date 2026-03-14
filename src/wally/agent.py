"""Wally MCP agent — photo search/consumption agent for OuEstCharlie."""

from __future__ import annotations

import calendar
from datetime import datetime

from mcp.server.fastmcp import Context

from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldType
from ouestcharlie_toolkit.schema import LeafManifest, serialize_leaf, serialize_parent
from ouestcharlie_toolkit.server import AgentBase

from .searcher import (
    CollectionFilter,
    PhotoMatch,
    RangeFilter,
    SearchPredicate,
    StringFilter,
    search_photos,
)


class WallyAgent(AgentBase):
    """Wally: searches the photo library by traversing manifests.

    Receives ``WOOF_BACKEND_CONFIG`` from the environment (set by Woof before
    launching). Exposes two MCP tools:
    - ``list_search_fields_tool``: returns all queryable fields with types and formats.
    - ``search_photos_tool``: searches photos using a generic ``filters`` dict driven
      by the field definitions in ``ouestcharlie_toolkit.fields.PHOTO_FIELDS``.

    Wally is read-only — it never writes XMP sidecars or manifests.
    """

    def __init__(self) -> None:
        super().__init__("wally", version="0.1.0")
        self._register_tools()

    def _register_tools(self) -> None:
        mcp = self.mcp

        @mcp.tool()
        async def list_search_fields_tool() -> dict:
            """List all searchable photo fields with their types and filter formats.

            Returns a ``fields`` list of descriptors. Use the field names and formats
            described here when constructing the ``filters`` argument for
            ``search_photos_tool``.

            Returns:
                ``fields`` — list of field descriptors, each with:
                    ``name`` — field name to use as key in ``filters``.
                    ``type`` — semantic type (DATE_RANGE, INT_RANGE, STRING_COLLECTION,
                        STRING_MATCH, GPS_BOX, DESCRIPTIVE).
                    ``filterFormat`` — description of the expected value format.
                    ``pruneable`` — True if this field supports partition-level pruning
                        (faster searches on large libraries).
            """
            _FORMAT: dict[FieldType, str] = {
                FieldType.DATE_RANGE: (
                    'object with optional "min" and/or "max" (ISO 8601 string; '
                    'partial dates supported: "2024", "2024-07", "2024-07-14")'
                ),
                FieldType.INT_RANGE: 'object with optional "min" and/or "max" (integer)',
                FieldType.STRING_COLLECTION: "list of strings (AND semantics — all must be present)",
                FieldType.STRING_MATCH: "string (case-insensitive substring match)",
                FieldType.GPS_BOX: "not yet implemented",
                FieldType.DESCRIPTIVE: "not yet implemented",
            }
            return {
                "fields": [
                    {
                        "name": fdef.name,
                        "type": fdef.type.name,
                        "filterFormat": _FORMAT[fdef.type],
                        "pruneable": fdef.summary_range,
                    }
                    for fdef in PHOTO_FIELDS
                ]
            }

        @mcp.tool()
        async def get_root_manifest_tool() -> dict:
            """Return the root manifest of this backend as a plain dict.
            Manifest contains the summary of each child partition
                and an aggregated summary.
            Returns ``{"unindexed": True}`` if no manifest exists yet.
            """
            try:
                manifest, _ = await self.manifest_store.read_any("")
            except FileNotFoundError:
                return {"unindexed": True}
            if isinstance(manifest, LeafManifest):
                return serialize_leaf(manifest)
            return serialize_parent(manifest)

        @mcp.tool()
        async def search_photos_tool(
            ctx: Context,
            filters: dict | None = None,
            root: str = "",
        ) -> dict:
            """Search photos matching structured predicates.

            Traverses the manifest tree from ``root``, pruning subtrees
            whose summary statistics exclude any possible match (two-level
            pruning), then scanning surviving leaf manifests entry by entry.
            Wally never reads XMP sidecars — all metadata is inline in manifests.

            Use ``list_search_fields_tool`` to discover all available fields and
            their expected filter formats.

            Args:
                filters: Optional dict mapping field names to filter values.
                    The valid fields and their formats are returned by
                    ``list_search_fields_tool``. Examples::

                        # Photos taken in 2024 rated 4 or 5 stars
                        {"date": {"min": "2024", "max": "2024"},
                         "rating": {"min": 4, "max": 5}}

                        # Tagged "vacation" AND "portrait", shot on Nikon
                        {"tags": ["vacation", "portrait"], "make": "nikon"}

                        # 4K landscape photos (width ≥ 3840)
                        {"width": {"min": 3840}}

                    Omitting a field (or passing None) is a wildcard — matches all.
                root: Subtree to search, relative to the backend root.
                    Defaults to ``""`` (entire library).

            Returns:
                ``matches`` — list of matching photo records, each containing
                    ``partition``, ``filename``, ``contentHash``, ``filePath``,
                    and optionally ``dateTaken``, ``rating``, ``tags``,
                    ``tileIndex``, ``thumbnailsPath``, ``previewsPath``.
                ``partitionsScanned`` — leaf manifests fully evaluated.
                ``partitionsPruned`` — subtrees skipped by summary pruning.
                ``errors`` — count of manifest read failures.
                ``errorDetails`` — per-failure error messages.
            """
            predicate_filters: dict = {}

            for fdef in PHOTO_FIELDS:
                raw = (filters or {}).get(fdef.name)
                if raw is None:
                    continue

                if fdef.type == FieldType.DATE_RANGE:
                    lo = _parse_date_min(raw.get("min")) if isinstance(raw, dict) else None
                    hi = _parse_date_max(raw.get("max")) if isinstance(raw, dict) else None
                    if lo is not None or hi is not None:
                        predicate_filters[fdef.name] = RangeFilter(lo=lo, hi=hi)

                elif fdef.type == FieldType.INT_RANGE:
                    lo = raw.get("min") if isinstance(raw, dict) else None
                    hi = raw.get("max") if isinstance(raw, dict) else None
                    if lo is not None or hi is not None:
                        predicate_filters[fdef.name] = RangeFilter(lo=lo, hi=hi)

                elif fdef.type == FieldType.STRING_COLLECTION:
                    if isinstance(raw, list) and raw:
                        predicate_filters[fdef.name] = CollectionFilter(values=tuple(raw))

                elif fdef.type == FieldType.STRING_MATCH:
                    if isinstance(raw, str) and raw:
                        predicate_filters[fdef.name] = StringFilter(value=raw)

                # GPS_BOX and DESCRIPTIVE: not yet implemented — silently ignored

            predicate = SearchPredicate(filters=predicate_filters)

            partitions_done = 0

            async def _on_progress(count: int, partition: str) -> None:
                nonlocal partitions_done
                partitions_done = count
                try:
                    await ctx.report_progress(
                        progress=count,
                        total=count + 1,  # total unknown; +1 keeps progress < 1.0
                        message=f"scanned {partition}",
                    )
                except Exception:
                    pass  # client may have disconnected; continue searching

            result = await search_photos(
                self.backend,
                predicate=predicate,
                root=root,
                on_progress=_on_progress,
            )

            return {
                "matches": [_match_to_dict(m) for m in result.matches],
                "partitionsScanned": result.partitions_scanned,
                "partitionsPruned": result.partitions_pruned,
                "errors": result.errors,
                "errorDetails": result.error_details,
            }


# ---------------------------------------------------------------------------
# Date parsing helpers (MCP interface concern — kept out of searcher.py)
# ---------------------------------------------------------------------------


def _parse_date_min(s: str | None) -> datetime | None:
    """Parse an optional date string as an inclusive lower bound.

    Partial dates are expanded to their earliest instant:
      "2024"       → datetime(2024, 1, 1, 0, 0, 0)
      "2024-07"    → datetime(2024, 7, 1, 0, 0, 0)
      "2024-07-14" → datetime(2024, 7, 14, 0, 0, 0)
    Full ISO 8601 timestamps are parsed as-is (timezone stripped).
    """
    if s is None:
        return None
    parts = s.strip().split("T")[0].split("-")
    try:
        if len(parts) == 1:
            return datetime(int(parts[0]), 1, 1, 0, 0, 0)
        elif len(parts) == 2:
            return datetime(int(parts[0]), int(parts[1]), 1, 0, 0, 0)
        else:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]), 0, 0, 0)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Invalid date_min {s!r}: {exc}") from exc


def _parse_date_max(s: str | None) -> datetime | None:
    """Parse an optional date string as an inclusive upper bound.

    Partial dates are expanded to their latest instant:
      "2024"       → datetime(2024, 12, 31, 23, 59, 59)
      "2024-07"    → datetime(2024, 7, 31, 23, 59, 59)
      "2024-07-14" → datetime(2024, 7, 14, 23, 59, 59)
    """
    if s is None:
        return None
    parts = s.strip().split("T")[0].split("-")
    try:
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
        "filePath": m.file_path,
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
    if m.thumbnails_path is not None:
        d["thumbnailsPath"] = m.thumbnails_path
    if m.thumbnail_cols is not None:
        d["thumbnailCols"] = m.thumbnail_cols
    if m.thumbnail_tile_size is not None:
        d["thumbnailTileSize"] = m.thumbnail_tile_size
    if m.previews_path is not None:
        d["previewsPath"] = m.previews_path
    if m.preview_cols is not None:
        d["previewCols"] = m.preview_cols
    if m.preview_tile_size is not None:
        d["previewTileSize"] = m.preview_tile_size
    return d
