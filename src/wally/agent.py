"""Wally MCP agent — photo search/consumption agent for OuEstCharlie."""

from __future__ import annotations

import calendar
from datetime import datetime

from mcp.server.fastmcp import Context

from ouestcharlie_toolkit.server import AgentBase

from .searcher import PhotoMatch, SearchPredicate, search_photos


class WallyAgent(AgentBase):
    """Wally: searches the photo library by traversing manifests.

    Receives ``WOOF_BACKEND_CONFIG`` from the environment (set by Woof before
    launching). Exposes one MCP tool: ``search_photos_tool``.

    Wally is read-only — it never writes XMP sidecars or manifests.
    """

    def __init__(self) -> None:
        super().__init__("wally", version="0.1.0")
        self._register_tools()

    def _register_tools(self) -> None:
        mcp = self.mcp

        @mcp.tool()
        async def search_photos_tool(
            ctx: Context,
            date_min: str | None = None,
            date_max: str | None = None,
            tags: list[str] | None = None,
            rating_min: int | None = None,
            rating_max: int | None = None,
            make: str | None = None,
            model: str | None = None,
            root: str = "",
        ) -> dict:
            """Search photos matching structured predicates.

            Traverses the manifest tree from ``root``, pruning subtrees
            whose summary statistics exclude any possible match (two-level
            pruning), then scanning surviving leaf manifests entry by entry.
            Wally never reads XMP sidecars — all metadata is inline in manifests.

            Args:
                date_min: Inclusive lower bound for ``date_taken`` (ISO 8601).
                    Partial dates are expanded to their earliest instant:
                    ``"2024"`` → ``2024-01-01T00:00:00``,
                    ``"2024-07"`` → ``2024-07-01T00:00:00``.
                date_max: Inclusive upper bound for ``date_taken`` (ISO 8601).
                    Partial dates are expanded to their latest instant:
                    ``"2024"`` → ``2024-12-31T23:59:59``,
                    ``"2024-07"`` → ``2024-07-31T23:59:59``.
                tags: List of tags that must ALL be present (AND semantics).
                    Matches ``dc:subject`` values in XMP/manifests.
                rating_min: Inclusive lower bound on ``xmp:Rating``
                    (0=unrated, 1–5=stars, -1=rejected).
                rating_max: Inclusive upper bound on ``xmp:Rating``.
                make: Case-insensitive substring match on camera make
                    (``tiff:Make``), e.g. ``"nikon"`` matches ``"Nikon D850"``.
                model: Case-insensitive substring match on camera model
                    (``tiff:Model``).
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
            predicate = SearchPredicate(
                date_min=_parse_date_min(date_min),
                date_max=_parse_date_max(date_max),
                tags=tags or [],
                rating_min=rating_min,
                rating_max=rating_max,
                make=make,
                model=model,
            )

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
    if m.date_taken is not None:
        d["dateTaken"] = m.date_taken.isoformat()
    if m.rating is not None:
        d["rating"] = m.rating
    if m.tags:
        d["tags"] = m.tags
    if m.tile_index is not None:
        d["tileIndex"] = m.tile_index
    if m.thumbnails_path is not None:
        d["thumbnailsPath"] = m.thumbnails_path
    if m.previews_path is not None:
        d["previewsPath"] = m.previews_path
    return d
