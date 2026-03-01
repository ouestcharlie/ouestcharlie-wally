"""Core photo search logic for Wally.

Pure async module — no MCP dependency. Independently testable.

The search algorithm uses two-level pruning (per query_design.md):
  1. Parent manifest summary pruning: skip subtrees whose date/rating
     ranges cannot contain any match.
  2. Leaf manifest scan: evaluate the full predicate per photo entry.

Wally is read-only — it never writes to manifests or XMP sidecars.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Awaitable, Callable

from ouestcharlie_toolkit.backend import Backend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    LeafManifest,
    ParentManifest,
    PartitionSummary,
)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass
class SearchPredicate:
    """Structured filter for a photo search query.

    All fields are optional. An absent field is a wildcard (matches anything).

    V1 supported fields (per query_design.md § V1 scope):
      - date range (date_min / date_max)
      - tags (AND semantics — all listed tags must be present)
      - rating range (rating_min / rating_max)
      - camera make / model (case-insensitive substring match)
    """

    date_min: datetime | None = None
    date_max: datetime | None = None
    tags: list[str] = field(default_factory=list)
    rating_min: int | None = None
    rating_max: int | None = None
    make: str | None = None
    model: str | None = None


@dataclass
class PhotoMatch:
    """A single photo that matched the search predicate.

    Contains all information Woof needs to render a gallery entry and
    route thumbnail/preview requests to the AVIF grid tile.
    """

    partition: str
    filename: str
    content_hash: str
    date_taken: datetime | None
    rating: int | None
    tags: list[str]

    # Thumbnail grid location (None when no grid exists for this partition)
    tile_index: int | None
    thumbnails_path: str | None  # relative path to thumbnails.avif
    previews_path: str | None    # relative path to previews.avif

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
) -> SearchResult:
    """Search all photos matching predicate, traversing from root.

    Reads the manifest tree starting at root, pruning parent subtrees
    whose summary statistics exclude any possible match, then scanning
    surviving leaf manifests entry by entry.

    A missing manifest at root is treated as an unindexed library and
    returns an empty result (not an error).

    Args:
        backend: Backend to search (read-only).
        predicate: Filter to apply. An empty predicate matches all photos.
        root: Subtree to search (default "" = entire backend).
        on_progress: Optional async callback(partitions_scanned, partition)
            invoked after each leaf manifest is scanned.

    Returns:
        SearchResult with all matching PhotoMatch entries.
    """
    result = SearchResult()
    store = ManifestStore(backend)
    await _traverse(store, root, predicate, result, on_progress)
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
) -> None:
    """Recursive descent through the manifest tree.

    Reads the manifest at partition. If it is a ParentManifest, prunes
    children by their summary then recurses into survivors. If it is a
    LeafManifest, scans all photo entries.
    """
    try:
        manifest, _version = await store.read_any(partition)
    except FileNotFoundError:
        # No manifest at this path — unindexed or empty folder, skip silently.
        _log.error("No manifest at %r", partition)
        return
    except Exception as exc:
        _log.error("Failed to read manifest at %r: %s", partition, exc)
        result.errors += 1
        result.error_details.append(f"{partition}: {exc}")
        return

    if isinstance(manifest, ParentManifest):
        await _handle_parent(manifest, store, predicate, result, on_progress)
    else:
        await _handle_leaf(manifest, predicate, result, on_progress)


async def _handle_parent(
    manifest: ParentManifest,
    store: ManifestStore,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
) -> None:
    """Process a parent manifest: prune children, recurse into survivors."""
    for child in manifest.children:
        if _can_prune(child, predicate):
            result.partitions_pruned += 1
            _log.debug("Pruned partition %r (summary out of range)", child.path)
        else:
            await _traverse(store, child.path, predicate, result, on_progress)


async def _handle_leaf(
    manifest: LeafManifest,
    predicate: SearchPredicate,
    result: SearchResult,
    on_progress: Callable[[int, str], Awaitable[None]] | None,
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
    previews_path = _avif_path(manifest.partition, "previews.avif") \
        if manifest.preview_grid is not None else None

    for entry in manifest.photos:
        if not _matches(entry, predicate):
            continue
        result.matches.append(
            PhotoMatch(
                partition=manifest.partition,
                filename=entry.filename,
                content_hash=entry.content_hash,
                date_taken=entry.date_taken,
                rating=entry.rating,
                tags=list(entry.tags),
                tile_index=thumb_index.get(entry.content_hash),
                thumbnails_path=thumbnails_path,
                previews_path=previews_path,
                file_path=_file_path(manifest.partition, entry.filename),
            )
        )

    if on_progress is not None:
        await on_progress(result.partitions_scanned, manifest.partition)


# ---------------------------------------------------------------------------
# Pruning and matching
# ---------------------------------------------------------------------------


def _naive(dt: datetime) -> datetime:
    """Return timezone-naive datetime for safe min/max comparison.

    Mirrors the same helper in whitebeard/indexer.py.
    """
    return dt.replace(tzinfo=None)


def _can_prune(summary: PartitionSummary, predicate: SearchPredicate) -> bool:
    """Return True if this partition's summary proves no photo can match.

    Uses only date and rating ranges (V1). Tag/camera pruning requires
    bloom filters — deferred to post-V1 (full leaf scan instead).

    Conservative: if a summary bound is None (unknown), never prune on that
    dimension.
    """
    # Date pruning
    if predicate.date_min is not None and summary.date_max is not None:
        if _naive(summary.date_max) < _naive(predicate.date_min):
            return True
    if predicate.date_max is not None and summary.date_min is not None:
        if _naive(summary.date_min) > _naive(predicate.date_max):
            return True

    # Rating pruning
    if predicate.rating_min is not None and summary.rating_max is not None:
        if summary.rating_max < predicate.rating_min:
            return True
    if predicate.rating_max is not None and summary.rating_min is not None:
        if summary.rating_min > predicate.rating_max:
            return True

    return False


def _matches(entry: object, predicate: SearchPredicate) -> bool:
    """Return True if entry satisfies all predicate constraints.

    entry is a PhotoEntry (typed as object to avoid circular imports at
    call site; duck-typed access is safe because callers always pass
    PhotoEntry instances).
    """
    # Date
    if predicate.date_min is not None:
        if entry.date_taken is None:  # type: ignore[union-attr]
            return False
        if _naive(entry.date_taken) < _naive(predicate.date_min):  # type: ignore[union-attr]
            return False
    if predicate.date_max is not None:
        if entry.date_taken is None:  # type: ignore[union-attr]
            return False
        if _naive(entry.date_taken) > _naive(predicate.date_max):  # type: ignore[union-attr]
            return False

    # Tags (AND semantics — all listed tags must be present)
    if predicate.tags:
        entry_tags = entry.tags  # type: ignore[union-attr]
        for tag in predicate.tags:
            if tag not in entry_tags:
                return False

    # Rating
    if predicate.rating_min is not None:
        if entry.rating is None:  # type: ignore[union-attr]
            return False
        if entry.rating < predicate.rating_min:  # type: ignore[union-attr]
            return False
    if predicate.rating_max is not None:
        if entry.rating is None:  # type: ignore[union-attr]
            return False
        if entry.rating > predicate.rating_max:  # type: ignore[union-attr]
            return False

    # Camera make (case-insensitive substring)
    if predicate.make is not None:
        make_val = entry.make  # type: ignore[union-attr]
        if make_val is None or predicate.make.lower() not in make_val.lower():
            return False

    # Camera model (case-insensitive substring)
    if predicate.model is not None:
        model_val = entry.model  # type: ignore[union-attr]
        if model_val is None or predicate.model.lower() not in model_val.lower():
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
