# Wally — Low-Level Design

## Overview

Wally is the V1 consumption agent for OuEstCharlie. It is **stateless and read-only**: Woof launches it as a child process (MCP server over stdio), passes a structured search predicate via an MCP tool call, and Wally returns matching photo metadata by traversing the manifest tree. It never reads XMP sidecars or writes anything.

Woof invokes Wally in response to Claude tool calls (e.g., `search_photos`) and forwards results to the gallery UI.

## Repository Structure

```
src/wally/
├── __main__.py     # Entry point (stdio MCP server, adapts from Whitebeard pattern)
├── agent.py        # WallyAgent(AgentBase) — registers MCP tools, date parsing, result serialization
└── searcher.py     # Pure async search logic — no MCP dependency, independently testable
tests/
└── test_searcher.py
```

`searcher.py` has no MCP dependency and can be unit-tested directly. `agent.py` is the thin adapter that registers the tool with FastMCP and handles MCP-layer concerns (date string parsing, progress reporting, result dict serialization).

## MCP Tool Interface

### `search_photos_tool`

**Input** (all fields optional):

| Field | Type | Description |
|---|---|---|
| `date_min` | `string` | Inclusive lower bound on `date_taken`. Partial dates accepted: `"2024"` → `2024-01-01T00:00:00`, `"2024-07"` → `2024-07-01T00:00:00` |
| `date_max` | `string` | Inclusive upper bound. `"2024-07"` → `2024-07-31T23:59:59` |
| `tags` | `string[]` | All tags must be present (AND semantics). Matches `dc:subject` values |
| `rating_min` | `int` | Minimum `xmp:Rating` (0=unrated, 1–5=stars, -1=rejected) |
| `rating_max` | `int` | Maximum `xmp:Rating` |
| `make` | `string` | Case-insensitive substring match on `tiff:Make` |
| `model` | `string` | Case-insensitive substring match on `tiff:Model` |
| `root` | `string` | Subtree to search (default `""` = entire library) |

**Output**:

| Field | Type | Description |
|---|---|---|
| `matches` | `PhotoMatch[]` | Matching photo records (see below) |
| `partitionsScanned` | `int` | Leaf manifests fully evaluated |
| `partitionsPruned` | `int` | Subtrees skipped by summary pruning |
| `errors` | `int` | Manifest read failures |
| `errorDetails` | `string[]` | Per-failure messages |

**PhotoMatch fields**: `partition`, `filename`, `contentHash`, `filePath` (always present), `tileIndex`, `thumbnailsPath`, `thumbnailCols`, `thumbnailTileSize`, `previewsPath`, `previewCols`, `previewTileSize` (grid fields, present when a thumbnail/preview grid exists), plus any searchable metadata fields driven by `PHOTO_FIELDS` (e.g. `dateTaken` as ISO 8601, `rating`, `tags`, `make`, `model`) — serialized by name using `FieldDef.name` as the JSON key.

## Query Execution: Two-Level Pruning

From [query_design.md](../ouestcharlie/query_design.md) § Query Execution.

### Level 1 — Parent manifest pruning

Read `ParentManifest.children` (list of `PartitionSummary`). For each child, check whether its summary statistics prove no photo can possibly match:

| Pruning condition | Triggered when |
|---|---|
| Date (lower) | `summary.date_max < predicate.date_min` |
| Date (upper) | `summary.date_min > predicate.date_max` |
| Rating (lower) | `summary.rating_max < predicate.rating_min` |
| Rating (upper) | `summary.rating_min > predicate.rating_max` |

Conservative: if a summary bound is `None` (unknown), that dimension is never pruned. Tags and camera fields have no V1 pruning (bloom filters deferred — full leaf scan instead).

### Level 2 — Leaf manifest scan

For each non-pruned partition, read `LeafManifest.photos` and evaluate the full predicate per `PhotoEntry`:

| Predicate field | Match rule |
|---|---|
| `date_min` / `date_max` | `entry.date_taken` in `[date_min, date_max]`; `None` date excluded by any date bound |
| `tags` | All listed tags in `entry.tags` (AND) |
| `rating_min` / `rating_max` | `entry.rating` in `[rating_min, rating_max]`; `None` excluded by any bound |
| `make` | `predicate.make.lower() in entry.make.lower()`; `None` excluded |
| `model` | `predicate.model.lower() in entry.model.lower()`; `None` excluded |

## Manifest Traversal

The traversal starts at `root` (default `""`). The manifest at each path can be either a `LeafManifest` or a `ParentManifest`.

**Disambiguation**: `ManifestStore.read_any(partition)` reads the raw JSON and routes based on the presence of the `photos` key (leaf) or `children` key (parent). This avoids speculative try/except dispatch that would double I/O on every parent node.

**Algorithm** (`_traverse` in `searcher.py`):
```
traverse(partition):
    manifest = read_any(partition)
    if missing → return (silent, not an error)
    if error   → increment errors, continue

    if ParentManifest:
        for each child in manifest.children:
            if can_prune(child.summary, predicate) → pruned++
            else → traverse(child.path)

    if LeafManifest:
        partitions_scanned++
        build tile_index dict from thumbnail_grid.photo_order
        for each entry in manifest.photos:
            if matches(entry, predicate) → append PhotoMatch
```

## Tile Index Computation

Each `LeafManifest` has `thumbnail_grid.photo_order: list[str]` — content hashes in row-major tile order, sorted by hash for stability. For a matching photo, its tile index is its position in this list.

To avoid O(n) `list.index()` per photo, `_handle_leaf()` inverts the list into a `dict[hash → index]` once per leaf manifest (O(n) once, then O(1) per lookup):

```python
thumb_index = {h: i for i, h in enumerate(manifest.thumbnail_grid.photo_order)}
tile_index = thumb_index.get(entry.content_hash)  # O(1)
```

`tile_index` is `None` when the leaf has no `thumbnail_grid` (partition not yet thumbnailed).

## Date Handling

All datetime comparisons use timezone-naive values to avoid `TypeError` when mixing aware and naive datetimes (the same pattern as `whitebeard/indexer.py`):

```python
def _naive(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None)
```

Partial date strings (`"2024"`, `"2024-07"`) are expanded in `agent.py` to full `datetime` bounds before passing to `searcher.py`. The searcher works only with `datetime | None`.

## Result Ordering

V1: results are returned in manifest traversal order — alphabetical by partition path, then in the order entries appear in each leaf manifest. No date-based sorting. This is an acknowledged limitation (see OP-Q3 in [query_design.md](../ouestcharlie/query_design.md)).

## Error Handling

| Situation | Behaviour |
|---|---|
| No manifest at root | Empty `SearchResult`, `errors == 0` (unindexed library) |
| No manifest at a child path | Silent skip (partition not yet indexed) |
| Corrupt/invalid JSON | `errors += 1`, message in `error_details`, traversal continues |
| Progress notification failure | Caught and logged at DEBUG; search continues |

## Scope and Deferred Items

**In scope:**
- Predicates: date range, tags (AND, full scan), rating range, camera make/model substring
- Single backend per Woof invocation
- No result pagination
- No manifest caching (Woof concern — OP-Q5)

**Deferred:**
- Tag bloom filter pruning at parent level (OP from query_design.md)
- Result ordering by date (OP-Q3)
- Pagination
- Cross-backend deduplication (Woof's responsibility — OP-Q4)
- Lucene DSL string input (lives in Woof for album definitions; Woof passes structured predicates to Wally)