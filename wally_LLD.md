# Wally — Low-Level Design

## Overview

Wally is the consumption agent for OuEstCharlie. It is **read-only**: it never reads XMP sidecars or writes manifests. Wally runs in two modes simultaneously:

1. **MCP search server** — Woof keeps Wally running as a persistent sidecar (stdio MCP server) for the duration of the Woof session. Woof calls `search_photos_tool` in response to Claude tool calls and forwards results to the gallery UI.
2. **HTTP server** — Wally exposes a local HTTP server that serves thumbnail AVIF strips and on-demand JPEG previews. Both are read via the backend abstraction. On preview cache miss, it generates the JPEG by calling `image-proc` (Rust CLI) and caches the result at `{partition}/.ouestcharlie/previews/{content_hash}.jpg`. Subsequent requests are served from the backend cache.

Wally is kept alive (not spawned per call) so its HTTP server remains available between MCP tool calls to serve preview requests from the gallery.

## Repository Structure

```
src/wally/
├── __main__.py     # Entry point — wraps MCP app in MediaMiddleware, then runs stdio MCP server
├── agent.py        # WallyAgent(AgentBase) — registers MCP tools, date parsing, result serialization
├── http_server.py  # MediaMiddleware: pure-ASGI middleware for thumbnail and preview serving
└── searcher.py     # Pure async search logic — no MCP dependency, independently testable
tests/
├── test_http_server.py
└── test_searcher.py
```

`searcher.py` has no MCP dependency and can be unit-tested directly. `agent.py` is the thin adapter that registers tools with FastMCP and handles MCP-layer concerns (date string parsing, progress reporting, result dict serialization). 
`http_server.py` runs independently of the MCP layer in its own daemon threads.

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

**PhotoMatch fields**: `partition`, `filename`, `contentHash`, `filePath` (always present), `tileIndex`, `thumbnailsPath`, `thumbnailCols`, `thumbnailTileSize` (thumbnail grid fields, present when the partition has been thumbnailed), plus any searchable metadata fields driven by `PHOTO_FIELDS` (e.g. `dateTaken` as ISO 8601, `rating`, `tags`, `make`, `model`, `width`, `height`) — serialized by name using `FieldDef.name` as the JSON key.

The `contentHash` field doubles as the preview JPEG identifier: the gallery constructs the preview URL as `http://127.0.0.1:<wally_port>/previews/<backend>/<partition>/<contentHash>.jpg` without needing a separate manifest field.

## HTTP Media Server

### Architecture

`http_server.py` implements `MediaMiddleware`, a pure-ASGI middleware class. It wraps the MCP Starlette app and intercepts `/thumbnails/` and `/previews/` routes before they reach the MCP layer (which requires Bearer authentication). All file access goes through the backend abstraction (`ouestcharlie_toolkit.backend.Backend`), so the storage layer can be swapped without touching this class.

`MediaMiddleware` runs entirely in the asyncio event loop that drives the Starlette/MCP app — no daemon threads or secondary event loops.

### URL scheme

```
GET /thumbnails/{backend_name}/{partition}/thumbnails.avif
GET /previews/{backend_name}/{partition}/{content_hash}.jpg
```

`{partition}` may contain slashes (e.g. `2024/2024-07`). For previews, the last path segment is `{content_hash}.jpg`; everything before it is the partition.

### Request handling

**Thumbnails:**
```
request arrives
  │
  ├─ wrong backend_name? → 404
  │
  ├─ backend.read("{partition}/.ouestcharlie/thumbnails.avif")
  │     ├─ FileNotFoundError → 404
  │     └─ success → 200 image/avif
```

**Previews:**
```
request arrives
  │
  ├─ wrong backend_name? → 404
  │
  ├─ backend.exists("{partition}/.ouestcharlie/previews/{hash}.jpg")?
  │     └─ no → _ensure_preview(partition, content_hash)
  │                 │
  │                 ├─ already in-flight? → wait on asyncio.Event (dedup)
  │                 │
  │                 └─ new → _generate_preview(backend, partition, content_hash)
  │                               │
  │                               1. ManifestStore.read_leaf(partition)
  │                               2. find PhotoEntry by content_hash
  │                               3. generate_preview_jpeg(backend, partition, entry)
  │                                  (stages photo → image-proc jpeg_preview → backend write)
  │                               4. signal asyncio.Event
  │
  ├─ backend.read("{partition}/.ouestcharlie/previews/{hash}.jpg")
  │     ├─ FileNotFoundError → 503 (generation failed)
  │     └─ success → 200 image/jpeg
```

An `asyncio.Lock` guards a `dict[str, asyncio.Event]` keyed by `"{partition}:{content_hash}"`. If two requests arrive simultaneously for the same photo, only one triggers generation; the other awaits the event with a 120 s timeout.

### Configuration

| Env var | Source | Purpose |
|---|---|---|
| `WALLY_HTTP_PORT` | Injected by Woof | Port to bind; falls back to OS-assigned if absent |
| `WALLY_BACKEND_NAME` | Injected by Woof | Validated against the `{backend_name}` URL segment |
| `WOOF_BACKEND_CONFIG` | Injected by Woof | JSON backend config (`{"type": "filesystem", "root": "..."}`) passed to `backend_from_config()` |

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

| Situation | Behavior |
|---|---|
| No manifest at root | Empty `SearchResult`, `errors == 0` (unindexed library) |
| No manifest at a child path | Silent skip (partition not yet indexed) |
| Corrupt/invalid JSON | `errors += 1`, message in `error_details`, traversal continues |
| Progress notification failure | Caught and logged at DEBUG; search continues |

## MCP Tools Summary

| Tool | Description |
|---|---|
| `search_photos_tool` | Search photos by structured predicates; returns matches with tile index and thumbnail grid metadata |
| `list_search_fields_tool` | Return all queryable fields with types and filter formats |
| `get_root_manifest_tool` | Return the root summary (all indexed partitions with statistics) |
| `get_http_port_tool` | Return the port Wally's HTTP preview server is listening on (diagnostic) |

## Scope and Deferred Items

**In scope:**
- Predicates: date range, tags (AND, full scan), rating range, camera make/model substring
- Single backend per Woof invocation
- On-demand JPEG preview generation and caching
- No result pagination
- No manifest caching (Woof concern — OP-Q5)

**Deferred:**
- Tag bloom filter pruning at parent level (OP from query_design.md)
- Result ordering by date (OP-Q3)
- Pagination
- Cross-backend deduplication (Woof's responsibility — OP-Q4)
- Lucene DSL string input (lives in Woof for album definitions; Woof passes structured predicates to Wally)