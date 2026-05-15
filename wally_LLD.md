# Wally — Low-Level Design

## Overview

Wally is the consumption agent for OuEstCharlie. It is **read-only**: it never reads XMP sidecars or writes manifests. Wally runs in two modes simultaneously:

1. **MCP search server** — Woof keeps Wally running as a persistent sidecar (stdio MCP server) for the duration of the Woof session. Woof calls `search_photos` in response to Claude tool calls and forwards results to the gallery UI.
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
├── test_gps_filter.py   # GPS bounding box filter end-to-end tests
├── test_http_server.py
├── test_searcher.py
└── test_where_clause.py # Unit tests for _build_where_clause SQL generation
```

`searcher.py` has no MCP dependency and can be unit-tested directly. `agent.py` is the thin adapter that registers tools with FastMCP and handles MCP-layer concerns (date string parsing, progress reporting, result dict serialization). 
`http_server.py` runs independently of the MCP layer in its own daemon threads.

## MCP Tool Interface

### `search_photos`

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
| `partitionsScanned` | `int` | Distinct partitions represented in the result set |
| `partitionsPruned` | `int` | Always 0 — filtering is handled entirely by LanceDB's query engine |
| `errors` | `int` | Manifest read failures |
| `errorDetails` | `string[]` | Per-failure messages |

**PhotoMatch fields**: `partition`, `filename`, `contentHash`, `tileIndex`, `avifHash`, plus any searchable metadata fields driven by `PHOTO_FIELDS` (e.g. `dateTaken` as ISO 8601, `rating`, `tags`, `make`, `model`, `width`, `height`) — serialized by name using `FieldDef.name` as the JSON key.

The `contentHash` field doubles as the preview JPEG identifier: the gallery constructs the preview URL as `http://127.0.0.1:<wally_port>/previews/<backend>/<partition>/<contentHash>.jpg` without needing a separate manifest field.

## HTTP Media Server

### Architecture

`http_server.py` implements `MediaMiddleware`, a pure-ASGI middleware class. It intercepts `/thumbnails/` and `/previews/` routes and serves them via the backend abstraction; all other requests pass through to the inner MCP Starlette app.

In `__main__.py` the ASGI stack is layered as (outermost first):

```
_BearerGuard        — enforces Bearer token auth on all routes
  → MediaMiddleware — handles /thumbnails/… and /previews/…
    → MCP app       — handles /mcp
```

`_BearerGuard` is only applied when `WOOF_AGENT_TOKEN` is set. When present, every request (MCP and media alike) must carry a matching `Authorization: Bearer <token>` header. Woof forwards this token when proxying media requests.

`MediaMiddleware` runs entirely in the asyncio event loop that drives the Starlette/MCP app — no daemon threads or secondary event loops. All file access goes through the backend abstraction (`ouestcharlie_toolkit.backend.Backend`), so the storage layer can be swapped without touching this class.

`MediaMiddleware` owns a single `PersistentImageProc` instance for the lifetime of the server. All preview generation requests share this process, eliminating per-request subprocess startup cost (significant on Windows). `close()` shuts the process down gracefully; it is called from `__main__.py` in a `finally` block when the server exits.

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
  │                 └─ new → _generate_preview(backend, partition, content_hash, image_proc)
  │                               │
  │                               1. ManifestStore.read_leaf(partition)
  │                               2. find PhotoEntry by content_hash
  │                               3. generate_preview_jpeg(backend, partition, entry,
  │                                    image_proc=self._image_proc)
  │                                  (stages photo → PersistentImageProc.request → backend write)
  │                               4. signal asyncio.Event
  │
  ├─ backend.read("{partition}/.ouestcharlie/previews/{hash}.jpg")
  │     ├─ FileNotFoundError → 503 (generation failed)
  │     └─ success → 200 image/jpeg
```

`_generate_preview` receives the `PersistentImageProc` instance from `MediaMiddleware` and passes it to `generate_preview_jpeg`. The persistent process is serialised internally by an `asyncio.Lock` inside `PersistentImageProc`, so concurrent preview requests proceed safely.

An `asyncio.Lock` guards a `dict[str, asyncio.Event]` keyed by `"{partition}:{content_hash}"`. If two requests arrive simultaneously for the same photo, only one triggers generation; the other awaits the event with a 120 s timeout.

### Configuration

| Env var | Source | Purpose |
|---|---|---|
| `WOOF_AGENT_TOKEN` | Injected by Woof | Security token for the HTTP server |
| `WOOF_BACKEND_CONFIG` | Injected by Woof | JSON backend config (`{"type": "filesystem", "root": "..."}`) passed to `backend_from_config()` |

## Query Execution: LanceDB SQL Query

Wally queries the LanceDB columnar index at `.ouestcharlie/index.lance/` using a single SQL WHERE clause built from the `SearchPredicate`. All filter predicates are translated by `_build_where_clause` in `searcher.py` and evaluated by LanceDB's query engine in one pass — no per-partition file reads or hierarchical traversal.

Before executing the query, `search_photos` reads `summary.json` to verify `schemaVersion`. A version mismatch raises `ValueError` with a message prompting a full re-index.

### SQL clause mapping

| Predicate field | SQL clause |
|---|---|
| `date_min` / `date_max` | `date_taken >= TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` / `date_taken <= …` |
| `rating_min` / `rating_max` | `rating >= N` / `rating <= N` |
| `tags` (AND) | `array_has(tags, 'value')` per tag |
| `make` / `model` | `lower(col) LIKE '%substring%'` |
| `gps` bounding box | `gps_lat IS NOT NULL AND gps_lon IS NOT NULL [AND gps_lat >= … AND …]` |

`_esc` (imported from `lance_index`) doubles single quotes in string values to prevent SQL injection.

A `GpsBoxFilter` with all-None bounds still produces `gps_lat IS NOT NULL AND gps_lon IS NOT NULL` — ensuring photos without GPS data are excluded.

### `root` parameter

When `root` is non-empty, a partition prefix condition is prepended to the WHERE clause:
`(partition = '<root>' OR starts_with(partition, '<root>/'))` — restricting results to the specified subtree without a separate traversal pass.

### Result assembly

Each row returned by LanceDB is converted to a `PhotoMatch`:

- `partition`, `filename`, `content_hash`: passed through directly.
- `thumbnail_avif_hash`, `thumbnail_tile_index`: flat nullable columns. `None` when no thumbnail has been generated for the photo.
- `searchable`: rebuilt from typed columns by `_row_to_searchable` (GPS as tuple, tags as list, dates as `datetime`).

`partitions_scanned` is the count of distinct `partition` values across all matches — computed post-query.
`partitions_pruned` is always 0; partition-level filtering is handled by LanceDB internally.

## Date Handling and Timezone Stripping

LanceDB stores `date_taken` as a timezone-naive timestamp. SQL literals in WHERE clauses are also naive (`TIMESTAMP 'YYYY-MM-DD HH:MM:SS'`). Timezone-aware `datetime` values from `RangeFilter.lo` / `hi` have their timezone stripped with `.replace(tzinfo=None)` before formatting.

Partial date strings (`"2024"`, `"2024-07"`) are expanded in `agent.py` to full `datetime` bounds before passing to `searcher.py`. The searcher works only with `datetime | None`.

## Result Ordering

V1: results are returned in LanceDB scan order (undefined within a query). No date-based sorting. This is an acknowledged limitation (see OP-Q3 in [query_design.md](../ouestcharlie/query_design.md)).

## Error Handling

| Situation | Behavior |
|---|---|
| `summary.json` absent | Empty `SearchResult`, `errors == 0` (unindexed library) |
| `summary.json` schema version mismatch | `ValueError` raised — user must run a full re-index |
| LanceDB index absent despite valid `summary.json` | `ValueError` raised — index is corrupt or incomplete |
| LanceDB query failure | `errors += 1`, message in `error_details`, empty matches returned |
| Progress notification failure | Caught and logged at DEBUG; search continues |

## MCP Tools Summary

| Tool | Description |
|---|---|
| `search_photos` | Search photos by structured predicates; returns matches with tile index and thumbnail grid metadata |
| `list_search_fields` | Return all queryable fields with types and filter formats |
| `get_partition_summaries` | Return the root summary (all indexed partitions with statistics) |
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