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
├── test_full_text_filter.py  # FTS filter validation and TEXT-field WHERE clause behaviour
├── test_gps_filter.py        # GPS bounding box filter end-to-end tests
├── test_http_server.py
├── test_search_validation.py # Unknown filter field rejection
├── test_searcher.py
└── test_where_clause.py      # Unit tests for _build_where_clause SQL generation
```

`searcher.py` has no MCP dependency and can be unit-tested directly. `agent.py` is the thin adapter that registers tools with FastMCP and handles MCP-layer concerns (date string parsing, progress reporting, result dict serialization). 
`http_server.py` runs independently of the MCP layer in its own daemon threads.

## MCP Tool Interface

### `search_photos`

**Input** (all fields optional):

| Field | Type | Default | Description |
|---|---|---|---|
| `filters` | `object` | — | Dict mapping field names to filter values. Use `list_search_fields` to discover available fields and their formats. Supported filter types: date range, int/float range, string collection (AND), string substring, GPS bounding box. |
| `full_text_filter` | `object` | — | Full-text search over one or more `TEXT`-typed fields. Shape: `{"query": "Canyon", "columns": ["description"]}`. `query` is a single string applied across all listed columns; `columns` must be `entry_attr` names of `FieldType.TEXT` fields (see `list_search_fields → full_text_search.fields`). Results are relevance-ranked and each match includes `score`. Compatible with `filters`. |
| `partitions` | `string[]` | — | Explicit partition paths to search. When non-empty, an unfiltered scan of those partitions is allowed. |
| `sort_by` | `string` | `"date_taken"` | Column to sort by (ignored when `full_text_filter` is set — results are relevance-ranked). |
| `sort_order` | `string` | `"desc"` | `"asc"` or `"desc"`. |
| `page` | `int` | `0` | 0-indexed page number; passed directly to `LanceIndex.search_where`. |

**Output**:

| Field | Type | Description |
|---|---|---|
| `totalCount` | `int` | Total number of matching photos across all pages |
| `page` | `int` | Current 0-indexed page number |
| `pageSize` | `int` | Page size (500) |
| `hasMore` | `bool` | `true` when further pages exist (`(page + 1) * pageSize < totalCount`) |
| `errors` | `int` | Query failures |
| `errorDetails` | `string[]` | Per-failure messages |
| `matches` | `PhotoMatch[]` | One page of matching photo records (up to `pageSize`) |

`search_photos` does not compute a tag-facet breakdown — call `get_summary` with the same `filters` for that (and other aggregate stats), scoped identically. Computing facets on every page fetch was wasted work for callers that only need matches.

**PhotoMatch fields**: `partition`, `filename`, `contentHash`, `tileIndex`, `avifHash`, plus any searchable metadata fields driven by `PHOTO_FIELDS` (e.g. `dateTaken` as ISO 8601, `rating`, `tags`, `make`, `model`, `width`, `height`) — serialized by name using `FieldDef.name` as the JSON key. When a `full_text_filter` was applied, each match also includes `score` (LanceDB relevance score, higher is better).

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
  │                               1. lance_index.search_where(
  │                                    "content_hash=… AND partition=…", page_size=1)
  │                               2. reconstruct PhotoEntry via row_to_photo_entry
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
| `dateTaken` range | `date_taken >= TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` / `date_taken <= …` |
| int/float range (rating, width, …) | `col >= N` / `col <= N` |
| `tags` (AND) | `array_has(tags, 'value')` per tag |
| string match (make, model, …) | `lower(col) LIKE '%substring%'` |
| GPS bounding box | `gps_lat IS NOT NULL AND gps_lon IS NOT NULL [AND gps_lat >= … AND …]` |
| `FieldType.TEXT` fields | **No SQL clause** — handled via `full_text_filter` / LanceDB FTS |

`_esc` (imported from `lance_index`) doubles single quotes in string values to prevent SQL injection.

A `GpsBoxFilter` with all-None bounds still produces `gps_lat IS NOT NULL AND gps_lon IS NOT NULL` — ensuring photos without GPS data are excluded.

### Full-text search path

When `full_text_filter` is provided, `search_where` uses LanceDB's `nearest_to_text(query, columns=[...])` instead of `order_by` + `offset` + `limit`. Results are returned in relevance order; `_score` is included automatically by LanceDB and exposed per match. SQL `filters` are still applied on top (the base query is built with the same `where_clause`). `FieldType.TEXT` fields in `filters` are silently skipped — they produce no SQL clause.

### `root` parameter

When `root` is non-empty, a partition prefix condition is prepended to the WHERE clause:
`(partition = '<root>' OR starts_with(partition, '<root>/'))` — restricting results to the specified subtree without a separate traversal pass.

### Result assembly

Each row returned by LanceDB is converted to a `PhotoMatch`:

- `partition`, `filename`, `content_hash`: passed through directly.
- `thumbnail_avif_hash`, `thumbnail_tile_index`: flat nullable columns. `None` when no thumbnail has been generated for the photo.
- `searchable`: rebuilt from typed columns by `_row_to_searchable` (GPS as tuple, tags as list, dates as `datetime`).

## Date Handling and Timezone Stripping

LanceDB stores `date_taken` as a timezone-naive timestamp. SQL literals in WHERE clauses are also naive (`TIMESTAMP 'YYYY-MM-DD HH:MM:SS'`). Timezone-aware `datetime` values from `RangeFilter.lo` / `hi` have their timezone stripped with `.replace(tzinfo=None)` before formatting.

Partial date strings (`"2024"`, `"2024-07"`) are expanded in `agent.py` to full `datetime` bounds before passing to `searcher.py`. The searcher works only with `datetime | None`.

Date bounds also accept a time-of-day component via a `T` separator (`"2024-07-14T18"`, `"2024-07-14T18:30"`, `"2024-07-14T18:30:00"`), enabling precise time-range filtering (e.g. photos taken between 18:00 and 20:00). Missing minute/second fields default to 0, and any timezone designator is stripped. This is purely an MCP-adapter parsing concern in `_parse_date_min` / `_parse_date_max`; `searcher.py` and the storage layer are agnostic to date-vs-timestamp granularity since both already operate on full `datetime` values.

## Result Ordering and Pagination

Results are sorted by `sort_by` column (default `date_taken`) in `sort_order` direction (default `desc` — newest first) using LanceDB's native `AsyncQuery.order_by(List[ColumnOrdering])`. A `filename` tiebreaker is always appended to make pagination deterministic when the primary key (e.g. `date_taken`) is NULL or duplicated across rows.

When `full_text_filter` is set, results are ranked by relevance instead (`nearest_to_text`) — `sort_by` / `sort_order` are ignored.

Results are paginated at 500 photos per page (`PAGE_SIZE`). `search_where` returns `(page_rows, total_count)` — callers use `total_count` to compute `hasMore`. Tag facets are not part of this return value; `LanceIndex.tag_facets_where(where_clause)` is a separate method, called only by `get_summary` (see below), not on every page fetch.

## Error Handling

| Situation | Behavior |
|---|---|
| `summary.json` absent | `ValueError` raised — user must run a full index (unindexed library) |
| `summary.json` schema version mismatch | `ValueError` raised — user must run a full re-index |
| LanceDB index absent despite valid `summary.json` | `ValueError` raised — index is corrupt or incomplete |
| LanceDB query failure | `errors += 1`, message in `error_details`, empty matches returned |
| Progress notification failure | Caught and logged at DEBUG; search continues |

`get_summary` shares the same schema-version check and error handling as `search_photos` (both go through `_open_verified_index` / `_verify_index_ready` in `searcher.py`).

## MCP Tools Summary

| Tool | Description |
|---|---|
| `search_photos` | Search photos by structured predicates; returns matches with tile index and thumbnail grid metadata |
| `get_summary` | Aggregate stats (count, date/rating/width/height/GPS ranges, tag facets) for photos matching the same filter syntax as `search_photos`, computed on demand from the LanceDB index — no precomputed data. Empty `filters` summarizes the whole library. The only tool that returns a tag-facet breakdown. See [Runtime Summaries](../HLD.md#runtime-summaries) in the HLD for the rationale (replaces the old precomputed root `summary.json` partition list). |
| `list_search_fields` | Return all queryable fields with types and filter formats |
| `get_http_port_tool` | Return the port Wally's HTTP preview server is listening on (diagnostic) |

### `get_summary` implementation

`searcher.get_summary(backend, predicate)` reuses `_build_where_clause`/`_build_group` — the exact same filter-to-SQL translation as `search_photos` (see [SQL clause mapping](#sql-clause-mapping) above) — then runs two independent queries against the same `where_clause`:
1. `ouestcharlie_toolkit.partition_summary.aggregate_where(lance_index, where_clause)`, a single DuckDB aggregation (`COUNT`/`MIN`/`MAX`) over the Arrow table LanceDB returns for that WHERE clause.
2. `lance_index.tag_facets_where(where_clause)`, a lightweight scan of the `tags` column producing a `{tag: count}` map.

It returns `(ManifestSummary, tag_facets)`; the MCP tool merges both into one response dict with `tagFacets` alongside the range stats. The `filters` wire format, parsing (`_parse_filter_node` in `agent.py`), and validation are identical to `search_photos` — the MCP tool docstrings share one `_FILTER_SYNTAX_DOC` constant so the syntax is documented once, not per tool.

## Scope and Deferred Items

**In scope:**
- Predicates: date range, tags (AND), rating range, camera make/model substring, GPS bounding box
- Full-text search over `TEXT`-typed fields (currently `description`) via `full_text_filter`
- Relevance-ranked results with `score` per match when FTS is used
- `tagFacets` aggregation over the full matching set (via `get_summary`, not `search_photos`)
- Native LanceDB sort with deterministic `filename` tiebreaker
- Result pagination (500 photos per page)
- Single backend per Woof invocation
- On-demand JPEG preview generation and caching

**Deferred:**
- Tag bloom filter pruning at parent level (OP from query_design.md)
- Cross-backend deduplication (Woof's responsibility — OP-Q4)
- Lucene DSL string input (lives in Woof for album definitions; Woof passes structured predicates to Wally)