# ouestcharlie-wally — Claude Working Rules

## Testing

Never use `python`, `python3`, or `uv run pytest` — use the project's own `.venv`:

```
.venv/bin/pytest tests/ -v
```

## Linting

Ruff is a dev dependency, pinned in `pyproject.toml` (locked in `uv.lock`) — the same version the pre-commit hook runs. Use the venv's copy, never `uvx ruff` or a global `ruff` (unpinned version, results may differ from the hook):

```
.venv/bin/ruff check .
.venv/bin/ruff format --check .
```

To upgrade it, change the pin with `uv add --dev ruff==<version>`.

## MCP Error Handling

- **Always log exceptions** in MCP tool handlers — FastMCP swallows unhandled errors silently, so without explicit logging they are invisible.
- Wrap `ctx.report_progress()` calls in `try/except` and log failures at DEBUG level. The MCP client may disconnect or time out while the tool is still running; a failed progress notification must never abort the operation.
- For long-running tools, the MCP Inspector timeout must be increased in its settings (default is too low for heavy queries).

## Code Style

- **All imports at the top of the file** — never use lazy imports inside functions or methods.

## Key Design Rules

- `searcher.py` is **pure async logic** with no MCP dependency — easy to unit test independently.
- `agent.py` wraps `searcher.py` in `AgentBase` and registers it as the MCP tool.
- Wally is **read-only** — it never writes XMP sidecars or manifests.
- All queryable metadata is inlined in leaf manifests by Whitebeard. Wally never reads XMP sidecars.
- Use `ManifestStore.read_any()` for traversal — dispatches on JSON `photos` vs `children` key.
- A missing manifest at root = unindexed library → return empty `SearchResult` (not an error).
- Date comparisons always use `_naive()` (strip timezone) — same pattern as `whitebeard/indexer.py`.

See [wally_LLD.md](../wally_LLD.md) for the full design.
