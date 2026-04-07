# OuEstCharlie — Wally

Wally is the search/consumption agent for OuEstCharlie. It is **stateless and read-only**: Woof launches it as a child process (MCP server over stdio), passes a structured search predicate via `search_photos`, and Wally returns matching photo metadata by traversing the manifest tree. It never reads XMP sidecars or writes anything.

## Design Documents

| Document | Purpose |
|----------|---------|
| [wally_LLD.md](wally_LLD.md) | Low-level design |

## Repository Structure

```
src/wally/
├── __main__.py   # Entry point (stdio MCP server)
├── agent.py      # WallyAgent(AgentBase) — MCP tool registration, date parsing
└── searcher.py   # Pure async search logic — no MCP dependency, independently testable

tests/
└── test_searcher.py
```

`searcher.py` has no MCP dependency and can be unit-tested directly. `agent.py` is the thin adapter that registers the tool with FastMCP and handles date string parsing and result serialization.

## Installation

### From PyPI (recommended)

```bash
pip install wally
```

### From source (development)

Requires the sibling `ouestcharlie-py-toolkit` repo:

```bash
uv venv
uv sync
```

## Running Tests

**Always use `.venv/bin/python -m pytest`:**

```bash
.venv/bin/python -m pytest tests/ -v
```

## Context

| Repository | Purpose |
|------------|---------|
| [ouestcharlie](https://github.com/ouestcharlie/ouestcharlie/) | Architecture docs, HLR/HLD, MCP interface |
| [ouestcharlie-py-toolkit](https://github.com/ouestcharlie/ouestcharlie-py-toolkit) | Python toolkit for agents |
| [ouestcharlie-whitebeard](https://github.com/ouestcharlie/ouestcharlie-whitebeard) | Indexing agent |
| [**ouestcharlie-wally** *(this repo)*](https://github.com/ouestcharlie/ouestcharlie-wally) | Search/consumption agent |
| [ouestcharlie-woof](https://github.com/ouestcharlie/ouestcharlie-woof) | Woof controller |

See [ouestcharlie/HLD.md](https://github.com/ouestcharlie/ouestcharlie/blob/master/HLD.md) for the overall system architecture.