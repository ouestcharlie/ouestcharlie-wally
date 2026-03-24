"""Tests for the get_partition_summaries logic.

The tool calls manifest_store.read_summary() then serializes the result.
Tests exercise that pipeline directly without going through the MCP layer.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    SCHEMA_VERSION,
    ManifestSummary,
    RootSummary,
    serialize_summary,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path))


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _summary(path: str, photo_count: int = 1) -> ManifestSummary:
    return ManifestSummary(path=path, photo_count=photo_count, _stats={})


# ---------------------------------------------------------------------------
# get_partition_summaries — unindexed backend
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unindexed_backend_returns_unindexed_flag(store: ManifestStore) -> None:
    """Missing summary.json → {"unindexed": True} (not an error)."""
    try:
        summary, _ = await store.read_summary()
    except FileNotFoundError:
        result = {"unindexed": True}
    else:
        result = serialize_summary(summary)

    assert result == {"unindexed": True}


# ---------------------------------------------------------------------------
# get_partition_summaries — indexed backend
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_summary_serialized(store: ManifestStore) -> None:
    """Root summary is returned with schemaVersion and partitions list."""
    root_summary = RootSummary(
        schema_version=SCHEMA_VERSION,
        partitions=[
            _summary("2024", photo_count=10),
            _summary("2023", photo_count=5),
        ],
    )
    await store.create_summary(root_summary)

    summary, _ = await store.read_summary()
    result = serialize_summary(summary)

    assert result["schemaVersion"] == SCHEMA_VERSION
    assert len(result["partitions"]) == 2
    assert result["partitions"][0]["path"] == "2024"
    assert result["partitions"][1]["path"] == "2023"


@pytest.mark.asyncio
async def test_summary_partition_stats_preserved(store: ManifestStore) -> None:
    """Partition with date stats is preserved in the serialized output."""
    partition = ManifestSummary(
        path="2024",
        photo_count=3,
        _stats={
            "dateTaken": {
                "type": "date_range",
                "min": datetime(2024, 1, 1),
                "max": datetime(2024, 12, 31),
            },
        },
    )
    root_summary = RootSummary(schema_version=SCHEMA_VERSION, partitions=[partition])
    await store.create_summary(root_summary)

    summary, _ = await store.read_summary()
    result = serialize_summary(summary)

    p = result["partitions"][0]
    assert p["path"] == "2024"
    assert "dateTaken" in p
