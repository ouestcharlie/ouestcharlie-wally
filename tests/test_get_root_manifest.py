"""Tests for the get_root_manifest_tool logic.

The tool calls manifest_store.read_any("") then serializes the result.
Tests exercise that pipeline directly without going through the MCP layer.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from ouestcharlie_toolkit.backends.local import LocalBackend
from ouestcharlie_toolkit.manifest import ManifestStore
from ouestcharlie_toolkit.schema import (
    METADATA_DIR,
    SCHEMA_VERSION,
    LeafManifest,
    ParentManifest,
    PartitionSummary,
    PhotoEntry,
    serialize_leaf,
    serialize_parent,
)


# ---------------------------------------------------------------------------
# Helpers (mirrors test_searcher.py conventions)
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(root=str(tmp_path))


@pytest.fixture()
def store(backend: LocalBackend) -> ManifestStore:
    return ManifestStore(backend)


def _entry(filename: str = "photo.jpg", content_hash: str = "sha256:aabbcc") -> PhotoEntry:
    return PhotoEntry(filename=filename, content_hash=content_hash, searchable={})


def _summary(path: str, photo_count: int = 1) -> PartitionSummary:
    return PartitionSummary(path=path, _stats={})


# ---------------------------------------------------------------------------
# get_root_manifest_tool — unindexed backend
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unindexed_backend_returns_unindexed_flag(backend: LocalBackend) -> None:
    """Missing root manifest → {"unindexed": True} (not an error)."""
    store = ManifestStore(backend)
    try:
        manifest, _ = await store.read_any("")
    except FileNotFoundError:
        result = {"unindexed": True}
    else:
        result = serialize_leaf(manifest) if isinstance(manifest, LeafManifest) else serialize_parent(manifest)

    assert result == {"unindexed": True}


# ---------------------------------------------------------------------------
# get_root_manifest_tool — parent manifest at root
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parent_manifest_serialized(store: ManifestStore) -> None:
    """Root parent manifest is returned with schemaVersion, path, and children."""
    parent = ParentManifest(
        schema_version=SCHEMA_VERSION,
        path="",
        children=[
            _summary("2024", photo_count=10),
            _summary("2023", photo_count=5),
        ],
    )
    await store.create_parent(parent)

    manifest, _ = await store.read_any("")
    assert isinstance(manifest, ParentManifest)
    result = serialize_parent(manifest)

    assert result["schemaVersion"] == SCHEMA_VERSION
    assert result["path"] == ""
    assert len(result["children"]) == 2
    assert result["children"][0]["path"] == "2024"
    assert result["children"][1]["path"] == "2023"


@pytest.mark.asyncio
async def test_parent_manifest_children_include_summary_stats(store: ManifestStore) -> None:
    """Children with date stats are preserved in the serialized output."""
    child = PartitionSummary(
        path="2024",
        _stats={
            "dateTaken": {
                "type": "date_range",
                "min": datetime(2024, 1, 1),
                "max": datetime(2024, 12, 31),
            },
        },
    )
    parent = ParentManifest(schema_version=SCHEMA_VERSION, path="", children=[child])
    await store.create_parent(parent)

    manifest, _ = await store.read_any("")
    result = serialize_parent(manifest)  # type: ignore[arg-type]

    child_dict = result["children"][0]
    assert child_dict["path"] == "2024"
    assert "dateTaken" in child_dict


# ---------------------------------------------------------------------------
# get_root_manifest_tool — leaf manifest at root (flat library)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_leaf_manifest_at_root_serialized(store: ManifestStore) -> None:
    """Root leaf manifest (flat library) is returned with schemaVersion, partition, photos."""
    photos = [
        _entry("a.jpg", "sha256:aa"),
        _entry("b.jpg", "sha256:bb"),
    ]
    leaf = LeafManifest(schema_version=SCHEMA_VERSION, partition="", photos=photos)
    await store.create_leaf(leaf)

    manifest, _ = await store.read_any("")
    assert isinstance(manifest, LeafManifest)
    result = serialize_leaf(manifest)

    assert result["schemaVersion"] == SCHEMA_VERSION
    assert result["partition"] == ""
    assert len(result["photos"]) == 2
    filenames = {p["filename"] for p in result["photos"]}
    assert filenames == {"a.jpg", "b.jpg"}
