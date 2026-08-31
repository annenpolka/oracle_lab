"""Pure presentation views over archived artifact integrity records."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol


class _ArtifactRecord(Protocol):
    name: str
    path: Path
    sha256: str
    size_bytes: int


def artifact_manifest_view(
    artifacts: Iterable[_ArtifactRecord],
) -> dict[str, dict[str, str | int]]:
    """Return the existing event-manifest view without reading artifact files."""

    return {
        artifact.name: {
            "path": str(artifact.path),
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
        }
        for artifact in artifacts
    }


__all__ = ["artifact_manifest_view"]
