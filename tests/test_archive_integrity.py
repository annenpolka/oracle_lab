from __future__ import annotations

import datetime as dt
import json

import pytest

from oracle_lab.archive import ArchiveIntegrityError, RawResponseArchive
from oracle_lab.jsonutil import sha256_bytes


def test_archive_preserves_raw_bytes_and_reserves_integrity_metadata(tmp_path) -> None:
    archive = RawResponseArchive(tmp_path / "raw")
    raw = b'{  "spacing" : [1, 2], "unicode": "\xe3\x81\x82" }\n'
    created = dt.datetime(2026, 8, 30, 10, 0, tzinfo=dt.UTC)

    record = archive.write(
        event_id="evt_test",
        raw_bytes=raw,
        created_at=created,
        metadata={
            "event_id": "attacker",
            "raw_file": "../../escape",
            "raw_sha256": "wrong",
            "provider_name": "fixture",
        },
    )

    assert record.raw_path.read_bytes() == raw
    sidecar = archive.read_sidecar(record)
    assert sidecar["event_id"] == "evt_test"
    assert sidecar["raw_file"] == "evt_test.json"
    assert sidecar["raw_sha256"] == sha256_bytes(raw)
    assert record.raw_path == tmp_path / "raw/2026/08/30/evt_test.json"


def test_archive_rejects_sidecar_path_traversal(tmp_path) -> None:
    archive = RawResponseArchive(tmp_path / "raw")
    record = archive.write(
        event_id="evt_test",
        raw_bytes=b"{}",
        metadata={},
        created_at=dt.datetime(2026, 8, 30, tzinfo=dt.UTC),
    )
    sidecar = json.loads(record.metadata_path.read_text(encoding="utf-8"))
    sidecar["raw_file"] = "../outside.json"
    record.metadata_path.write_text(json.dumps(sidecar), encoding="utf-8")

    with pytest.raises(ArchiveIntegrityError, match="unsafe raw_file"):
        archive.read_sidecar(record.metadata_path)
