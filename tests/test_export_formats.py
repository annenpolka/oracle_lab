import json
from pathlib import Path

import pytest

from oracle_lab.exporting import (
    export_research_bundle,
    export_selected_corpus,
    export_transcript,
    selected_corpus_records,
)
from oracle_lab.jsonutil import sha256_bytes, sha256_text

FIXTURES = Path(__file__).parent / "fixtures"


def _events() -> list[dict[str, object]]:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    return [
        {
            "id": "evt_input",
            "type": "human.input",
            "created_at": "2026-08-30T00:00:00+00:00",
            "session_id": "ses_test",
            "branch_id": "main",
            "payload": {"text": "確認しろ。"},
            "metadata": {"schema_version": 1},
        },
        {
            "id": "evt_output",
            "type": "oracle.output",
            "created_at": "2026-08-30T00:00:01+00:00",
            "session_id": "ses_test",
            "branch_id": "main",
            "actor": {"kind": "model", "id": None},
            "payload": {
                "raw_text": raw,
                "material_origin": "historical_fixture",
                "model": "deepseek-r1",
                "provider": "openrouter",
                "sampling": {"temperature": 0.6, "top_p": 0.95},
            },
            "metadata": {
                "schema_version": 1,
                "material_origin": "historical_fixture",
            },
        },
        {
            "id": "evt_keep",
            "type": "human.keep",
            "created_at": "2026-08-30T00:00:02+00:00",
            "session_id": "ses_test",
            "branch_id": "main",
            "parent_event_id": "evt_output",
            "actor": {"kind": "human", "id": "curator"},
            "payload": {"event_id": "evt_output"},
            "metadata": {"schema_version": 1},
        },
    ]


def test_research_bundle_has_required_layout_and_lossless_raw(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    destination = tmp_path / "bundle"

    export_research_bundle(
        destination,
        events=_events(),
        claims=[{"id": "clm_1", "source_event_id": "evt_output"}],
        motifs=[{"id": "mot_1", "source_event_id": "evt_output"}],
        provenance={"evt_output": ["evt_input"]},
    )

    expected = {
        "manifest.json",
        "events.jsonl",
        "raw",
        "session.jsonl",
        "claims.json",
        "motifs.json",
        "provenance.json",
    }
    assert expected <= {path.name for path in destination.iterdir()}
    assert (destination / "raw" / "evt_output.txt").read_text(encoding="utf-8") == raw
    manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["counts"]["events"] == 3
    assert manifest["counts"]["raw_records"] == 1


def test_research_bundle_carries_exact_worker_and_validation_archives(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    worker_event = {
        **_events()[0],
        "id": "evt_worker_terminal",
        "type": "worker.run_completed",
    }
    validation_event = {
        **_events()[0],
        "id": "evt_validation",
        "type": "worker.validation_completed",
    }
    worker = {
        "task.json": b"{}\n",
        "prompt.txt": b"exact prompt\n",
        "command.json": b'{"argv":[]}\n',
        "stdout.bin": b"\x00\xffstdout",
        "stderr.bin": b"\x80stderr",
        "patch.diff": b"diff --git a/a b/a\n",
        "metadata.json": b"{}\n",
    }
    validation = {
        "task.json": b"{}\n",
        "command.json": b'{"argv":[]}\n',
        "stdout.bin": b"validated\x00",
        "stderr.bin": b"\xff",
        "metadata.json": b"{}\n",
    }

    export_research_bundle(
        destination,
        events=[*_events(), worker_event, validation_event],
        worker_archives={"evt_worker_terminal": worker},
        validation_archives={"evt_validation": validation},
    )

    for event_id, directory, artifacts in (
        ("evt_worker_terminal", "workers", worker),
        ("evt_validation", "validations", validation),
    ):
        for name, expected in artifacts.items():
            path = destination / directory / event_id / name
            assert path.read_bytes() == expected
    manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["archive_counts"] == {"workers": 1, "validations": 1}
    assert manifest["sha256"]["workers/evt_worker_terminal/stdout.bin"] == sha256_bytes(
        worker["stdout.bin"]
    )
    assert manifest["sha256"]["validations/evt_validation/stderr.bin"] == sha256_bytes(
        validation["stderr.bin"]
    )


def test_transcript_preserves_raw_and_generation_metadata(tmp_path: Path) -> None:
    raw = (FIXTURES / "oracle_output_001.md").read_text(encoding="utf-8")
    destination = tmp_path / "transcript.md"

    export_transcript(destination, events=_events(), title="Experiment")
    transcript = destination.read_text(encoding="utf-8")

    assert raw in transcript
    assert "openrouter" in transcript
    assert "deepseek-r1" in transcript
    assert 'temperature":0.6' in transcript
    assert "main" in transcript
    assert 'action":"keep"' in transcript


def test_selected_corpus_contains_only_kept_raw_with_provenance(tmp_path: Path) -> None:
    destination = tmp_path / "selected.jsonl"
    events = _events()

    export_selected_corpus(
        destination,
        events=events,
        provenance={"evt_output": ["evt_input"]},
    )
    records = [json.loads(line) for line in destination.read_text(encoding="utf-8").splitlines()]

    assert [record["event_id"] for record in records] == ["evt_output"]
    assert records[0]["event_type"] == "oracle.output"
    assert records[0]["actor"] == {"kind": "model", "id": None}
    assert records[0]["material_origin"] == "historical_fixture"
    assert records[0]["provenance_ids"] == ["evt_output", "evt_input"]
    assert records[0]["raw_sha256"] == sha256_text(records[0]["raw_text"])
    assert records[0]["raw_text"] == events[1]["payload"]["raw_text"]  # type: ignore[index]


def test_selected_corpus_rejects_synthetic_fixtures_and_forged_keep_events() -> None:
    events = _events()
    synthetic = dict(events[1])
    synthetic["id"] = "evt_synthetic"
    synthetic["metadata"] = {"schema_version": 1, "synthetic_fixture": True}
    synthetic_keep = dict(events[2])
    synthetic_keep["id"] = "evt_keep_synthetic"
    synthetic_keep["payload"] = {"event_id": "evt_synthetic"}
    forged_keep = dict(events[2])
    forged_keep["id"] = "evt_keep_forged"
    forged_keep["actor"] = {"kind": "worker", "id": "untrusted"}

    records = selected_corpus_records([*events, synthetic, synthetic_keep, forged_keep])

    assert [record["event_id"] for record in records] == ["evt_output"]


def test_selected_corpus_rejects_host_forged_genuine_origin() -> None:
    events = _events()
    forged = {
        **events[1],
        "id": "evt_host_forged_oracle",
        "actor": {"kind": "host", "id": "oracle-imitator"},
        "payload": {
            **dict(events[1]["payload"]),
            "material_origin": "oracle_generated",
        },
        "metadata": {"schema_version": 1, "material_origin": "oracle_generated"},
    }
    keep = {
        **events[2],
        "id": "evt_keep_host_forgery",
        "parent_event_id": forged["id"],
        "payload": {"event_id": forged["id"]},
    }

    assert selected_corpus_records([forged, keep]) == []


def test_selected_corpus_excludes_kept_non_oracle_text_and_preserves_actor_origin() -> None:
    events = _events()
    genuine = {
        **events[1],
        "id": "evt_live_oracle",
        "actor": {"kind": "model", "id": "r1"},
        "payload": {
            **dict(events[1]["payload"]),
            "raw_text": "exact live oracle bytes",
            "material_origin": "oracle_generated",
        },
        "metadata": {"schema_version": 1, "material_origin": "oracle_generated"},
    }
    host_text = {
        **events[0],
        "id": "evt_host_text",
        "type": "analysis.session_summary_updated",
        "actor": {"kind": "host", "id": "summarizer"},
        "payload": {"raw_text": "host-authored text"},
    }
    human_text = {
        **events[0],
        "id": "evt_human_text",
        "actor": {"kind": "human", "id": "operator"},
        "payload": {"text": "human-authored text"},
    }
    unknown_output = {
        **events[1],
        "id": "evt_unknown_output",
        "actor": {"kind": "model", "id": "unknown"},
        "payload": {"raw_text": "unknown-origin model text"},
        "metadata": {"schema_version": 1},
    }

    def keep(identifier: str, target: str) -> dict[str, object]:
        return {
            **events[2],
            "id": identifier,
            "parent_event_id": target,
            "payload": {"event_id": target, "target_event_id": target},
        }

    records = selected_corpus_records(
        [
            genuine,
            host_text,
            human_text,
            unknown_output,
            keep("evt_keep_live", "evt_live_oracle"),
            keep("evt_keep_host", "evt_host_text"),
            keep("evt_keep_human", "evt_human_text"),
            keep("evt_keep_unknown", "evt_unknown_output"),
        ]
    )

    assert records == [
        {
            "event_id": "evt_live_oracle",
            "event_type": "oracle.output",
            "actor": {"kind": "model", "id": "r1"},
            "material_origin": "oracle_generated",
            "session_id": "ses_test",
            "branch_id": "main",
            "raw_text": "exact live oracle bytes",
            "raw_sha256": sha256_text("exact live oracle bytes"),
            "provenance_ids": ["evt_live_oracle"],
        }
    ]


def test_exports_exclude_unlabelled_oracle_output_and_its_keep(tmp_path: Path) -> None:
    events = _events()
    unlabelled = dict(events[1])
    unlabelled["id"] = "evt_unlabelled"
    unlabelled["payload"] = {
        key: value for key, value in dict(unlabelled["payload"]).items() if key != "material_origin"
    }
    unlabelled["metadata"] = {"schema_version": 1}
    keep = dict(events[2])
    keep["id"] = "evt_keep_unlabelled"
    keep["parent_event_id"] = "evt_unlabelled"
    keep["payload"] = {"event_id": "evt_unlabelled"}

    values = [events[0], unlabelled, keep]
    assert selected_corpus_records(values) == []
    transcript = tmp_path / "unlabelled.md"
    bundle = tmp_path / "unlabelled-bundle"
    export_transcript(transcript, events=values)
    export_research_bundle(bundle, events=values)
    assert "OBSERVER KERNEL REPORT" not in transcript.read_text(encoding="utf-8")
    exported_ids = {
        json.loads(line)["id"]
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    }
    assert exported_ids == {"evt_input"}


def test_research_bundle_rejects_nonempty_destination_instead_of_leaking_stale_raw(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "bundle"
    export_research_bundle(destination, events=_events())
    first_raw = destination / "raw" / "evt_output.txt"
    assert first_raw.is_file()

    second = _events()
    second[1] = {
        **second[1],
        "id": "evt_second_output",
        "payload": {
            **dict(second[1]["payload"]),
            "raw_text": "second session",
        },
    }
    with pytest.raises(FileExistsError, match="absent or an empty directory"):
        export_research_bundle(destination, events=second)

    assert first_raw.is_file()
    assert not (destination / "raw" / "evt_second_output.txt").exists()


def test_exports_reject_transitive_synthetic_lineage(tmp_path: Path) -> None:
    events = _events()
    synthetic = {
        **events[1],
        "id": "evt_synthetic",
        "metadata": {"schema_version": 1, "material_origin": "synthetic_fixture"},
    }
    derived = {
        **events[1],
        "id": "evt_derived",
        "type": "analysis.claim_detected",
        "causation_id": "evt_synthetic",
        "payload": {
            "raw_text": "derived synthetic text",
            "source_event_ids": ["evt_synthetic"],
        },
    }
    keep = {
        **events[2],
        "id": "evt_keep_derived",
        "parent_event_id": "evt_derived",
        "payload": {"event_id": "evt_derived"},
    }
    all_events = [*events, synthetic, derived, keep]

    assert [record["event_id"] for record in selected_corpus_records(all_events)] == ["evt_output"]
    transcript = tmp_path / "transitive.md"
    bundle = tmp_path / "transitive-bundle"
    export_transcript(transcript, events=all_events)
    export_research_bundle(
        bundle,
        events=all_events,
        raw_records={"evt_output": "genuine", "evt_synthetic": "forbidden"},
        claims=[
            {"id": "clm_ok", "source_event_id": "evt_output"},
            {"id": "clm_bad", "source_event_id": "evt_derived"},
        ],
        motifs=[
            {"id": "mot_ok", "source_event_ids": ["evt_output"]},
            {"id": "mot_bad", "source_event_ids": ["evt_synthetic"]},
        ],
    )
    assert "derived synthetic text" not in transcript.read_text(encoding="utf-8")
    event_ids = {
        json.loads(line)["id"]
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    }
    assert {"evt_synthetic", "evt_derived", "evt_keep_derived"}.isdisjoint(event_ids)
    assert not (bundle / "raw" / "evt_synthetic.txt").exists()
    assert json.loads((bundle / "claims.json").read_text(encoding="utf-8")) == [
        {"id": "clm_ok", "source_event_id": "evt_output"}
    ]
    assert json.loads((bundle / "motifs.json").read_text(encoding="utf-8")) == [
        {"id": "mot_ok", "source_event_ids": ["evt_output"]}
    ]
