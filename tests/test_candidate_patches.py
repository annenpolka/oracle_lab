from __future__ import annotations

import os
import subprocess
from dataclasses import FrozenInstanceError, dataclass, replace
from pathlib import Path

import pytest

import oracle_lab.patches as patches_module
from oracle_lab.git_control import fingerprint_git_control
from oracle_lab.jsonutil import sha256_bytes
from oracle_lab.patches import (
    CandidatePatch,
    CandidatePatchError,
    PatchApplicationError,
    PatchDecision,
    PatchDecisionKind,
    PatchDecisionState,
    apply_candidate_to_staging,
    evaluate_patch_decisions,
    preflight_candidate_patch,
)

_BASE = "a" * 40
_VALID_TEXT_DIFF = b"""diff --git a/safe.txt b/safe.txt
index 257cc56..3bd1f0e 100644
--- a/safe.txt
+++ b/safe.txt
@@ -1 +1 @@
-old
+new
"""


def _git(repository: Path, *arguments: str, input_bytes: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "-C", str(repository), *arguments],
        input=input_bytes,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr.decode("utf-8", "replace"))
    return result.stdout


@dataclass(frozen=True, slots=True)
class _RepositoryPatch:
    repository: Path
    candidate: CandidatePatch
    original_text: bytes
    original_binary: bytes
    changed_text: bytes
    changed_binary: bytes


def _repository_patch(tmp_path: Path) -> _RepositoryPatch:
    repository = tmp_path / "source"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Oracle Lab Test")
    _git(repository, "config", "user.email", "oracle-lab@example.invalid")
    _git(repository, "config", "core.fileMode", "true")
    original_text = b"old line\n"
    original_binary = b"\x00old\xffbinary\n"
    changed_text = b"new line\n"
    changed_binary = b"\x00new\xfebinary\n"
    (repository / "safe.txt").write_bytes(original_text)
    (repository / "binary.dat").write_bytes(original_binary)
    (repository / ".gitattributes").write_text(
        "*.txt filter=attack diff=attack\n",
        encoding="utf-8",
    )
    _git(repository, "add", "--", "safe.txt", "binary.dat", ".gitattributes")
    _git(repository, "commit", "-q", "-m", "base")
    base_commit = _git(repository, "rev-parse", "HEAD").decode().strip()

    (repository / "safe.txt").write_bytes(changed_text)
    (repository / "binary.dat").write_bytes(changed_binary)
    diff_bytes = _git(
        repository,
        "diff",
        "--binary",
        "--no-ext-diff",
        "--no-renames",
        base_commit,
        "--",
    )
    raw_paths = _git(
        repository,
        "diff",
        "--name-only",
        "-z",
        "--no-renames",
        base_commit,
        "--",
    )
    changed_paths = tuple(value.decode() for value in raw_paths.split(b"\0") if value)
    precondition_sha256 = {
        "binary.dat": sha256_bytes(original_binary),
        "safe.txt": sha256_bytes(original_text),
    }
    changed_modes = {path: "100644" for path in changed_paths}

    (repository / "safe.txt").write_bytes(original_text)
    (repository / "binary.dat").write_bytes(original_binary)
    assert _git(repository, "status", "--porcelain=v1", "-z") == b""
    candidate = CandidatePatch.from_capture(
        worker_run_id="run_patch",
        source_event_ids=("evt_source",),
        base_commit=base_commit,
        workspace_head=base_commit,
        diff_bytes=diff_bytes,
        patch_sha256=sha256_bytes(diff_bytes),
        changed_paths=changed_paths,
        precondition_sha256=precondition_sha256,
        changed_modes=changed_modes,
    )
    return _RepositoryPatch(
        repository=repository,
        candidate=candidate,
        original_text=original_text,
        original_binary=original_binary,
        changed_text=changed_text,
        changed_binary=changed_binary,
    )


def _candidate_from_diff(
    diff_bytes: bytes = _VALID_TEXT_DIFF,
    *,
    path: str = "safe.txt",
    workspace_head: str = _BASE,
    changed_mode: str | None = "100644",
) -> CandidatePatch:
    return CandidatePatch.from_capture(
        worker_run_id="run_static",
        source_event_ids=("evt_source",),
        base_commit=_BASE,
        workspace_head=workspace_head,
        diff_bytes=diff_bytes,
        patch_sha256=sha256_bytes(diff_bytes),
        changed_paths=(path,),
        precondition_sha256={path: "0" * 64},
        changed_modes={path: changed_mode},
    )


def _decision(
    candidate: CandidatePatch,
    kind: PatchDecisionKind,
    *,
    decision_id: str,
    patch_event_id: str = "evt_patch",
    actor_kind: str = "human",
) -> PatchDecision:
    return PatchDecision(
        decision_event_id=decision_id,
        patch_event_id=patch_event_id,
        worker_run_id=candidate.worker_run_id,
        patch_sha256=candidate.patch_sha256,
        base_commit=candidate.base_commit,
        decision=kind,
        actor_kind=actor_kind,
    )


def test_candidate_patch_preserves_binary_diff_and_freezes_all_identity(tmp_path: Path) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate

    assert b"GIT binary patch" in candidate.diff_bytes
    assert candidate.patch_sha256 == sha256_bytes(candidate.diff_bytes)
    assert candidate.changed_paths == ("binary.dat", "safe.txt")
    assert tuple(item.path for item in candidate.preconditions) == candidate.changed_paths
    assert all(item.mode == "100644" for item in candidate.preconditions)
    assert all(item.result_mode == "100644" for item in candidate.preconditions)
    assert candidate.patch_bytes is candidate.diff_bytes
    assert dict(candidate.precondition_sha256) == {
        "binary.dat": sha256_bytes(captured.original_binary),
        "safe.txt": sha256_bytes(captured.original_text),
    }
    assert dict(candidate.precondition_modes) == {"binary.dat": "100644", "safe.txt": "100644"}
    assert dict(candidate.changed_modes) == {"binary.dat": "100644", "safe.txt": "100644"}
    assert candidate.artifact_origin == "worker_generated"
    candidate.verify_sha256()
    with pytest.raises(FrozenInstanceError):
        candidate.base_commit = "f" * 40  # type: ignore[misc]


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({"patch_sha256": "0" * 64}, "SHA-256"),
        ({"changed_paths": ("other.txt",)}, "changed paths"),
        ({"workspace_head": "f" * 40}, "commits"),
        ({"artifact_origin": "oracle_generated"}, "worker_generated"),
    ],
)
def test_candidate_patch_rechecks_hash_paths_head_and_origin(
    tmp_path: Path,
    replacement: dict[str, object],
    message: str,
) -> None:
    candidate = _repository_patch(tmp_path).candidate

    with pytest.raises(CandidatePatchError, match=message):
        replace(candidate, **replacement)


@pytest.mark.parametrize(
    ("diff_bytes", "path"),
    [
        (
            _VALID_TEXT_DIFF.replace(b"a/safe.txt", b"a//etc/passwd").replace(
                b"b/safe.txt", b"b//etc/passwd"
            ),
            "/etc/passwd",
        ),
        (
            _VALID_TEXT_DIFF.replace(b"safe.txt", b"../escape.txt"),
            "../escape.txt",
        ),
        (
            _VALID_TEXT_DIFF.replace(b"safe.txt", b".git/config"),
            ".git/config",
        ),
        (
            _VALID_TEXT_DIFF.replace(b"safe.txt", b".gitmodules"),
            ".gitmodules",
        ),
        (
            b"""diff --git a/link b/link
new file mode 120000
index 0000000..e69de29
--- /dev/null
+++ b/link
@@ -0,0 +1 @@
+target
""",
            "link",
        ),
        (
            b"""diff --git a/module b/module
new file mode 160000
index 0000000..1111111
--- /dev/null
+++ b/module
@@ -0,0 +1 @@
+Subproject commit 1111111111111111111111111111111111111111
""",
            "module",
        ),
        (
            _VALID_TEXT_DIFF.replace(b"100644", b"100600"),
            "safe.txt",
        ),
        (
            b"From " + b"1" * 40 + b" Mon Sep 17 00:00:00 2001\n" + _VALID_TEXT_DIFF,
            "safe.txt",
        ),
        (
            _VALID_TEXT_DIFF.replace(
                b"diff --git a/safe.txt b/safe.txt",
                b"diff --git a/old.txt b/new.txt",
            ),
            "old.txt",
        ),
    ],
)
def test_candidate_patch_rejects_unsafe_paths_modes_submodules_and_commit_artifacts(
    diff_bytes: bytes,
    path: str,
) -> None:
    with pytest.raises(CandidatePatchError):
        _candidate_from_diff(diff_bytes, path=path)


def test_candidate_patch_requires_complete_hash_and_mode_preconditions() -> None:
    with pytest.raises(CandidatePatchError, match="cover every changed path"):
        CandidatePatch.from_capture(
            worker_run_id="run_static",
            source_event_ids=("evt_source",),
            base_commit=_BASE,
            workspace_head=_BASE,
            diff_bytes=_VALID_TEXT_DIFF,
            patch_sha256=sha256_bytes(_VALID_TEXT_DIFF),
            changed_paths=("safe.txt",),
            precondition_sha256={},
            changed_modes={"safe.txt": "100644"},
        )
    with pytest.raises(CandidatePatchError, match="result file mode"):
        _candidate_from_diff(changed_mode="100755")
    with pytest.raises(CandidatePatchError, match="invalid file mode"):
        CandidatePatch.from_capture(
            worker_run_id="run_static",
            source_event_ids=("evt_source",),
            base_commit=_BASE,
            workspace_head=_BASE,
            diff_bytes=_VALID_TEXT_DIFF,
            patch_sha256=sha256_bytes(_VALID_TEXT_DIFF),
            changed_paths=("safe.txt",),
            precondition_sha256={"safe.txt": "0" * 64},
            precondition_modes={"safe.txt": "120000"},
            changed_modes={"safe.txt": "100644"},
        )


def test_candidate_patch_accepts_regular_create_delete_and_chmod_capture(tmp_path: Path) -> None:
    repository = tmp_path / "mode-source"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Oracle Lab Test")
    _git(repository, "config", "user.email", "oracle-lab@example.invalid")
    _git(repository, "config", "core.fileMode", "true")
    deleted_bytes = b"delete me\n"
    executable_bytes = b"#!/bin/sh\nexit 0\n"
    (repository / "deleted.txt").write_bytes(deleted_bytes)
    (repository / "script.sh").write_bytes(executable_bytes)
    os.chmod(repository / "script.sh", 0o644)
    _git(repository, "add", "--all", "--")
    _git(repository, "commit", "-q", "-m", "base")
    base_commit = _git(repository, "rev-parse", "HEAD").decode().strip()

    (repository / "deleted.txt").unlink()
    (repository / "created.txt").write_bytes(b"created\n")
    os.chmod(repository / "script.sh", 0o755)
    _git(repository, "add", "--all", "--")
    diff_bytes = _git(
        repository,
        "diff",
        "--cached",
        "--binary",
        "--no-ext-diff",
        "--no-renames",
        base_commit,
        "--",
    )
    raw_paths = _git(
        repository,
        "diff",
        "--cached",
        "--name-only",
        "-z",
        "--no-renames",
        base_commit,
        "--",
    )
    changed_paths = tuple(value.decode() for value in raw_paths.split(b"\0") if value)
    before_hashes = {
        "created.txt": None,
        "deleted.txt": sha256_bytes(deleted_bytes),
        "script.sh": sha256_bytes(executable_bytes),
    }
    changed_modes: dict[str, str | None] = {}
    for path in changed_paths:
        entry = _git(repository, "ls-files", "-s", "--", path)
        changed_modes[path] = entry.split(maxsplit=1)[0].decode() if entry else None

    candidate = CandidatePatch.from_capture(
        worker_run_id="run_modes",
        source_event_ids=("evt_source",),
        base_commit=base_commit,
        workspace_head=base_commit,
        diff_bytes=diff_bytes,
        patch_sha256=sha256_bytes(diff_bytes),
        changed_paths=changed_paths,
        precondition_sha256=before_hashes,
        changed_modes=changed_modes,
    )

    preconditions = {item.path: item for item in candidate.preconditions}
    assert (preconditions["created.txt"].mode, preconditions["created.txt"].result_mode) == (
        None,
        "100644",
    )
    assert (preconditions["deleted.txt"].mode, preconditions["deleted.txt"].result_mode) == (
        "100644",
        None,
    )
    assert (preconditions["script.sh"].mode, preconditions["script.sh"].result_mode) == (
        "100644",
        "100755",
    )


def test_patch_decision_state_is_pure_human_only_and_conflict_aware() -> None:
    candidate = _candidate_from_diff()
    approved = _decision(candidate, PatchDecisionKind.APPROVE, decision_id="evt_approve")
    rejected = _decision(candidate, PatchDecisionKind.REJECT, decision_id="evt_reject")

    assert (
        evaluate_patch_decisions(candidate, patch_event_id="evt_patch", decisions=()).state
        is PatchDecisionState.PENDING
    )
    assert (
        evaluate_patch_decisions(candidate, patch_event_id="evt_patch", decisions=(approved,)).state
        is PatchDecisionState.APPROVED
    )
    assert (
        evaluate_patch_decisions(candidate, patch_event_id="evt_patch", decisions=(rejected,)).state
        is PatchDecisionState.REJECTED
    )
    assert (
        evaluate_patch_decisions(
            candidate,
            patch_event_id="evt_patch",
            decisions=(approved, rejected),
        ).state
        is PatchDecisionState.CONFLICT
    )
    with pytest.raises(CandidatePatchError, match="Human"):
        _decision(
            candidate,
            PatchDecisionKind.APPROVE,
            decision_id="evt_host",
            actor_kind="host",
        )


def test_preflight_rejects_content_mode_and_base_conflicts_without_writing(
    tmp_path: Path,
) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate
    source = captured.repository

    result = preflight_candidate_patch(candidate, source)
    assert result.changed_paths == candidate.changed_paths
    assert _git(source, "status", "--porcelain=v1", "-z") == b""

    (source / "safe.txt").write_bytes(b"conflict\n")
    with pytest.raises(PatchApplicationError, match="content changed"):
        preflight_candidate_patch(candidate, source)
    (source / "safe.txt").write_bytes(captured.original_text)

    os.chmod(source / "safe.txt", 0o755)
    with pytest.raises(PatchApplicationError, match="mode changed"):
        preflight_candidate_patch(candidate, source)
    os.chmod(source / "safe.txt", 0o644)

    (source / "unrelated.txt").write_text("new commit\n", encoding="utf-8")
    _git(source, "add", "--", "unrelated.txt")
    _git(source, "commit", "-q", "-m", "advance")
    with pytest.raises(PatchApplicationError, match="base commit"):
        preflight_candidate_patch(candidate, source)


def test_approved_patch_applies_only_to_persistent_staging_and_refuses_duplicate(
    tmp_path: Path,
) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate
    approval = _decision(candidate, PatchDecisionKind.APPROVE, decision_id="evt_approve")
    staging = tmp_path / "staging" / "patch-1"
    source_before = {
        "status": _git(captured.repository, "status", "--porcelain=v1", "-z"),
        "text": (captured.repository / "safe.txt").read_bytes(),
        "binary": (captured.repository / "binary.dat").read_bytes(),
    }

    result = apply_candidate_to_staging(
        candidate,
        patch_event_id="evt_patch",
        decisions=(approval,),
        source_worktree=captured.repository,
        staging_worktree=staging,
    )

    assert result.approval_event_id == "evt_approve"
    assert result.staging_worktree == staging
    assert (staging / "safe.txt").read_bytes() == captured.changed_text
    assert (staging / "binary.dat").read_bytes() == captured.changed_binary
    assert _git(staging, "rev-parse", "HEAD").decode().strip() == candidate.base_commit
    assert _git(captured.repository, "status", "--porcelain=v1", "-z") == source_before["status"]
    assert (captured.repository / "safe.txt").read_bytes() == source_before["text"]
    assert (captured.repository / "binary.dat").read_bytes() == source_before["binary"]

    with pytest.raises(PatchApplicationError, match="already exists"):
        apply_candidate_to_staging(
            candidate,
            patch_event_id="evt_patch",
            decisions=(approval,),
            source_worktree=captured.repository,
            staging_worktree=staging,
        )


def test_staging_clone_ignores_source_hook_and_clean_filter_config(tmp_path: Path) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate
    approval = _decision(candidate, PatchDecisionKind.APPROVE, decision_id="evt_approve")
    hook_marker = tmp_path / "staging-post-checkout-ran"
    filter_marker = tmp_path / "staging-clean-filter-ran"
    hook = captured.repository / ".git" / "hooks" / "post-checkout"
    hook.write_text(
        f"#!/bin/sh\nprintf ran > {hook_marker}\n",
        encoding="utf-8",
    )
    hook.chmod(0o700)
    clean_filter = tmp_path / "clean-filter"
    clean_filter.write_text(
        f"#!/bin/sh\nprintf ran > {filter_marker}\ncat\n",
        encoding="utf-8",
    )
    clean_filter.chmod(0o700)
    _git(captured.repository, "config", "filter.attack.clean", str(clean_filter))
    _git(captured.repository, "config", "filter.attack.required", "true")
    _git(captured.repository, "config", "diff.external", str(clean_filter))
    _git(captured.repository, "config", "diff.attack.textconv", str(clean_filter))
    source_git_before = fingerprint_git_control(captured.repository / ".git")
    staging = tmp_path / "safe-staging"

    result = apply_candidate_to_staging(
        candidate,
        patch_event_id="evt_patch",
        decisions=(approval,),
        source_worktree=captured.repository,
        staging_worktree=staging,
    )

    assert result.staging_worktree == staging
    assert (staging / ".git").is_dir()
    assert _git(staging, "remote") == b""
    assert not (staging / ".git" / "objects" / "info" / "alternates").exists()
    assert not hook_marker.exists()
    assert not filter_marker.exists()
    assert fingerprint_git_control(captured.repository / ".git") == source_git_before


def test_staging_apply_requires_approval_and_rejects_source_or_current_worktree(
    tmp_path: Path,
) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate
    approval = _decision(candidate, PatchDecisionKind.APPROVE, decision_id="evt_approve")
    pending_staging = tmp_path / "pending"

    with pytest.raises(PatchApplicationError, match="pending"):
        apply_candidate_to_staging(
            candidate,
            patch_event_id="evt_patch",
            decisions=(),
            source_worktree=captured.repository,
            staging_worktree=pending_staging,
        )
    assert not pending_staging.exists()

    with pytest.raises(PatchApplicationError, match="source or current"):
        apply_candidate_to_staging(
            candidate,
            patch_event_id="evt_patch",
            decisions=(approval,),
            source_worktree=captured.repository,
            staging_worktree=captured.repository,
        )


def test_failed_apply_removes_partial_staging_and_preserves_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _repository_patch(tmp_path)
    candidate = captured.candidate
    approval = _decision(candidate, PatchDecisionKind.APPROVE, decision_id="evt_approve")
    staging = tmp_path / "failed-staging"
    source_before = _git(captured.repository, "status", "--porcelain=v1", "-z")
    original_git = patches_module._git

    def fail_actual_apply(worktree: Path, *arguments: str, **kwargs):
        if arguments and arguments[0] == "apply" and "--check" not in arguments:
            raise PatchApplicationError("injected apply failure")
        return original_git(worktree, *arguments, **kwargs)

    monkeypatch.setattr(patches_module, "_git", fail_actual_apply)
    with pytest.raises(PatchApplicationError, match="injected apply failure"):
        apply_candidate_to_staging(
            candidate,
            patch_event_id="evt_patch",
            decisions=(approval,),
            source_worktree=captured.repository,
            staging_worktree=staging,
        )

    assert not staging.exists()
    assert _git(captured.repository, "status", "--porcelain=v1", "-z") == source_before
