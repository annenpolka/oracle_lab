"""Fail-closed candidate-patch validation and isolated staging application.

Coding-worker patches are untrusted, worker-generated artifacts.  This module
validates their immutable identity without promoting them into Oracle material,
evaluates explicit Human decisions as pure data, and applies an approved patch
only to a newly-created persistent standalone Git clone.
"""

from __future__ import annotations

import contextlib
import re
import shlex
import stat
import subprocess
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from oracle_lab.git_control import (
    GitControlError,
    create_standalone_clone,
    remove_standalone_clone,
    run_git,
)
from oracle_lab.jsonutil import sha256_bytes

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
_GIT_INDEX = re.compile(r"^index [0-9a-fA-F]+\.\.[0-9a-fA-F]+(?: ([0-9]{6}))?$")
_GIT_MODE = re.compile(r"^(old mode|new mode|new file mode|deleted file mode) ([0-9]{6})$")
_ALLOWED_FILE_MODES = frozenset({"100644", "100755"})


class CandidatePatchError(ValueError):
    """Raised when untrusted patch material cannot pass the security boundary."""


class PatchApplicationError(RuntimeError):
    """Raised before a patch can remain partially applied to a worktree."""


def _validate_repository_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise CandidatePatchError("candidate patch path must be non-empty text")
    if "\x00" in path or "\\" in path or any(ord(character) < 32 for character in path):
        raise CandidatePatchError(f"candidate patch contains an unsafe path: {path!r}")
    if path.startswith("/") or re.match(r"^[A-Za-z]:", path):
        raise CandidatePatchError(f"candidate patch contains an absolute path: {path!r}")
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise CandidatePatchError(f"candidate patch path is not normalized: {path!r}")
    if any(part.casefold() in {".git", ".gitmodules"} for part in parts):
        raise CandidatePatchError(f"candidate patch targets Git control data: {path!r}")
    if any(":" in part for part in parts):
        raise CandidatePatchError(
            f"candidate patch contains a cross-platform unsafe path: {path!r}"
        )
    normalized = PurePosixPath(path).as_posix()
    if normalized != path:
        raise CandidatePatchError(f"candidate patch path is not normalized: {path!r}")
    return path


def _validate_mode(mode: str | None, *, allow_absent: bool) -> str | None:
    if mode is None and allow_absent:
        return None
    if mode not in _ALLOWED_FILE_MODES:
        raise CandidatePatchError(
            f"candidate patch contains a symlink, submodule, or invalid file mode: {mode!r}"
        )
    return mode


def _decode_git_tokens(value: str, *, expected: int) -> tuple[str, ...]:
    if "\\" in value:
        raise CandidatePatchError("candidate patch uses unsupported escaped Git paths")
    try:
        tokens = tuple(shlex.split(value, posix=True))
    except ValueError as error:
        raise CandidatePatchError("candidate patch contains malformed Git path quoting") from error
    if len(tokens) != expected:
        raise CandidatePatchError("candidate patch contains ambiguous Git path headers")
    return tokens


def _strip_git_prefix(value: str, prefix: str) -> str:
    if not value.startswith(prefix):
        raise CandidatePatchError("candidate patch is not a canonical git diff")
    return _validate_repository_path(value[len(prefix) :])


@dataclass(slots=True)
class _SectionBuilder:
    path: str
    before_mode: str | None = None
    after_mode: str | None = None
    before_path: str | None = None
    after_path: str | None = None
    saw_before_marker: bool = False
    saw_after_marker: bool = False


@dataclass(frozen=True, slots=True)
class _PatchSection:
    path: str
    before_mode: str | None
    after_mode: str | None


def _finalize_section(section: _SectionBuilder) -> _PatchSection:
    if section.before_mode is None and section.after_mode is None:
        raise CandidatePatchError(f"candidate patch has no auditable file mode: {section.path}")
    if section.saw_before_marker:
        expected = None if section.before_path == "/dev/null" else section.path
        actual = None if section.before_path == "/dev/null" else section.before_path
        if actual != expected:
            raise CandidatePatchError("candidate patch path headers disagree")
    if section.saw_after_marker:
        expected = None if section.after_path == "/dev/null" else section.path
        actual = None if section.after_path == "/dev/null" else section.after_path
        if actual != expected:
            raise CandidatePatchError("candidate patch path headers disagree")
    if section.before_path == "/dev/null" and section.before_mode is not None:
        raise CandidatePatchError("new-file patch claims an existing precondition mode")
    if section.after_path == "/dev/null" and section.after_mode is not None:
        raise CandidatePatchError("deleted-file patch claims a resulting mode")
    if section.before_path == "/dev/null" and section.after_path == "/dev/null":
        raise CandidatePatchError("candidate patch has no repository target")
    return _PatchSection(section.path, section.before_mode, section.after_mode)


def _parse_git_binary_diff(diff_bytes: bytes) -> tuple[_PatchSection, ...]:
    if not isinstance(diff_bytes, bytes) or not diff_bytes:
        raise CandidatePatchError("candidate patch must contain exact git diff bytes")
    if b"\x00" in diff_bytes:
        raise CandidatePatchError("candidate patch contains a NUL byte")
    try:
        text = diff_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CandidatePatchError("candidate patch contains non-UTF-8 path material") from error
    lines = text.splitlines()
    if not lines or not lines[0].startswith("diff --git "):
        raise CandidatePatchError("candidate patch must be a raw git diff, not a commit artifact")

    sections: list[_PatchSection] = []
    current: _SectionBuilder | None = None
    for line in lines:
        if line.startswith("diff --git "):
            if current is not None:
                sections.append(_finalize_section(current))
            old_token, new_token = _decode_git_tokens(line[len("diff --git ") :], expected=2)
            old_path = _strip_git_prefix(old_token, "a/")
            new_path = _strip_git_prefix(new_token, "b/")
            if old_path != new_path:
                raise CandidatePatchError("rename and copy patches are not accepted")
            current = _SectionBuilder(path=old_path)
            continue
        if current is None:
            raise CandidatePatchError("candidate patch contains data before its first diff header")
        if line.startswith(("rename from ", "rename to ", "copy from ", "copy to ")):
            raise CandidatePatchError("rename and copy patches are not accepted")
        if line.startswith("Submodule ") or line.startswith("Subproject commit "):
            raise CandidatePatchError("submodule patches are not accepted")
        mode_match = _GIT_MODE.fullmatch(line)
        if mode_match is not None:
            operation, raw_mode = mode_match.groups()
            mode = _validate_mode(raw_mode, allow_absent=False)
            if operation in {"old mode", "deleted file mode"}:
                if current.before_mode is not None:
                    raise CandidatePatchError("candidate patch repeats base mode metadata")
                current.before_mode = mode
            else:
                if current.after_mode is not None:
                    raise CandidatePatchError("candidate patch repeats result mode metadata")
                current.after_mode = mode
            continue
        if line.startswith(("old mode ", "new mode ", "new file mode ", "deleted file mode ")):
            raise CandidatePatchError("candidate patch contains malformed mode metadata")
        if line.startswith("index "):
            index_match = _GIT_INDEX.fullmatch(line)
            if index_match is None:
                raise CandidatePatchError("candidate patch contains malformed index metadata")
            raw_mode = index_match.group(1)
            if raw_mode is not None:
                mode = _validate_mode(raw_mode, allow_absent=False)
                if current.before_mode is not None or current.after_mode is not None:
                    raise CandidatePatchError(
                        "candidate patch contains contradictory mode metadata"
                    )
                current.before_mode = mode
                current.after_mode = mode
            continue
        if line.startswith("Binary files "):
            raise CandidatePatchError("binary changes must use git diff --binary encoding")
        if line.startswith("--- "):
            if current.saw_before_marker:
                raise CandidatePatchError("candidate patch repeats its before-path header")
            (raw_path,) = _decode_git_tokens(line[4:], expected=1)
            current.saw_before_marker = True
            current.before_path = (
                "/dev/null" if raw_path == "/dev/null" else _strip_git_prefix(raw_path, "a/")
            )
            continue
        if line.startswith("+++ "):
            if current.saw_after_marker:
                raise CandidatePatchError("candidate patch repeats its after-path header")
            (raw_path,) = _decode_git_tokens(line[4:], expected=1)
            current.saw_after_marker = True
            current.after_path = (
                "/dev/null" if raw_path == "/dev/null" else _strip_git_prefix(raw_path, "b/")
            )
    if current is not None:
        sections.append(_finalize_section(current))
    if not sections:
        raise CandidatePatchError("candidate patch contains no changed files")
    paths = [section.path for section in sections]
    if len(set(paths)) != len(paths):
        raise CandidatePatchError("candidate patch contains duplicate file sections")
    return tuple(sections)


@dataclass(frozen=True, slots=True)
class PathPrecondition:
    """Expected base and resulting identity for one changed repository path."""

    path: str
    sha256: str | None
    mode: str | None
    result_mode: str | None

    def __post_init__(self) -> None:
        _validate_repository_path(self.path)
        if self.sha256 is None:
            if self.mode is not None:
                raise CandidatePatchError("an absent-path precondition may not carry a mode")
        else:
            if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
                raise CandidatePatchError("path precondition SHA-256 is invalid")
            _validate_mode(self.mode, allow_absent=False)
        _validate_mode(self.result_mode, allow_absent=True)
        if self.sha256 is None and self.result_mode is None:
            raise CandidatePatchError("a path cannot be absent both before and after a patch")

    @property
    def expects_absent(self) -> bool:
        return self.sha256 is None


@dataclass(frozen=True, slots=True)
class CandidatePatch:
    """Immutable candidate patch that has passed deterministic static validation."""

    worker_run_id: str
    source_event_ids: tuple[str, ...]
    base_commit: str
    workspace_head: str
    diff_bytes: bytes
    patch_sha256: str
    changed_paths: tuple[str, ...]
    preconditions: tuple[PathPrecondition, ...]
    artifact_origin: str = "worker_generated"

    def __post_init__(self) -> None:
        if not isinstance(self.worker_run_id, str) or not self.worker_run_id:
            raise CandidatePatchError("candidate patch requires a worker run ID")
        if isinstance(self.source_event_ids, (str, bytes, bytearray)):
            raise CandidatePatchError("candidate patch source event IDs must be a sequence")
        source_ids = tuple(self.source_event_ids)
        if not source_ids or any(not isinstance(value, str) or not value for value in source_ids):
            raise CandidatePatchError("candidate patch requires source event IDs")
        if len(set(source_ids)) != len(source_ids):
            raise CandidatePatchError("candidate patch source event IDs must be unique")
        if not isinstance(self.base_commit, str) or _GIT_COMMIT.fullmatch(self.base_commit) is None:
            raise CandidatePatchError("candidate patch base commit is invalid")
        if (
            not isinstance(self.workspace_head, str)
            or _GIT_COMMIT.fullmatch(self.workspace_head) is None
        ):
            raise CandidatePatchError("candidate patch workspace head is invalid")
        if self.workspace_head.casefold() != self.base_commit.casefold():
            raise CandidatePatchError(
                "worker-created commits are not accepted as candidate patches"
            )
        if not isinstance(self.diff_bytes, bytes):
            raise CandidatePatchError("candidate patch must preserve exact bytes")
        if not isinstance(self.patch_sha256, str) or _SHA256.fullmatch(self.patch_sha256) is None:
            raise CandidatePatchError("candidate patch SHA-256 is invalid")
        if sha256_bytes(self.diff_bytes) != self.patch_sha256:
            raise CandidatePatchError("candidate patch SHA-256 does not match exact diff bytes")
        if self.artifact_origin != "worker_generated":
            raise CandidatePatchError("candidate patches must remain worker_generated material")

        sections = _parse_git_binary_diff(self.diff_bytes)
        parsed_paths = tuple(section.path for section in sections)
        if isinstance(self.changed_paths, (str, bytes, bytearray)):
            raise CandidatePatchError("candidate patch changed paths must be a sequence")
        changed_paths = tuple(self.changed_paths)
        if parsed_paths != changed_paths:
            raise CandidatePatchError("declared changed paths do not match exact diff bytes")
        preconditions = tuple(self.preconditions)
        by_path = {precondition.path: precondition for precondition in preconditions}
        if len(by_path) != len(preconditions) or set(by_path) != set(parsed_paths):
            raise CandidatePatchError("candidate patch requires one precondition per changed path")
        for section in sections:
            precondition = by_path[section.path]
            if precondition.mode != section.before_mode:
                raise CandidatePatchError("base file mode does not match patch metadata")
            if precondition.result_mode != section.after_mode:
                raise CandidatePatchError("result file mode does not match patch metadata")

        object.__setattr__(self, "source_event_ids", source_ids)
        object.__setattr__(self, "diff_bytes", bytes(self.diff_bytes))
        object.__setattr__(self, "changed_paths", changed_paths)
        object.__setattr__(self, "preconditions", preconditions)

    @classmethod
    def from_capture(
        cls,
        *,
        worker_run_id: str,
        source_event_ids: Sequence[str],
        base_commit: str,
        workspace_head: str,
        diff_bytes: bytes,
        patch_sha256: str,
        changed_paths: Sequence[str],
        precondition_sha256: Mapping[str, str | None],
        changed_modes: Mapping[str, str | None],
        precondition_modes: Mapping[str, str | None] | None = None,
    ) -> CandidatePatch:
        """Build from repository-capture mappings while freezing all identities."""

        sections = _parse_git_binary_diff(diff_bytes)
        parsed_paths = tuple(section.path for section in sections)
        expected_keys = set(parsed_paths)
        if set(precondition_sha256) != expected_keys or set(changed_modes) != expected_keys:
            raise CandidatePatchError("capture mappings must cover every changed path exactly")
        if precondition_modes is not None and set(precondition_modes) != expected_keys:
            raise CandidatePatchError("precondition modes must cover every changed path exactly")
        preconditions = tuple(
            PathPrecondition(
                path=section.path,
                sha256=precondition_sha256[section.path],
                mode=(
                    section.before_mode
                    if precondition_modes is None
                    else precondition_modes[section.path]
                ),
                result_mode=changed_modes[section.path],
            )
            for section in sections
        )
        return cls(
            worker_run_id=worker_run_id,
            source_event_ids=tuple(source_event_ids),
            base_commit=base_commit,
            workspace_head=workspace_head,
            diff_bytes=diff_bytes,
            patch_sha256=patch_sha256,
            changed_paths=tuple(changed_paths),
            preconditions=preconditions,
        )

    def verify_sha256(self) -> None:
        """Recheck the patch bytes at an application or persistence boundary."""

        if sha256_bytes(self.diff_bytes) != self.patch_sha256:
            raise CandidatePatchError("candidate patch SHA-256 changed after validation")

    @property
    def patch_bytes(self) -> bytes:
        """Alias matching the repository-capture result vocabulary."""

        return self.diff_bytes

    @property
    def precondition_sha256(self) -> Mapping[str, str | None]:
        return MappingProxyType({item.path: item.sha256 for item in self.preconditions})

    @property
    def precondition_modes(self) -> Mapping[str, str | None]:
        return MappingProxyType({item.path: item.mode for item in self.preconditions})

    @property
    def changed_modes(self) -> Mapping[str, str | None]:
        return MappingProxyType({item.path: item.result_mode for item in self.preconditions})


class PatchDecisionKind(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class PatchDecisionState(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class PatchDecision:
    """Minimal immutable projection of one explicit Human patch decision event."""

    decision_event_id: str
    patch_event_id: str
    worker_run_id: str
    patch_sha256: str
    base_commit: str
    decision: PatchDecisionKind
    actor_kind: str = "human"

    def __post_init__(self) -> None:
        for field_name in ("decision_event_id", "patch_event_id", "worker_run_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise CandidatePatchError(f"patch decision requires {field_name}")
        if self.actor_kind != "human":
            raise CandidatePatchError("only an explicit Human actor may decide a candidate patch")
        if not isinstance(self.patch_sha256, str) or _SHA256.fullmatch(self.patch_sha256) is None:
            raise CandidatePatchError("patch decision SHA-256 is invalid")
        if not isinstance(self.base_commit, str) or _GIT_COMMIT.fullmatch(self.base_commit) is None:
            raise CandidatePatchError("patch decision base commit is invalid")
        if not isinstance(self.decision, PatchDecisionKind):
            try:
                object.__setattr__(self, "decision", PatchDecisionKind(self.decision))
            except (TypeError, ValueError) as error:
                raise CandidatePatchError("patch decision must approve or reject") from error


@dataclass(frozen=True, slots=True)
class PatchDecisionStatus:
    state: PatchDecisionState
    decision_event_ids: tuple[str, ...]

    @property
    def approved(self) -> bool:
        return self.state is PatchDecisionState.APPROVED


def evaluate_patch_decisions(
    candidate: CandidatePatch,
    *,
    patch_event_id: str,
    decisions: Iterable[PatchDecision],
) -> PatchDecisionStatus:
    """Purely evaluate matching Human decisions; contradictory history is conflict."""

    if not isinstance(patch_event_id, str) or not patch_event_id:
        raise CandidatePatchError("patch decision evaluation requires a patch event ID")
    matching = tuple(
        decision
        for decision in decisions
        if decision.patch_event_id == patch_event_id
        and decision.worker_run_id == candidate.worker_run_id
        and decision.patch_sha256 == candidate.patch_sha256
        and decision.base_commit.casefold() == candidate.base_commit.casefold()
    )
    ids = tuple(decision.decision_event_id for decision in matching)
    if len(set(ids)) != len(ids):
        raise CandidatePatchError("patch decision event IDs must be unique")
    kinds = {decision.decision for decision in matching}
    if not kinds:
        state = PatchDecisionState.PENDING
    elif kinds == {PatchDecisionKind.APPROVE}:
        state = PatchDecisionState.APPROVED
    elif kinds == {PatchDecisionKind.REJECT}:
        state = PatchDecisionState.REJECTED
    else:
        state = PatchDecisionState.CONFLICT
    return PatchDecisionStatus(state=state, decision_event_ids=ids)


@dataclass(frozen=True, slots=True)
class PatchPreflightResult:
    worktree: Path
    base_commit: str
    patch_sha256: str
    changed_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PatchApplicationResult:
    patch_event_id: str
    approval_event_id: str
    staging_worktree: Path
    base_commit: str
    patch_sha256: str
    changed_paths: tuple[str, ...]


def _git(
    worktree: Path,
    *arguments: str,
    input_bytes: bytes | None = None,
    git_executable: str = "git",
    check: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    try:
        result = run_git(
            worktree,
            *arguments,
            input_bytes=input_bytes,
            git_executable=git_executable,
            timeout=30,
        )
    except GitControlError as error:
        raise PatchApplicationError(f"Git control-plane command failed: {arguments[0]}") from error
    if check and result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()[:2000]
        raise PatchApplicationError(
            f"Git control-plane command rejected candidate patch: {arguments[0]}: {detail}"
        )
    return result


def _git_root(path: Path, *, git_executable: str) -> Path:
    result = _git(path, "rev-parse", "--show-toplevel", git_executable=git_executable)
    try:
        root = Path(result.stdout.decode("utf-8").strip()).resolve(strict=True)
    except (UnicodeDecodeError, OSError) as error:
        raise PatchApplicationError("Git worktree root is unavailable") from error
    if not root.is_dir() or root.is_symlink():
        raise PatchApplicationError("Git worktree root is not a safe directory")
    return root


def _try_git_root(path: Path, *, git_executable: str) -> Path | None:
    with contextlib.suppress(PatchApplicationError):
        return _git_root(path, git_executable=git_executable)
    return None


def _safe_worktree_path(root: Path, repository_path: str) -> Path:
    relative = PurePosixPath(_validate_repository_path(repository_path))
    current = root
    for part in relative.parts[:-1]:
        current /= part
        if current.is_symlink():
            raise PatchApplicationError(f"worktree path traverses a symlink: {repository_path}")
        if current.exists() and not current.is_dir():
            raise PatchApplicationError(
                f"worktree path has a non-directory parent: {repository_path}"
            )
    candidate = root.joinpath(*relative.parts)
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise PatchApplicationError(f"worktree path escapes its root: {repository_path}")
    return candidate


def _verify_worktree_preconditions(candidate: CandidatePatch, root: Path) -> None:
    by_path = {precondition.path: precondition for precondition in candidate.preconditions}
    for repository_path in candidate.changed_paths:
        precondition = by_path[repository_path]
        path = _safe_worktree_path(root, repository_path)
        present = path.exists() or path.is_symlink()
        if precondition.expects_absent:
            if present:
                raise PatchApplicationError(
                    f"patch precondition expected an absent path: {repository_path}"
                )
            continue
        if not present:
            raise PatchApplicationError(f"patch precondition path is missing: {repository_path}")
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise PatchApplicationError(f"patch precondition path is a symlink: {repository_path}")
        if not stat.S_ISREG(info.st_mode):
            raise PatchApplicationError(
                f"patch precondition path is not a regular file: {repository_path}"
            )
        actual_mode = "100755" if info.st_mode & 0o111 else "100644"
        if actual_mode != precondition.mode:
            raise PatchApplicationError(f"patch precondition mode changed: {repository_path}")
        if sha256_bytes(path.read_bytes()) != precondition.sha256:
            raise PatchApplicationError(f"patch precondition content changed: {repository_path}")


def preflight_candidate_patch(
    candidate: CandidatePatch,
    worktree: str | Path,
    *,
    git_executable: str = "git",
    _trusted_git_directory: bool = False,
) -> PatchPreflightResult:
    """Recheck base, paths, preconditions, and whole-patch applicability without writing."""

    candidate.verify_sha256()
    root = _git_root(Path(worktree), git_executable=git_executable)
    head = _git(root, "rev-parse", "--verify", "HEAD", git_executable=git_executable)
    actual_head = head.stdout.decode("ascii").strip()
    if actual_head.casefold() != candidate.base_commit.casefold():
        raise PatchApplicationError("candidate patch base commit no longer matches the worktree")
    _verify_worktree_preconditions(candidate, root)
    if _trusted_git_directory:
        applicability_root = root
        temporary = None
    else:
        temporary = tempfile.TemporaryDirectory(prefix="oracle-patch-preflight-")
        applicability_root = Path(temporary.name) / "clone"
        try:
            create_standalone_clone(
                root,
                applicability_root,
                candidate.base_commit,
                git_executable=git_executable,
            )
        except GitControlError as error:
            temporary.cleanup()
            raise PatchApplicationError(str(error)) from error
    try:
        _git(
            applicability_root,
            "apply",
            "--check",
            "--binary",
            "--index",
            "--whitespace=nowarn",
            "-",
            input_bytes=candidate.diff_bytes,
            git_executable=git_executable,
        )
    finally:
        if temporary is not None:
            temporary.cleanup()
    return PatchPreflightResult(
        worktree=root,
        base_commit=candidate.base_commit,
        patch_sha256=candidate.patch_sha256,
        changed_paths=candidate.changed_paths,
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _cleanup_staging(staging: Path) -> None:
    if staging.exists() and staging.is_dir() and not staging.is_symlink():
        try:
            remove_standalone_clone(staging)
        except GitControlError as error:
            raise PatchApplicationError(str(error)) from error


def apply_candidate_to_staging(
    candidate: CandidatePatch,
    *,
    patch_event_id: str,
    decisions: Iterable[PatchDecision],
    source_worktree: str | Path,
    staging_worktree: str | Path,
    git_executable: str = "git",
) -> PatchApplicationResult:
    """Apply one approved patch to a new persistent standalone clone only."""

    frozen_decisions = tuple(decisions)
    status = evaluate_patch_decisions(
        candidate,
        patch_event_id=patch_event_id,
        decisions=frozen_decisions,
    )
    if status.state is not PatchDecisionState.APPROVED:
        raise PatchApplicationError(
            f"candidate patch is not uniquely approved: {status.state.value}"
        )
    approvals = tuple(
        decision
        for decision in frozen_decisions
        if decision.patch_event_id == patch_event_id
        and decision.worker_run_id == candidate.worker_run_id
        and decision.patch_sha256 == candidate.patch_sha256
        and decision.base_commit.casefold() == candidate.base_commit.casefold()
        and decision.decision is PatchDecisionKind.APPROVE
    )
    if len(approvals) != 1:
        raise PatchApplicationError("candidate patch requires exactly one matching Human approval")

    candidate.verify_sha256()
    source_root = _git_root(Path(source_worktree), git_executable=git_executable)
    # Stale source state is a conflict even though application happens elsewhere.
    preflight_candidate_patch(candidate, source_root, git_executable=git_executable)
    staging = Path(staging_worktree).expanduser().resolve(strict=False)
    current_root = _try_git_root(Path.cwd(), git_executable=git_executable)
    forbidden = (source_root,) if current_root is None else (source_root, current_root)
    if any(_paths_overlap(staging, root) for root in forbidden):
        raise PatchApplicationError("staging worktree may not be the source or current worktree")
    if staging.exists() or staging.is_symlink():
        raise PatchApplicationError("staging worktree already exists; duplicate apply refused")
    staging.parent.mkdir(parents=True, exist_ok=True)

    created = False
    try:
        try:
            create_standalone_clone(
                source_root,
                staging,
                candidate.base_commit,
                git_executable=git_executable,
            )
        except GitControlError as error:
            raise PatchApplicationError(str(error)) from error
        created = True
        preflight_candidate_patch(
            candidate,
            staging,
            git_executable=git_executable,
            _trusted_git_directory=True,
        )
        _git(
            staging,
            "apply",
            "--binary",
            "--index",
            "--whitespace=nowarn",
            "-",
            input_bytes=candidate.diff_bytes,
            git_executable=git_executable,
        )
        cached = _git(
            staging,
            "diff",
            "--cached",
            "--name-only",
            "-z",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "HEAD",
            "--",
            git_executable=git_executable,
        ).stdout
        try:
            applied_paths = tuple(value.decode("utf-8") for value in cached.split(b"\0") if value)
        except UnicodeDecodeError as error:
            raise PatchApplicationError("applied patch produced a non-UTF-8 path") from error
        if applied_paths != candidate.changed_paths:
            raise PatchApplicationError("applied paths differ from the validated candidate")
        unstaged = _git(
            staging,
            "diff",
            "--name-only",
            "-z",
            "--",
            git_executable=git_executable,
        ).stdout
        untracked = _git(
            staging,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            git_executable=git_executable,
        ).stdout
        if unstaged or untracked:
            raise PatchApplicationError("staging application left unvalidated filesystem changes")
    except BaseException:
        if created:
            _cleanup_staging(staging)
        raise

    return PatchApplicationResult(
        patch_event_id=patch_event_id,
        approval_event_id=approvals[0].decision_event_id,
        staging_worktree=staging,
        base_commit=candidate.base_commit,
        patch_sha256=candidate.patch_sha256,
        changed_paths=candidate.changed_paths,
    )


__all__ = [
    "CandidatePatch",
    "CandidatePatchError",
    "PatchApplicationError",
    "PatchApplicationResult",
    "PatchDecision",
    "PatchDecisionKind",
    "PatchDecisionState",
    "PatchDecisionStatus",
    "PatchPreflightResult",
    "PathPrecondition",
    "apply_candidate_to_staging",
    "evaluate_patch_decisions",
    "preflight_candidate_patch",
]
