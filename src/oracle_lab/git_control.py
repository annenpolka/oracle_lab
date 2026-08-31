"""Fail-closed Git control-plane helpers for coding-worker artifacts.

Worker processes never receive authority over a Git directory subsequently used
by the Host.  Repository snapshots are transferred through a temporary bundle
into standalone clones, and every Host Git process starts from a scrubbed
environment with executable hooks and inherited configuration disabled.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path


class GitControlError(RuntimeError):
    """Raised when a trusted Git control-plane operation cannot be proven safe."""


_SAFE_CONFIG_ARGUMENTS = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.untrackedCache=false",
    "-c",
    "core.attributesFile=/dev/null",
    "-c",
    "submodule.recurse=false",
    "-c",
    "protocol.file.allow=always",
)


def sanitized_git_environment(
    source: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a minimal environment with no inherited Git control variables."""

    inherited = os.environ if source is None else source
    environment = {
        key: inherited[key] for key in ("PATH", "TMPDIR", "TMP", "TEMP") if key in inherited
    }
    environment.update(
        {
            "LANG": "C",
            "LC_ALL": "C",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_PROTOCOL_FROM_USER": "0",
            "SSH_ASKPASS_REQUIRE": "never",
        }
    )
    return environment


def run_git(
    repository: str | Path,
    *arguments: str,
    input_bytes: bytes | None = None,
    git_executable: str = "git",
    timeout: float = 60,
) -> subprocess.CompletedProcess[bytes]:
    """Run Git without inherited Git state or executable Host-side extensions."""

    command = [
        git_executable,
        *_SAFE_CONFIG_ARGUMENTS,
        "-C",
        str(Path(repository)),
        *arguments,
    ]
    try:
        return subprocess.run(
            command,
            input=input_bytes,
            stdin=subprocess.DEVNULL if input_bytes is None else None,
            capture_output=True,
            env=sanitized_git_environment(),
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        operation = arguments[0] if arguments else "unknown"
        raise GitControlError(f"Git control-plane command failed: {operation}") from error


def require_git(
    repository: str | Path,
    *arguments: str,
    input_bytes: bytes | None = None,
    git_executable: str = "git",
    timeout: float = 60,
) -> bytes:
    result = run_git(
        repository,
        *arguments,
        input_bytes=input_bytes,
        git_executable=git_executable,
        timeout=timeout,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()[:2000]
        operation = arguments[0] if arguments else "unknown"
        raise GitControlError(
            f"Git control-plane command rejected operation {operation}: "
            f"{detail or result.returncode}"
        )
    return bytes(result.stdout)


def _safe_directory(path: Path, *, label: str, must_exist: bool) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise GitControlError(f"{label} may not be a symlink")
    resolved = expanded.resolve(strict=False)
    if must_exist and not resolved.is_dir():
        raise GitControlError(f"{label} is not an existing directory")
    return resolved


def remove_standalone_clone(path: str | Path) -> None:
    """Remove one explicitly resolved clone without consulting any Git metadata."""

    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise GitControlError("standalone clone may not be a symlink")
    clone = requested.resolve(strict=False)
    if not clone.is_dir():
        raise GitControlError("standalone clone is not a safe directory")
    shutil.rmtree(clone)


def create_standalone_clone(
    source_repository: str | Path,
    destination: str | Path,
    revision: str,
    *,
    git_executable: str = "git",
) -> Path:
    """Create a full, source-independent clone at one exact commit.

    A temporary bundle prevents source repository hooks, config, alternates, or
    upload-pack hooks from becoming destination control state.  The clone has no
    remote and cannot push back to the source by accident.
    """

    source = _safe_directory(Path(source_repository), label="source repository", must_exist=True)
    requested_clone = Path(destination).expanduser()
    if requested_clone.is_symlink():
        raise GitControlError("standalone clone destination may not be a symlink")
    clone = requested_clone.resolve(strict=False)
    if clone.exists():
        raise GitControlError("standalone clone destination already exists")
    clone.parent.mkdir(parents=True, exist_ok=True)
    if clone.parent.is_symlink() or not clone.parent.is_dir():
        raise GitControlError("standalone clone parent is not a safe directory")

    descriptor, raw_bundle = tempfile.mkstemp(
        prefix=".oracle-lab-source-",
        suffix=".bundle",
        dir=clone.parent,
    )
    os.close(descriptor)
    bundle = Path(raw_bundle)
    bundle.unlink()
    try:
        # HEAD is included explicitly for detached repositories; branches and
        # tags retain useful history without creating a ref in the source.
        require_git(
            source,
            "bundle",
            "create",
            str(bundle),
            "HEAD",
            "--branches",
            "--tags",
            git_executable=git_executable,
        )
        require_git(
            clone.parent,
            "clone",
            "--no-local",
            "--no-hardlinks",
            "--no-checkout",
            "--no-tags",
            "--no-recurse-submodules",
            "--template=",
            "--",
            str(bundle),
            str(clone),
            git_executable=git_executable,
        )
        git_directory = clone / ".git"
        if git_directory.is_symlink() or not git_directory.is_dir():
            raise GitControlError("standalone clone does not own an independent Git directory")
        alternates = git_directory / "objects" / "info" / "alternates"
        if alternates.exists() or alternates.is_symlink():
            raise GitControlError("standalone clone unexpectedly shares an object database")
        require_git(clone, "remote", "remove", "origin", git_executable=git_executable)
        for key, value in (
            ("core.hooksPath", os.devnull),
            ("core.fsmonitor", "false"),
            ("core.untrackedCache", "false"),
            ("submodule.recurse", "false"),
        ):
            require_git(
                clone,
                "config",
                "--local",
                "--replace-all",
                key,
                value,
                git_executable=git_executable,
            )
        require_git(
            clone,
            "checkout",
            "--detach",
            "--force",
            "--no-recurse-submodules",
            revision,
            "--",
            git_executable=git_executable,
        )
        actual = (
            require_git(
                clone,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
                git_executable=git_executable,
            )
            .decode("ascii")
            .strip()
        )
        if actual.casefold() != revision.casefold():
            raise GitControlError("standalone clone checked out a different revision")
        return clone
    except BaseException:
        if clone.exists() and clone.is_dir() and not clone.is_symlink():
            shutil.rmtree(clone)
        raise
    finally:
        if bundle.exists() and bundle.is_file() and not bundle.is_symlink():
            bundle.unlink()


def _iter_control_entries(root: Path) -> Iterable[tuple[Path, Path]]:
    """Yield Git control entries, excluding only the mutable staging index."""

    for raw_root, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(raw_root)
        relative_root = current.relative_to(root)
        if relative_root == Path("."):
            file_names[:] = [name for name in file_names if name != "index"]
        directory_names.sort()
        file_names.sort()
        yield current, relative_root
        for name in tuple(directory_names):
            path = current / name
            yield path, path.relative_to(root)
            if path.is_symlink():
                directory_names.remove(name)
        for name in file_names:
            path = current / name
            yield path, path.relative_to(root)


def fingerprint_git_control(
    git_directory: str | Path,
    *,
    common_directory: str | Path | None = None,
) -> str:
    """Hash config, refs, hooks, objects, and all other executable control state."""

    git_dir = _safe_directory(Path(git_directory), label="Git directory", must_exist=True)
    common = (
        git_dir
        if common_directory is None
        else _safe_directory(Path(common_directory), label="Git common directory", must_exist=True)
    )
    roots = tuple(dict.fromkeys((git_dir, common)))
    digest = hashlib.sha256()
    for root_number, root in enumerate(roots):
        root_label = str(root_number).encode("ascii")
        for path, relative in _iter_control_entries(root):
            try:
                details = path.lstat()
            except OSError as error:
                raise GitControlError("Git control state changed during inspection") from error
            relative_bytes = os.fsencode(str(relative))
            mode = stat.S_IMODE(details.st_mode).to_bytes(4, "big")
            digest.update(root_label)
            digest.update(len(relative_bytes).to_bytes(8, "big"))
            digest.update(relative_bytes)
            if stat.S_ISLNK(details.st_mode):
                payload = b"L" + mode + os.fsencode(os.readlink(path))
            elif stat.S_ISDIR(details.st_mode):
                payload = b"D" + mode
            elif stat.S_ISREG(details.st_mode):
                try:
                    content = path.read_bytes()
                except OSError as error:
                    raise GitControlError("Git control state cannot be read") from error
                payload = b"F" + mode + len(content).to_bytes(8, "big") + content
            else:
                payload = b"S" + mode + int(details.st_rdev).to_bytes(8, "big")
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def detached_head_from_control(git_directory: str | Path) -> str | None:
    """Read a detached HEAD without loading repository config or running Git."""

    head = Path(git_directory) / "HEAD"
    if head.is_symlink() or not head.is_file():
        return None
    try:
        value = head.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return None
    if len(value) not in {40, 64} or any(
        character not in "0123456789abcdefABCDEF" for character in value
    ):
        return None
    return value.lower()


def replace_worktree_from_untrusted(source: str | Path, destination: str | Path) -> None:
    """Copy only filesystem material into a trusted clone, never `.git` control data."""

    worker_root = _safe_directory(Path(source), label="worker worktree", must_exist=True)
    trusted_root = _safe_directory(Path(destination), label="capture worktree", must_exist=True)
    trusted_git = trusted_root / ".git"
    if trusted_git.is_symlink() or not trusted_git.is_dir():
        raise GitControlError("capture worktree has no trusted standalone Git directory")

    for raw_root, directory_names, file_names in os.walk(
        worker_root,
        topdown=True,
        followlinks=False,
    ):
        current = Path(raw_root)
        relative = current.relative_to(worker_root)
        if relative == Path("."):
            directory_names[:] = [name for name in directory_names if name != ".git"]
            file_names[:] = [name for name in file_names if name != ".git"]
        if ".git" in directory_names or ".git" in file_names:
            location = relative / ".git"
            raise GitControlError(f"worker worktree contains nested Git control data: {location}")
        directory_names.sort()
        file_names.sort()
        directory_names[:] = [name for name in directory_names if not (current / name).is_symlink()]

    for child in tuple(trusted_root.iterdir()):
        if child.name == ".git":
            continue
        if child.is_symlink() or child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)
        else:
            raise GitControlError("capture worktree contains an unsupported filesystem entry")

    for child in worker_root.iterdir():
        if child.name == ".git":
            continue
        target = trusted_root / child.name
        details = child.lstat()
        if stat.S_ISLNK(details.st_mode):
            target.symlink_to(os.readlink(child))
        elif stat.S_ISREG(details.st_mode):
            shutil.copy2(child, target, follow_symlinks=False)
        elif stat.S_ISDIR(details.st_mode):
            shutil.copytree(child, target, symlinks=True, copy_function=shutil.copy2)
        else:
            raise GitControlError("worker produced an unsupported filesystem entry")


__all__ = [
    "GitControlError",
    "create_standalone_clone",
    "detached_head_from_control",
    "fingerprint_git_control",
    "remove_standalone_clone",
    "replace_worktree_from_untrusted",
    "require_git",
    "run_git",
    "sanitized_git_environment",
]
