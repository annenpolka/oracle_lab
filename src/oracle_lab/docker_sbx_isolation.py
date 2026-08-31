"""Fail-closed Docker ``sbx`` microVM broker for coding workers.

Configuration describes an intended boundary.  This module does not turn that
description into authority until ``bind`` has measured the installed binary,
the client/server contract, the immutable template identity, the effective
network policy, and an ephemeral negative-probe sandbox.  Every sandbox is
destroyed and its disappearance is observed before bytes cross back into a
trusted worktree.

The Docker CLI JSON surfaces are deliberately decoded as versioned protocols.
Unknown keys, missing keys, duplicate JSON keys, update banners, and future
schemas are errors.  This is stricter than a normal CLI integration because an
empty or partially decoded policy would otherwise be interpreted as proof of
isolation.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import selectors
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from oracle_lab.coding_isolation import (
    PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS,
    REQUIRED_ISOLATION_CAPABILITIES,
    SAFE_ISOLATED_ENVIRONMENT_NAMES,
    CodingIsolationError,
    CodingWorkerIsolationBinding,
    CodingWorkerIsolationBroker,
    IsolationAttestation,
    IsolationRunFailed,
    IsolationRunRequest,
    IsolationRunResult,
    receipt_sha256,
)
from oracle_lab.git_control import (
    GitControlError,
    fingerprint_git_control,
    require_git,
)
from oracle_lab.jsonutil import canonical_json
from oracle_lab.workspace_archive import (
    WorkspaceArchiveError,
    WorkspaceArchiveLimits,
    validate_workspace_archive,
)

_BACKEND_ID = "docker-sbx-microvm"
_SYNTHETIC_BACKEND_ID = "docker-sbx-synthetic-fixture"
_CONFORMANCE_SUITE_VERSION = "oracle-lab-docker-sbx-v1"
_SYNTHETIC_EVIDENCE_ORIGIN = "synthetic_fixture"
_MINIMUM_VERSION = (0, 39, 0)
_CONTROL_TIMEOUT_SECONDS = 60.0
_CREATE_TIMEOUT_SECONDS = 300.0
_CONTROL_MAX_OUTPUT_BYTES = 1024 * 1024
_PROBE_MAX_OUTPUT_BYTES = 256 * 1024
_PINNED_TEMPLATE = re.compile(r"^([^\s@]+)@sha256:([0-9a-f]{64})$")
_SANDBOX_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{1,199}$")
_HOST = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
# This grammar is retained only for the explicitly synthetic lifecycle fixture.
# A real standalone sbx v0.39.0 observed on 2026-08-31 prints a single
# ``sbx version: ...`` line and exposes daemon health separately through
# ``sbx diagnose``.  The production guard in ``bind`` must remain in place
# until that real protocol has its own reviewed identity contract.
_VERSION_OUTPUT = re.compile(
    rb"\AClient Version:\s{2}v(0|[1-9][0-9]*)\."
    rb"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*) ([0-9a-f]{7,64})\n"
    rb"Server Version:\s{2}v(0|[1-9][0-9]*)\."
    rb"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*) ([0-9a-f]{7,64})\n\Z"
)


# This script is fixed Host code, not model-authored shell.  It emits the same
# intentionally small archive format validated by ``workspace_archive`` and
# never follows symlinks.  The resulting stdout is still untrusted until the
# Host validator accepts every frame and aggregate limit.
_GUEST_WORKSPACE_EXPORT_SCRIPT = r"""
import os
import stat
import struct
import sys

MAGIC = b"ORACLELAB-WORKSPACE-V1\x00"
HEADER = struct.Struct(">BIIQ")
ROOT = os.getcwd()
MAX_RAW = int(sys.argv[1])
MAX_ENTRIES = int(sys.argv[2])
entries = []
payload_total = 0

def visit(directory, components):
    global payload_total
    children = sorted(os.scandir(directory), key=lambda item: item.name.encode("utf-8", "strict"))
    for child in children:
        if not components and child.name == ".git":
            continue
        child_components = components + (child.name,)
        raw_path = "/".join(child_components).encode("utf-8", "strict")
        details = child.stat(follow_symlinks=False)
        mode = stat.S_IMODE(details.st_mode)
        if mode > 0o777:
            raise RuntimeError("special permission bits are forbidden")
        if stat.S_ISDIR(details.st_mode):
            kind = 1
            payload = b""
        elif stat.S_ISREG(details.st_mode):
            if details.st_nlink != 1:
                raise RuntimeError("hard-linked entries are forbidden")
            kind = 2
            nofollow = getattr(os, "O_NOFOLLOW", None)
            if nofollow is None:
                raise RuntimeError("O_NOFOLLOW is unavailable")
            descriptor = os.open(child.path, os.O_RDONLY | nofollow)
            try:
                opened = os.fstat(descriptor)
                identity = lambda value: (
                    value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
                    value.st_size, value.st_mtime_ns, value.st_ctime_ns,
                )
                if identity(opened) != identity(details) or not stat.S_ISREG(opened.st_mode):
                    raise RuntimeError("workspace changed during export")
                chunks = []
                size = 0
                while True:
                    block = os.read(descriptor, min(1024 * 1024, MAX_RAW + 1))
                    if not block:
                        break
                    size += len(block)
                    payload_total += len(block)
                    if size > details.st_size or payload_total > MAX_RAW:
                        raise RuntimeError("workspace export exceeds byte limit")
                    chunks.append(block)
                after = os.fstat(descriptor)
                if identity(after) != identity(details) or size != details.st_size:
                    raise RuntimeError("workspace changed during export")
                payload = b"".join(chunks)
            finally:
                os.close(descriptor)
        elif stat.S_ISLNK(details.st_mode):
            if details.st_nlink != 1:
                raise RuntimeError("hard-linked entries are forbidden")
            kind = 3
            mode = 0o777
            payload = os.readlink(child.path).encode("utf-8", "strict")
        else:
            raise RuntimeError("unsupported workspace entry")
        entries.append((raw_path, kind, mode, payload))
        if len(entries) > MAX_ENTRIES:
            raise RuntimeError("workspace export exceeds entry limit")
        if kind == 1:
            visit(child.path, child_components)

visit(ROOT, ())
entries.sort(key=lambda item: item[0])
size = len(MAGIC) + 8
for raw_path, _kind, _mode, payload in entries:
    size += HEADER.size + len(raw_path) + len(payload)
    if size > MAX_RAW:
        raise RuntimeError("workspace export exceeds raw byte limit")
out = bytearray(MAGIC)
out.extend(struct.pack(">Q", len(entries)))
for raw_path, kind, mode, payload in entries:
    out.extend(HEADER.pack(kind, mode, len(raw_path), len(payload)))
    out.extend(raw_path)
    out.extend(payload)
sys.stdout.buffer.write(out)
""".strip()


_GUEST_CONFORMANCE_PROBE_SCRIPT = r"""
import json
import os
import pathlib
import platform
import subprocess
import sys

workspace = pathlib.Path(sys.argv[1])
host_marker = pathlib.Path(sys.argv[2])
host_home = pathlib.Path(sys.argv[3])
sentinel = sys.argv[4].encode("ascii")
source_marker = workspace / "source-marker.txt"
guest_marker = workspace / "guest-private-marker.txt"

process_sentinel_visible = False
proc = pathlib.Path("/proc")
if proc.is_dir():
    for child in proc.iterdir():
        if not child.name.isdigit():
            continue
        for leaf in ("cmdline", "environ"):
            try:
                if sentinel in (child / leaf).read_bytes():
                    process_sentinel_visible = True
            except (OSError, PermissionError):
                pass

source_marker.write_text("guest-private-change\n", encoding="utf-8")
guest_marker.write_text("private-only\n", encoding="utf-8")
mountinfo = b""
try:
    mountinfo = pathlib.Path("/proc/self/mountinfo").read_bytes()
except OSError:
    pass
guest_home = pathlib.Path.home()
shared_state_mount_visible = any(
    marker in mountinfo
    for marker in (b"/.codex/skills", b"/.agents/skills", b"/.config/opencode")
)
credential_name_fragments = ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "AUTH")
unsafe_credential_environment = any(
    any(fragment in name.upper() for fragment in credential_name_fragments)
    and value not in {"", "proxy-managed"}
    for name, value in os.environ.items()
)
ssh_auth_sock = os.environ.get("SSH_AUTH_SOCK", "")
host_docker = os.environ.get("DOCKER_HOST", "")
descendant = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(600)"],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
)
result = {
    "schema_version": 1,
    "linux_guest": platform.system() == "Linux" and proc.is_dir(),
    "host_marker_visible": os.path.lexists(host_marker),
    "host_home_visible": os.path.lexists(host_home),
    "host_process_sentinel_visible": process_sentinel_visible,
    "credential_sentinel_visible": any(
        sentinel in value.encode("utf-8", "surrogateescape")
        for value in os.environ.values()
    ),
    "sandbox_docker_socket_visible": os.path.lexists("/var/run/docker.sock"),
    "host_docker_endpoint_visible": bool(host_docker) and (
        str(host_home) in host_docker or "host.docker.internal" in host_docker
    ),
    "ssh_auth_sock_visible": bool(ssh_auth_sock) and os.path.lexists(ssh_auth_sock),
    "unsafe_credential_environment": unsafe_credential_environment,
    "shared_agent_state_mount_visible": shared_state_mount_visible,
    "guest_home": str(guest_home),
    "guest_write_complete": source_marker.read_text(encoding="utf-8") == "guest-private-change\n"
        and guest_marker.read_text(encoding="utf-8") == "private-only\n",
    "detached_descendant_pid": descendant.pid,
}
sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")))
""".strip()


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Bounded result of one argv-only Host control process."""

    argv: tuple[str, ...]
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool = False
    output_limited: bool = False


class CommandRunner(Protocol):
    evidence_origin: str

    def run(
        self,
        argv: Sequence[str],
        *,
        input_bytes: bytes,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> CommandResult: ...


class SubprocessCommandRunner:
    """Run one process group without buffering beyond the configured cap."""

    evidence_origin = "production"

    @staticmethod
    def _terminate(process: subprocess.Popen[bytes]) -> None:
        if os.name == "posix":
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(ProcessLookupError, OSError):
            process.kill()

    def run(
        self,
        argv: Sequence[str],
        *,
        input_bytes: bytes,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> CommandResult:
        command = tuple(argv)
        if not command or timeout_seconds <= 0 or max_output_bytes <= 0:
            raise CodingIsolationError("invalid bounded broker command")
        if any(not isinstance(value, str) or "\x00" in value for value in command):
            raise CodingIsolationError("broker command contains an invalid argument")

        with tempfile.TemporaryFile() as input_handle:
            input_handle.write(input_bytes)
            input_handle.seek(0)
            try:
                process = subprocess.Popen(
                    list(command),
                    stdin=input_handle,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=dict(environment),
                    shell=False,
                    start_new_session=True,
                )
            except OSError as error:
                raise CodingIsolationError("isolation broker process could not start") from error
            assert process.stdout is not None and process.stderr is not None
            selector = selectors.DefaultSelector()
            buffers = {"stdout": bytearray(), "stderr": bytearray()}
            for name, stream in (("stdout", process.stdout), ("stderr", process.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)

            deadline = time.monotonic() + timeout_seconds
            timed_out = False
            output_limited = False

            def consume(key: selectors.SelectorKey) -> None:
                nonlocal output_limited
                used = len(buffers["stdout"]) + len(buffers["stderr"])
                remaining = max_output_bytes - used
                try:
                    block = os.read(key.fileobj.fileno(), min(65_536, max(1, remaining + 1)))
                except BlockingIOError:
                    return
                if not block:
                    with contextlib.suppress(KeyError):
                        selector.unregister(key.fileobj)
                    return
                buffers[str(key.data)].extend(block[: max(0, remaining)])
                if len(block) > remaining:
                    output_limited = True

            try:
                while selector.get_map() and not output_limited:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        break
                    for key, _mask in selector.select(min(remaining, 0.1)):
                        consume(key)
                        if output_limited:
                            break
                if not timed_out and not output_limited:
                    try:
                        process.wait(timeout=max(0.001, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        timed_out = True
                if timed_out or output_limited:
                    self._terminate(process)
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=2)
                drain_deadline = time.monotonic() + 1
                while selector.get_map() and time.monotonic() < drain_deadline:
                    ready = selector.select(0.05)
                    if not ready:
                        if process.poll() is not None:
                            break
                        continue
                    for key, _mask in ready:
                        consume(key)
            finally:
                selector.close()
                for stream in (process.stdout, process.stderr):
                    with contextlib.suppress(OSError):
                        stream.close()
                if process.poll() is None:
                    self._terminate(process)
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=2)

        return CommandResult(
            argv=command,
            exit_code=process.returncode,
            stdout=bytes(buffers["stdout"]),
            stderr=bytes(buffers["stderr"]),
            timed_out=timed_out,
            output_limited=output_limited,
        )


@dataclass(frozen=True, slots=True)
class _SbxVersion:
    client: tuple[int, int, int]
    server: tuple[int, int, int]
    client_text: str
    server_text: str


@dataclass(frozen=True, slots=True)
class _PolicyRule:
    id: str
    name: str
    policy_id: str
    scope: str
    applies_to: str
    resource_type: str
    decision: str
    resources: tuple[str, ...]
    origin: str
    layer: str
    status: str
    editable: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "policy_id": self.policy_id,
            "scope": self.scope,
            "applies_to": self.applies_to,
            "resource_type": self.resource_type,
            "decision": self.decision,
            "resources": list(self.resources),
            "origin": self.origin,
            "layer": self.layer,
            "status": self.status,
            "editable": self.editable,
        }


@dataclass(frozen=True, slots=True)
class _RuntimeIdentity:
    executable_path: str
    executable_sha256: str
    version: _SbxVersion
    template_reference: str
    template_identity: str
    global_policy: tuple[_PolicyRule, ...]
    global_policy_sha256: str
    json_schema: str


@dataclass(frozen=True, slots=True)
class _BoundProfile:
    id: str
    adapter: str
    executable: str
    model: str | None
    timeout_seconds: float
    max_output_bytes: int
    max_workspace_export_bytes: int
    max_workspace_entries: int
    environment_names: tuple[str, ...]
    allowed_hosts: tuple[str, ...]
    template_reference: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "adapter": self.adapter,
            "executable": self.executable,
            "model": self.model,
            "timeout_seconds": self.timeout_seconds,
            "max_output_bytes": self.max_output_bytes,
            "max_workspace_export_bytes": self.max_workspace_export_bytes,
            "max_workspace_entries": self.max_workspace_entries,
            "environment_names": list(self.environment_names),
            "allowed_hosts": list(self.allowed_hosts),
            "template_reference": self.template_reference,
        }


def _decode_json_object(raw: bytes, *, command: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise CodingIsolationError(f"sbx {command} JSON contains duplicate keys")
            result[key] = value
        return result

    try:
        decoded = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CodingIsolationError(f"sbx {command} did not return one strict JSON value") from error
    if not isinstance(decoded, dict):
        raise CodingIsolationError(f"sbx {command} JSON root must be an object")
    return decoded


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    label: str,
) -> None:
    actual = set(value)
    if actual != expected:
        raise CodingIsolationError(
            f"{label} JSON schema changed: expected {sorted(expected)}, got {sorted(actual)}"
        )


def _parse_version(raw: bytes) -> _SbxVersion:
    match = _VERSION_OUTPUT.fullmatch(raw)
    if match is None:
        raise CodingIsolationError("sbx version output does not match the recognized grammar")
    values = match.groups()
    client = tuple(int(value) for value in values[:3])
    server = tuple(int(value) for value in values[4:7])
    assert len(client) == 3 and len(server) == 3
    if client < _MINIMUM_VERSION or server < _MINIMUM_VERSION:
        raise CodingIsolationError("sbx client and server must both be at least v0.39.0")
    client_text = f"v{client[0]}.{client[1]}.{client[2]} {values[3].decode('ascii')}"
    server_text = f"v{server[0]}.{server[1]}.{server[2]} {values[7].decode('ascii')}"
    return _SbxVersion(client, server, client_text, server_text)


def _json_schema_for(version: _SbxVersion) -> str:
    # This is the staged synthetic v0.39 fixture contract, not production
    # evidence.  A future or real protocol needs a separately reviewed decoder.
    if version.client[:2] != (0, 39) or version.server[:2] != (0, 39):
        raise CodingIsolationError("installed sbx JSON contract has not been reviewed")
    return "docker-sbx-0.39-json-v1"


def _decode_template_identity(raw: bytes, *, reference: str) -> str:
    root = _decode_json_object(raw, command="template ls --json")
    _require_exact_keys(root, {"images"}, label="sbx template inventory")
    images = root["images"]
    if not isinstance(images, list):
        raise CodingIsolationError("sbx template inventory images must be an array")
    pinned = _PINNED_TEMPLATE.fullmatch(reference)
    if pinned is None:
        raise CodingIsolationError("isolation template must be pinned by lowercase SHA-256")
    repository, digest_hex = pinned.groups()
    wanted_digest = f"sha256:{digest_hex}"
    matches: list[str] = []
    base_keys = {"id", "repository", "tag", "flavor", "created_at", "size"}
    digest_keys = base_keys | {"digest"}
    for index, image in enumerate(images):
        if not isinstance(image, dict):
            raise CodingIsolationError("sbx template inventory entry must be an object")
        if set(image) not in {frozenset(base_keys), frozenset(digest_keys)}:
            raise CodingIsolationError(f"sbx template inventory entry {index} schema changed")
        for key in ("id", "repository", "tag", "flavor", "created_at"):
            if not isinstance(image[key], str):
                raise CodingIsolationError(f"sbx template inventory entry {index} is malformed")
        if isinstance(image["size"], bool) or not isinstance(image["size"], int):
            raise CodingIsolationError(f"sbx template inventory entry {index} size is malformed")
        if image["repository"] != repository:
            continue
        digest = image.get("digest")
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise CodingIsolationError(
                "sbx template inventory does not expose an attestable registry digest"
            )
        if digest == wanted_digest:
            matches.append(digest)
    if matches != [wanted_digest]:
        raise CodingIsolationError("pinned isolation template identity is absent or ambiguous")
    return wanted_digest


def _decode_policy(raw: bytes, *, label: str) -> tuple[_PolicyRule, ...]:
    root = _decode_json_object(raw, command=label)
    _require_exact_keys(root, {"rules"}, label=f"sbx {label}")
    raw_rules = root["rules"]
    if not isinstance(raw_rules, list):
        raise CodingIsolationError(f"sbx {label} rules must be an array")
    keys = {
        "id",
        "name",
        "policy_id",
        "scope",
        "applies_to",
        "resource_type",
        "decision",
        "resources",
        "origin",
        "layer",
        "status",
        "editable",
    }
    rules: list[_PolicyRule] = []
    for index, value in enumerate(raw_rules):
        if not isinstance(value, dict):
            raise CodingIsolationError(f"sbx {label} rule {index} must be an object")
        _require_exact_keys(value, keys, label=f"sbx {label} rule {index}")
        string_keys = keys - {"resources", "editable"}
        if any(not isinstance(value[key], str) for key in string_keys):
            raise CodingIsolationError(f"sbx {label} rule {index} string field is malformed")
        resources = value["resources"]
        if not isinstance(resources, list) or any(
            not isinstance(resource, str) or not resource for resource in resources
        ):
            raise CodingIsolationError(f"sbx {label} rule {index} resources are malformed")
        if not isinstance(value["editable"], bool):
            raise CodingIsolationError(f"sbx {label} rule {index} editable is malformed")
        if value["resource_type"] != "network":
            raise CodingIsolationError(f"sbx {label} returned a non-network rule")
        if value["decision"] not in {"allow", "deny"}:
            raise CodingIsolationError(f"sbx {label} returned an unknown decision")
        if value["origin"] not in {"local", "org", "kit"}:
            raise CodingIsolationError(f"sbx {label} returned an unknown origin")
        if value["layer"] not in {"local", "org", "kit"}:
            raise CodingIsolationError(f"sbx {label} returned an unknown layer")
        if value["status"] not in {"active", "inactive"}:
            raise CodingIsolationError(f"sbx {label} returned an unknown rule status")
        rules.append(
            _PolicyRule(
                id=value["id"],
                name=value["name"],
                policy_id=value["policy_id"],
                scope=value["scope"],
                applies_to=value["applies_to"],
                resource_type=value["resource_type"],
                decision=value["decision"],
                resources=tuple(resources),
                origin=value["origin"],
                layer=value["layer"],
                status=value["status"],
                editable=value["editable"],
            )
        )
    rules.sort(key=lambda rule: canonical_json(rule.to_dict()))
    return tuple(rules)


def _policy_document(rules: Sequence[_PolicyRule]) -> dict[str, Any]:
    return {"rules": [rule.to_dict() for rule in rules]}


def _require_locked_down_global_policy(rules: Sequence[_PolicyRule]) -> None:
    if any(rule.origin == "org" or rule.layer == "org" for rule in rules):
        raise CodingIsolationError(
            "organization network governance is not an attestable local policy"
        )
    if any(rule.status == "active" and rule.decision == "allow" for rule in rules):
        raise CodingIsolationError("global sbx network policy is not Locked Down")


def _require_exact_sandbox_policy(
    global_rules: Sequence[_PolicyRule],
    sandbox_rules: Sequence[_PolicyRule],
    *,
    sandbox: str,
    allowed_hosts: Sequence[str],
) -> None:
    global_documents = {canonical_json(rule.to_dict()) for rule in global_rules}
    inherited: set[str] = set()
    local_allows: list[_PolicyRule] = []
    for rule in sandbox_rules:
        document = canonical_json(rule.to_dict())
        if document in global_documents:
            inherited.add(document)
            continue
        if (
            rule.origin != "local"
            or rule.layer != "local"
            or rule.status != "active"
            or rule.decision != "allow"
            or rule.scope == "global"
            or rule.applies_to not in {sandbox, f"sandbox:{sandbox}"}
        ):
            raise CodingIsolationError("sandbox effective policy contains an unexpected rule")
        local_allows.append(rule)
    if inherited != global_documents:
        raise CodingIsolationError("sandbox effective policy omitted a global rule")
    observed_resources = {resource for rule in local_allows for resource in rule.resources}
    expected_resources = set(allowed_hosts)
    if observed_resources != expected_resources:
        raise CodingIsolationError("sandbox effective network allowlist is not exact")


def _decode_policy_check(raw: bytes, *, target: str, expected: bool) -> None:
    value = _decode_json_object(raw, command="policy check network --json")
    required = {
        "allowed",
        "action",
        "context",
        "governance",
        "resource_type",
        "resource_value",
        "target",
        "type",
    }
    optional = {"deny_kind", "origin", "reason", "rule"}
    if not required.issubset(value) or not set(value).issubset(required | optional):
        raise CodingIsolationError("sbx policy check JSON schema changed")
    if value["allowed"] is not expected or value["target"] != target:
        raise CodingIsolationError("sbx policy check returned an unexpected decision")
    if value["type"] != "network" or value["action"] != "net:connect:tcp":
        raise CodingIsolationError("sbx policy check returned an unknown action")
    governance = value["governance"]
    if not isinstance(governance, dict):
        raise CodingIsolationError("sbx policy check governance is malformed")
    governance_keys = {
        "active",
        "organization",
        "organization_unavailable",
        "last_synced_status",
        "last_synced_message",
    }
    _require_exact_keys(governance, governance_keys, label="sbx policy check governance")
    if governance["active"] is not False or governance["organization_unavailable"] is not False:
        raise CodingIsolationError("sbx policy check reports remote governance")
    if expected and value.get("origin") not in {None, "", "local"}:
        raise CodingIsolationError("sbx policy allow decision did not originate locally")
    if not expected and value.get("deny_kind") != "implicit":
        raise CodingIsolationError("sbx default-deny probe was not an implicit denial")


def _decode_policy_log(raw: bytes, *, sandbox: str, allowed_hosts: Sequence[str]) -> None:
    value = _decode_json_object(raw, command="policy log --json")
    _require_exact_keys(value, {"allowed_hosts", "blocked_hosts"}, label="sbx policy log")
    expected_keys = {
        "host",
        "vm_name",
        "proxy_type",
        "rule",
        "last_seen",
        "since",
        "count_since",
    }
    expected_hosts = set(allowed_hosts) | {f"{host}:443" for host in allowed_hosts}
    for group in ("allowed_hosts", "blocked_hosts"):
        entries = value[group]
        if not isinstance(entries, list):
            raise CodingIsolationError("sbx policy log entries must be arrays")
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise CodingIsolationError("sbx policy log entry must be an object")
            _require_exact_keys(entry, expected_keys, label=f"sbx policy log {group} {index}")
            if (
                any(not isinstance(entry[key], str) for key in expected_keys - {"count_since"})
                or isinstance(entry["count_since"], bool)
                or not isinstance(entry["count_since"], int)
            ):
                raise CodingIsolationError("sbx policy log entry is malformed")
            if entry["vm_name"] != sandbox:
                raise CodingIsolationError("sbx policy log contains another sandbox")
            if group == "allowed_hosts" and entry["host"] not in expected_hosts:
                raise CodingIsolationError("sandbox contacted a host outside the exact allowlist")


def _decode_conformance_probe(raw: bytes) -> dict[str, Any]:
    value = _decode_json_object(raw, command="exec conformance probe")
    keys = {
        "schema_version",
        "linux_guest",
        "host_marker_visible",
        "host_home_visible",
        "host_process_sentinel_visible",
        "credential_sentinel_visible",
        "sandbox_docker_socket_visible",
        "host_docker_endpoint_visible",
        "ssh_auth_sock_visible",
        "unsafe_credential_environment",
        "shared_agent_state_mount_visible",
        "guest_home",
        "guest_write_complete",
        "detached_descendant_pid",
    }
    _require_exact_keys(value, keys, label="sbx conformance probe")
    if value["schema_version"] != 1 or value["linux_guest"] is not True:
        raise CodingIsolationError("sbx conformance probe did not observe a Linux guest boundary")
    for key in (
        "host_marker_visible",
        "host_home_visible",
        "host_process_sentinel_visible",
        "credential_sentinel_visible",
        "host_docker_endpoint_visible",
        "ssh_auth_sock_visible",
        "unsafe_credential_environment",
        "shared_agent_state_mount_visible",
    ):
        if value[key] is not False:
            raise CodingIsolationError(f"sbx conformance negative probe failed: {key}")
    if value["guest_write_complete"] is not True:
        raise CodingIsolationError("sbx private clone was not writable")
    descendant = value["detached_descendant_pid"]
    if isinstance(descendant, bool) or not isinstance(descendant, int) or descendant <= 1:
        raise CodingIsolationError("sbx descendant probe did not start")
    if not isinstance(value["sandbox_docker_socket_visible"], bool):
        raise CodingIsolationError("sbx sandbox Docker socket observation is malformed")
    if not isinstance(value["guest_home"], str) or not value["guest_home"].startswith("/"):
        raise CodingIsolationError("sbx guest home observation is malformed")
    return value


def _parse_quiet_names(raw: bytes) -> frozenset[str]:
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise CodingIsolationError("sbx ls --quiet output is not UTF-8") from error
    names: list[str] = []
    for line in text.splitlines():
        if not line:
            continue
        if _SANDBOX_NAME.fullmatch(line) is None:
            raise CodingIsolationError("sbx ls --quiet returned an unknown output line")
        names.append(line)
    if len(set(names)) != len(names):
        raise CodingIsolationError("sbx ls --quiet returned duplicate sandbox names")
    return frozenset(names)


def _sha256_regular_executable(configured: str) -> tuple[str, str]:
    candidate_text = shutil.which(configured) if not Path(configured).is_absolute() else configured
    if candidate_text is None:
        raise CodingIsolationError("sbx isolation broker executable is unavailable")
    try:
        resolved = Path(candidate_text).expanduser().resolve(strict=True)
        details = resolved.lstat()
    except OSError as error:
        raise CodingIsolationError("sbx isolation broker executable cannot be resolved") from error
    if (
        not resolved.is_absolute()
        or stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
    ):
        raise CodingIsolationError("sbx isolation broker must resolve to a regular non-symlink")
    if not os.access(resolved, os.X_OK):
        raise CodingIsolationError("sbx isolation broker is not executable")
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise CodingIsolationError("sbx executable hashing requires O_NOFOLLOW")
    try:
        descriptor = os.open(resolved, os.O_RDONLY | nofollow)
    except OSError as error:
        raise CodingIsolationError("sbx isolation broker could not be opened safely") from error
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        while block := os.read(descriptor, 1024 * 1024):
            digest.update(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)

    def identity(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if identity(before) != identity(after) or not stat.S_ISREG(before.st_mode):
        raise CodingIsolationError("sbx isolation broker changed while it was hashed")
    return str(resolved), digest.hexdigest()


class DockerSbxIsolationBroker(CodingWorkerIsolationBroker):
    """Factory for profile-bound, conformance-attested ``sbx`` bindings."""

    def __init__(
        self,
        *,
        executable: str = "sbx",
        state_root: str | Path,
        workspace_root: str | Path | None = None,
        runner: CommandRunner | None = None,
        clock: Callable[[], datetime] | None = None,
        synthetic_fixture: bool = False,
    ) -> None:
        if not isinstance(executable, str) or not executable.strip() or "\x00" in executable:
            raise CodingIsolationError("sbx isolation broker executable is invalid")
        self._configured_executable = executable
        self._state_root = Path(state_root).expanduser()
        self._runner = runner or SubprocessCommandRunner()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._workspace_root = None if workspace_root is None else Path(workspace_root).expanduser()
        self._synthetic_fixture = synthetic_fixture
        if synthetic_fixture and (
            runner is None or getattr(runner, "evidence_origin", None) != _SYNTHETIC_EVIDENCE_ORIGIN
        ):
            raise CodingIsolationError(
                "synthetic_fixture isolation requires an explicitly marked synthetic runner"
            )

    def _environment(self, *, probe_sentinel: str | None = None) -> dict[str, str]:
        environment = {
            key: os.environ[key]
            for key in (
                "HOME",
                "LANG",
                "LC_ALL",
                "TMPDIR",
                "XDG_CONFIG_HOME",
                "XDG_RUNTIME_DIR",
                "XDG_STATE_HOME",
            )
            if key in os.environ
        }
        environment["SBX_NO_TELEMETRY"] = "1"
        if probe_sentinel is not None:
            environment["ORACLE_LAB_CREDENTIAL_PROBE"] = probe_sentinel
        return environment

    def _run(
        self,
        executable: str,
        arguments: Sequence[str],
        *,
        input_bytes: bytes = b"",
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = _CONTROL_TIMEOUT_SECONDS,
        max_output_bytes: int = _CONTROL_MAX_OUTPUT_BYTES,
    ) -> CommandResult:
        return self._runner.run(
            (executable, *arguments),
            input_bytes=input_bytes,
            environment=self._environment() if environment is None else environment,
            timeout_seconds=timeout_seconds,
            max_output_bytes=max_output_bytes,
        )

    def _checked(
        self,
        executable: str,
        arguments: Sequence[str],
        **options: Any,
    ) -> CommandResult:
        result = self._run(executable, arguments, **options)
        operation = " ".join(arguments[:3])
        if result.timed_out:
            raise CodingIsolationError(f"sbx {operation} timed out")
        if result.output_limited:
            raise CodingIsolationError(f"sbx {operation} exceeded its output limit")
        if result.exit_code != 0:
            raise CodingIsolationError(f"sbx {operation} failed with exit code {result.exit_code}")
        return result

    def _state_directory(self) -> Path:
        requested = self._state_root.absolute()
        if requested.is_symlink():
            raise CodingIsolationError("isolation broker state root may not be a symlink")
        try:
            requested.mkdir(parents=True, exist_ok=True)
            details = requested.lstat()
        except OSError as error:
            raise CodingIsolationError("isolation broker state root is unavailable") from error
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise CodingIsolationError("isolation broker state root must be a real directory")
        return requested.resolve(strict=True)

    def _read_version(self, executable: str) -> _SbxVersion:
        return _parse_version(self._checked(executable, ("version",)).stdout)

    def _read_global_policy(self, executable: str) -> tuple[_PolicyRule, ...]:
        result = self._checked(
            executable,
            ("policy", "ls", "--type", "network", "--include-inactive", "--json"),
        )
        rules = _decode_policy(result.stdout, label="policy ls --json")
        _require_locked_down_global_policy(rules)
        return rules

    def _read_sandbox_policy(
        self,
        executable: str,
        sandbox: str,
    ) -> tuple[_PolicyRule, ...]:
        result = self._checked(
            executable,
            (
                "policy",
                "ls",
                sandbox,
                "--type",
                "network",
                "--include-inactive",
                "--json",
            ),
        )
        return _decode_policy(result.stdout, label="policy ls sandbox --json")

    def _read_names(self, executable: str) -> frozenset[str]:
        return _parse_quiet_names(self._checked(executable, ("ls", "--quiet")).stdout)

    def _read_template_identity(self, executable: str, reference: str) -> str:
        raw = self._checked(executable, ("template", "ls", "--json")).stdout
        return _decode_template_identity(raw, reference=reference)

    def _measure_runtime_identity(self, profile: Any) -> _RuntimeIdentity:
        reference = getattr(profile, "isolation_template_reference", None)
        if not isinstance(reference, str) or _PINNED_TEMPLATE.fullmatch(reference) is None:
            raise CodingIsolationError("isolation template must be pinned by lowercase SHA-256")
        executable, executable_sha256 = _sha256_regular_executable(self._configured_executable)
        version = self._read_version(executable)
        schema = _json_schema_for(version)
        template_identity = self._read_template_identity(executable, reference)
        global_policy = self._read_global_policy(executable)
        global_policy_sha256 = hashlib.sha256(
            canonical_json(_policy_document(global_policy)).encode("utf-8")
        ).hexdigest()
        return _RuntimeIdentity(
            executable_path=executable,
            executable_sha256=executable_sha256,
            version=version,
            template_reference=reference,
            template_identity=template_identity,
            global_policy=global_policy,
            global_policy_sha256=global_policy_sha256,
            json_schema=schema,
        )

    @staticmethod
    def _profile_hosts(profile: Any) -> tuple[str, ...]:
        raw = getattr(profile, "isolation_allowed_hosts", ())
        if not isinstance(raw, (list, tuple)):
            raise CodingIsolationError("isolation allowed hosts must be a sequence")
        hosts = tuple(raw)
        if any(
            not isinstance(host, str)
            or host != host.lower().rstrip(".")
            or _HOST.fullmatch(host) is None
            for host in hosts
        ):
            raise CodingIsolationError("isolation allowed hosts must be exact DNS names")
        if len(set(hosts)) != len(hosts):
            raise CodingIsolationError("isolation allowed hosts must be unique")
        return tuple(sorted(hosts))

    @staticmethod
    def _profile_environment_names(profile: Any) -> tuple[str, ...]:
        raw = getattr(profile, "allowed_environment_names", ())
        if not isinstance(raw, (list, tuple)):
            raise CodingIsolationError("isolated environment names must be a sequence")
        names = tuple(raw)
        if any(name not in SAFE_ISOLATED_ENVIRONMENT_NAMES for name in names):
            raise CodingIsolationError("isolated worker environment contains a sensitive name")
        if len(set(names)) != len(names):
            raise CodingIsolationError("isolated worker environment names must be unique")
        return tuple(sorted(set(names)))

    @staticmethod
    def _validate_profile(profile: Any) -> None:
        if getattr(profile, "adapter", None) not in {"codex", "opencode"}:
            raise CodingIsolationError("Docker sbx broker only binds codex or opencode")
        if getattr(profile, "sandbox_profile", None) != "external-broker":
            raise CodingIsolationError("Docker sbx broker requires sandbox_profile=external-broker")

    def _workspace_boundary(self) -> tuple[Path, Path]:
        if self._workspace_root is None:
            raise CodingIsolationError("coding-worker isolation workspace_root is required")
        if not self._workspace_root.is_absolute() or not self._state_root.is_absolute():
            raise CodingIsolationError("isolation workspace_root and state_root must be absolute")
        requested_workspace = self._workspace_root.absolute()
        if requested_workspace.is_symlink():
            raise CodingIsolationError("isolation workspace_root may not be a symlink")
        try:
            requested_workspace.mkdir(parents=True, exist_ok=True)
            workspace_details = requested_workspace.lstat()
            workspace_root = requested_workspace.resolve(strict=True)
        except OSError as error:
            raise CodingIsolationError("isolation workspace_root is unavailable") from error
        if stat.S_ISLNK(workspace_details.st_mode) or not stat.S_ISDIR(workspace_details.st_mode):
            raise CodingIsolationError("isolation workspace_root must be a real directory")
        if self._state_root.is_symlink():
            raise CodingIsolationError("isolation state_root may not be a symlink")
        state_root = self._state_root.resolve(strict=False)
        if (
            workspace_root == state_root
            or workspace_root.is_relative_to(state_root)
            or state_root.is_relative_to(workspace_root)
        ):
            raise CodingIsolationError("isolation workspace_root and state_root must be disjoint")
        return workspace_root, state_root

    def _bound_profile(
        self,
        profile: Any,
        *,
        hosts: tuple[str, ...],
        environment_names: tuple[str, ...],
    ) -> _BoundProfile:
        self._validate_profile(profile)
        profile_id = getattr(profile, "id", None)
        executable = getattr(profile, "executable", None)
        model = getattr(profile, "model", None)
        if not isinstance(profile_id, str) or not profile_id or "\x00" in profile_id:
            raise CodingIsolationError("isolated profile id is invalid")
        if not isinstance(executable, str) or not executable or "\x00" in executable:
            raise CodingIsolationError("isolated profile executable is invalid")
        if model is not None and (not isinstance(model, str) or not model or "\x00" in model):
            raise CodingIsolationError("isolated profile model is invalid")
        timeout_seconds = getattr(profile, "timeout_seconds", None)
        max_output_bytes = getattr(profile, "max_output_bytes", None)
        max_export_bytes = getattr(profile, "max_workspace_export_bytes", None)
        max_entries = getattr(profile, "max_workspace_entries", None)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or isinstance(max_output_bytes, bool)
            or not isinstance(max_output_bytes, int)
            or max_output_bytes <= 0
            or isinstance(max_export_bytes, bool)
            or not isinstance(max_export_bytes, int)
            or max_export_bytes <= 0
            or isinstance(max_entries, bool)
            or not isinstance(max_entries, int)
            or max_entries <= 0
        ):
            raise CodingIsolationError("isolated profile limits are invalid")
        reference = getattr(profile, "isolation_template_reference", None)
        if not isinstance(reference, str) or _PINNED_TEMPLATE.fullmatch(reference) is None:
            raise CodingIsolationError("isolation template must be pinned by lowercase SHA-256")
        return _BoundProfile(
            id=profile_id,
            adapter=str(profile.adapter),
            executable=executable,
            model=model,
            timeout_seconds=float(timeout_seconds),
            max_output_bytes=max_output_bytes,
            max_workspace_export_bytes=max_export_bytes,
            max_workspace_entries=max_entries,
            environment_names=environment_names,
            allowed_hosts=hosts,
            template_reference=reference,
        )

    @staticmethod
    def _require_canonical_command(
        profile: _BoundProfile,
        request: IsolationRunRequest,
    ) -> None:
        command = request.command
        if profile.adapter == "codex":
            expected = [
                profile.executable,
                "exec",
                "--json",
                "--ephemeral",
                "--ignore-user-config",
                "--ignore-rules",
                "--dangerously-bypass-approvals-and-sandbox",
                "--color",
                "never",
            ]
            if profile.model is not None:
                expected.extend(("--model", profile.model))
            expected.append("-")
            if command != tuple(expected):
                raise CodingIsolationError("isolated Codex argv is not canonical")
            return
        try:
            prompt = request.input_bytes.decode("utf-8", "strict")
        except UnicodeDecodeError as error:
            raise CodingIsolationError("isolated OpenCode prompt must be UTF-8") from error
        expected = [profile.executable, "run", "--format", "json"]
        if profile.model is not None:
            expected.extend(("--model", profile.model))
        expected.append(prompt)
        if command != tuple(expected):
            raise CodingIsolationError("isolated OpenCode argv is not canonical")

    def _policy_check(
        self,
        executable: str,
        *,
        sandbox: str | None,
        target: str,
        expected: bool,
    ) -> None:
        arguments = ["policy", "check", "network"]
        if sandbox is not None:
            arguments.extend(("--sandbox", sandbox))
        arguments.extend(("--json", target))
        result = self._checked(executable, tuple(arguments))
        _decode_policy_check(result.stdout, target=target, expected=expected)

    def _configure_sandbox_policy(
        self,
        identity: _RuntimeIdentity,
        sandbox: str,
        hosts: Sequence[str],
    ) -> None:
        for host in hosts:
            self._checked(
                identity.executable_path,
                ("policy", "allow", "network", "--sandbox", sandbox, host),
            )
        policy = self._read_sandbox_policy(identity.executable_path, sandbox)
        _require_exact_sandbox_policy(
            identity.global_policy,
            policy,
            sandbox=sandbox,
            allowed_hosts=hosts,
        )
        for host in hosts:
            self._policy_check(
                identity.executable_path,
                sandbox=sandbox,
                target=host,
                expected=True,
            )
        self._policy_check(
            identity.executable_path,
            sandbox=sandbox,
            target="oracle-lab-deny-probe.invalid",
            expected=False,
        )

    def _require_sandbox_name_available(
        self,
        identity: _RuntimeIdentity,
        *,
        sandbox: str,
    ) -> None:
        if sandbox in self._read_names(identity.executable_path):
            raise CodingIsolationError("generated sbx sandbox name already exists")

    def _create_sandbox(
        self,
        identity: _RuntimeIdentity,
        *,
        sandbox: str,
        carrier: Path,
    ) -> None:
        self._checked(
            identity.executable_path,
            (
                "create",
                "shell",
                str(carrier),
                "--clone",
                "--name",
                sandbox,
                "--template",
                identity.template_reference,
                "--no-share-skills",
            ),
            timeout_seconds=_CREATE_TIMEOUT_SECONDS,
        )
        if sandbox not in self._read_names(identity.executable_path):
            raise CodingIsolationError("created sbx sandbox is absent from sbx ls")

    def _verify_post_policy(
        self,
        identity: _RuntimeIdentity,
        *,
        sandbox: str,
        hosts: Sequence[str],
    ) -> None:
        policy = self._read_sandbox_policy(identity.executable_path, sandbox)
        _require_exact_sandbox_policy(
            identity.global_policy,
            policy,
            sandbox=sandbox,
            allowed_hosts=hosts,
        )
        log = self._checked(
            identity.executable_path,
            ("policy", "log", sandbox, "--type", "network", "--json"),
        )
        _decode_policy_log(log.stdout, sandbox=sandbox, allowed_hosts=hosts)

    def _cleanup_and_confirm(
        self,
        identity: _RuntimeIdentity,
        *,
        sandbox: str,
    ) -> None:
        # Removal is attempted even after partial create failures.  A non-zero
        # remove is acceptable only when the independent list confirms absence.
        self._run(
            identity.executable_path,
            ("rm", "--force", sandbox),
            timeout_seconds=_CONTROL_TIMEOUT_SECONDS,
        )
        if sandbox in self._read_names(identity.executable_path):
            raise CodingIsolationError("sbx sandbox cleanup could not be confirmed")
        current = self._read_global_policy(identity.executable_path)
        current_sha256 = hashlib.sha256(
            canonical_json(_policy_document(current)).encode("utf-8")
        ).hexdigest()
        if current_sha256 != identity.global_policy_sha256:
            raise CodingIsolationError("global sbx network policy changed during sandbox lifecycle")

    @staticmethod
    def _initialize_repository(repository: Path) -> None:
        try:
            require_git(repository, "init", "--initial-branch=oracle-lab")
            require_git(repository, "add", "--all")
            require_git(
                repository,
                "-c",
                "user.name=Oracle Lab",
                "-c",
                "user.email=oracle-lab.invalid",
                "commit",
                "--allow-empty",
                "-m",
                "isolation carrier",
            )
        except GitControlError as error:
            raise CodingIsolationError(
                "isolation carrier Git repository could not be created"
            ) from error

    @staticmethod
    def _copy_carrier(source: Path, destination: Path) -> None:
        try:
            details = source.lstat()
            if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                raise CodingIsolationError("isolated workspace must be a real directory")
            shutil.copytree(source, destination, symlinks=True, copy_function=shutil.copy2)
        except OSError as error:
            raise CodingIsolationError("isolated workspace carrier could not be created") from error
        git_directory = destination / ".git"
        if not git_directory.is_dir() or git_directory.is_symlink():
            DockerSbxIsolationBroker._initialize_repository(destination)

    def _export_guest_workspace(
        self,
        identity: _RuntimeIdentity,
        *,
        sandbox: str,
        carrier: Path,
        limits: WorkspaceArchiveLimits,
    ) -> Any:
        result = self._checked(
            identity.executable_path,
            (
                "exec",
                "-i",
                "-w",
                str(carrier),
                sandbox,
                "/usr/bin/python3",
                "-c",
                _GUEST_WORKSPACE_EXPORT_SCRIPT,
                str(limits.max_raw_bytes),
                str(limits.max_entries),
            ),
            timeout_seconds=_CONTROL_TIMEOUT_SECONDS,
            max_output_bytes=limits.max_raw_bytes,
        )
        try:
            return validate_workspace_archive(result.stdout, limits)
        except WorkspaceArchiveError as error:
            raise CodingIsolationError("guest workspace export failed validation") from error

    def _run_conformance(
        self,
        profile: _BoundProfile,
        identity: _RuntimeIdentity,
        *,
        hosts: Sequence[str],
    ) -> Mapping[str, Any]:
        state_root = self._state_directory()
        sandbox = f"oracle-lab-probe-{uuid.uuid4().hex}"
        cleanup_error: BaseException | None = None
        primary_error: BaseException | None = None
        probe_observation: dict[str, Any] | None = None
        export_observation: dict[str, Any] | None = None
        source_git_observation: dict[str, str] | None = None
        owns_sandbox = False
        sentinel = uuid.uuid4().hex
        try:
            with tempfile.TemporaryDirectory(prefix="conformance-", dir=state_root) as raw_root:
                root = Path(raw_root)
                source = root / "trusted-source"
                source.mkdir()
                (source / "source-marker.txt").write_bytes(b"host-source\n")
                self._initialize_repository(source)
                source_git_before = fingerprint_git_control(source / ".git")
                carrier = root / "carrier"
                self._copy_carrier(source, carrier)
                host_marker = root / "host-only-marker.txt"
                host_marker.write_bytes(b"must-not-be-visible\n")
                carrier_git_before = fingerprint_git_control(carrier / ".git")
                source_bytes_before = (carrier / "source-marker.txt").read_bytes()
                self._require_sandbox_name_available(identity, sandbox=sandbox)
                self._create_sandbox(identity, sandbox=sandbox, carrier=carrier)
                owns_sandbox = True
                self._configure_sandbox_policy(identity, sandbox, hosts)
                environment = self._environment(probe_sentinel=sentinel)
                probe = self._checked(
                    identity.executable_path,
                    (
                        "exec",
                        "-i",
                        "-w",
                        str(carrier),
                        sandbox,
                        "/usr/bin/python3",
                        "-c",
                        _GUEST_CONFORMANCE_PROBE_SCRIPT,
                        str(carrier),
                        str(host_marker),
                        os.environ.get("HOME", "/oracle-lab-host-home-unset"),
                        sentinel,
                    ),
                    environment=environment,
                    max_output_bytes=_PROBE_MAX_OUTPUT_BYTES,
                )
                probe_observation = _decode_conformance_probe(probe.stdout)
                if (carrier / "source-marker.txt").read_bytes() != source_bytes_before:
                    raise CodingIsolationError("private clone mutated the Host carrier worktree")
                if (carrier / "guest-private-marker.txt").exists():
                    raise CodingIsolationError("private clone write appeared on the Host carrier")
                source_git_after = fingerprint_git_control(source / ".git")
                if source_git_after != source_git_before:
                    raise CodingIsolationError("sandbox mutated trusted Host Git control data")
                source_git_observation = {
                    "before": source_git_before,
                    "after": source_git_after,
                }
                # sbx --clone is allowed to add its own remote to the disposable
                # carrier.  The trusted source above, never passed to sbx, is the
                # Git control plane whose immutability matters.
                if not carrier_git_before:
                    raise CodingIsolationError("disposable carrier Git fingerprint is unavailable")
                limits = WorkspaceArchiveLimits(
                    max_raw_bytes=1024 * 1024,
                    max_entries=1024,
                    max_regular_payload_bytes=1024 * 1024,
                )
                exported = self._export_guest_workspace(
                    identity,
                    sandbox=sandbox,
                    carrier=carrier,
                    limits=limits,
                )
                export_observation = {
                    "sha256": exported.sha256,
                    "size_bytes": exported.size_bytes,
                    "entry_count": exported.entry_count,
                }
                self._verify_post_policy(identity, sandbox=sandbox, hosts=hosts)
        except BaseException as error:
            primary_error = error
        finally:
            if owns_sandbox:
                try:
                    self._cleanup_and_confirm(identity, sandbox=sandbox)
                except BaseException as error:
                    cleanup_error = error
        if cleanup_error is not None:
            raise CodingIsolationError(
                "sbx conformance cleanup was not confirmed"
            ) from cleanup_error
        if primary_error is not None:
            if isinstance(primary_error, CodingIsolationError):
                raise primary_error
            raise CodingIsolationError("sbx conformance probe failed") from primary_error
        if (
            probe_observation is None
            or export_observation is None
            or source_git_observation is None
        ):
            raise CodingIsolationError("sbx conformance evidence is incomplete")

        observed = self._clock()
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise CodingIsolationError("isolation conformance clock must be timezone-aware")
        evidence_by_capability: dict[str, Mapping[str, Any]] = {
            "microvm_or_equivalent_os_boundary": {
                "json_schema": identity.json_schema,
                "linux_guest": probe_observation["linux_guest"],
            },
            "host_filesystem_unavailable": {
                "host_marker_visible": probe_observation["host_marker_visible"],
                "host_home_visible": probe_observation["host_home_visible"],
            },
            "host_git_control_unavailable": source_git_observation,
            "workspace_source_read_only": {
                "host_carrier_source_unchanged": True,
            },
            "workspace_changes_private_until_export": {
                "guest_write_complete": probe_observation["guest_write_complete"],
                "guest_marker_visible_on_host_carrier": False,
            },
            "network_default_deny": {
                "probe_target": "oracle-lab-deny-probe.invalid",
                "decision": "implicit-deny",
            },
            "network_exact_allowlist": {
                "allowed_hosts": list(hosts),
                "global_policy_sha256": identity.global_policy_sha256,
            },
            "credential_proxy_values_unavailable": {
                "credential_sentinel_visible": probe_observation["credential_sentinel_visible"],
                "unsafe_credential_environment": probe_observation["unsafe_credential_environment"],
            },
            "host_processes_unavailable": {
                "host_process_sentinel_visible": probe_observation["host_process_sentinel_visible"],
            },
            "host_docker_unavailable": {
                "host_docker_endpoint_visible": probe_observation["host_docker_endpoint_visible"],
                "sandbox_private_docker_socket_visible": probe_observation[
                    "sandbox_docker_socket_visible"
                ],
            },
            "all_descendants_confined": {
                "detached_descendant_pid": probe_observation["detached_descendant_pid"],
                "guest_boundary": "microvm",
            },
            "all_descendants_destroyed_on_cleanup": {
                "sandbox": sandbox,
                "cleanup_confirmed_absent": True,
            },
            "shared_agent_state_disabled": {
                "no_share_skills_flag": True,
                "shared_agent_state_mount_visible": probe_observation[
                    "shared_agent_state_mount_visible"
                ],
                "ssh_auth_sock_visible": probe_observation["ssh_auth_sock_visible"],
            },
            "bounded_workspace_export": export_observation,
        }
        evidence_by_capability = {
            capability: {
                **dict(evidence),
                "evidence_origin": _SYNTHETIC_EVIDENCE_ORIGIN,
                "bound_profile_sha256": hashlib.sha256(
                    canonical_json(profile.to_dict()).encode("utf-8")
                ).hexdigest(),
            }
            for capability, evidence in evidence_by_capability.items()
        }
        checks = [
            {
                "id": capability,
                "status": "passed",
                "evidence": evidence_by_capability[capability],
            }
            for capability in sorted(REQUIRED_ISOLATION_CAPABILITIES)
        ]
        return {
            "schema_version": 1,
            "status": "passed",
            "evidence_origin": _SYNTHETIC_EVIDENCE_ORIGIN,
            "observed_at": observed.astimezone(UTC).isoformat(),
            "checks": checks,
        }

    def bind(self, profile: Any) -> CodingWorkerIsolationBinding:
        if (
            not self._synthetic_fixture
            or isinstance(self._runner, SubprocessCommandRunner)
            or getattr(self._runner, "evidence_origin", None) != _SYNTHETIC_EVIDENCE_ORIGIN
        ):
            raise CodingIsolationError(
                "production Docker sbx attestation is unavailable: "
                + ", ".join(PRODUCTION_ISOLATION_EVIDENCE_BLOCKERS)
            )
        workspace_root, state_root = self._workspace_boundary()
        hosts = self._profile_hosts(profile)
        environment_names = self._profile_environment_names(profile)
        bound_profile = self._bound_profile(
            profile,
            hosts=hosts,
            environment_names=environment_names,
        )
        identity = self._measure_runtime_identity(profile)
        self._policy_check(
            identity.executable_path,
            sandbox=None,
            target="oracle-lab-deny-probe.invalid",
            expected=False,
        )
        receipt = self._run_conformance(bound_profile, identity, hosts=hosts)
        policy_identity = {
            "global_policy": _policy_document(identity.global_policy),
            "allowed_hosts": list(hosts),
            "bound_profile": bound_profile.to_dict(),
            "evidence_origin": _SYNTHETIC_EVIDENCE_ORIGIN,
        }
        policy_sha256 = hashlib.sha256(canonical_json(policy_identity).encode("utf-8")).hexdigest()
        attestation = IsolationAttestation(
            backend=_SYNTHETIC_BACKEND_ID,
            broker_executable_path=identity.executable_path,
            broker_executable_sha256=identity.executable_sha256,
            client_version=identity.version.client_text,
            server_version=identity.version.server_text,
            template_reference=identity.template_reference,
            template_identity=identity.template_identity,
            policy_sha256=policy_sha256,
            conformance_suite_version=(
                f"{_CONFORMANCE_SUITE_VERSION}-{_SYNTHETIC_EVIDENCE_ORIGIN}"
            ),
            conformance_receipt_sha256=receipt_sha256(receipt),
            capabilities=tuple(REQUIRED_ISOLATION_CAPABILITIES),
            receipt=receipt,
        )
        return _DockerSbxBinding(
            broker=self,
            profile=bound_profile,
            workspace_root=workspace_root,
            state_root=state_root,
            identity=identity,
            attestation=attestation,
        )

    def _require_identity(self, identity: _RuntimeIdentity) -> None:
        path, digest = _sha256_regular_executable(identity.executable_path)
        if path != identity.executable_path or digest != identity.executable_sha256:
            raise CodingIsolationError("sbx isolation broker executable identity drifted")
        version = self._read_version(path)
        if version != identity.version or _json_schema_for(version) != identity.json_schema:
            raise CodingIsolationError("sbx client or server identity drifted")
        if (
            self._read_template_identity(path, identity.template_reference)
            != identity.template_identity
        ):
            raise CodingIsolationError("sbx isolation template identity drifted")
        current = self._read_global_policy(path)
        current_sha256 = hashlib.sha256(
            canonical_json(_policy_document(current)).encode("utf-8")
        ).hexdigest()
        if current_sha256 != identity.global_policy_sha256:
            raise CodingIsolationError("global sbx network policy drifted")


@dataclass(frozen=True, slots=True)
class _DockerSbxBinding(CodingWorkerIsolationBinding):
    broker: DockerSbxIsolationBroker
    profile: _BoundProfile
    workspace_root: Path
    state_root: Path
    identity: _RuntimeIdentity
    attestation: IsolationAttestation

    def run(self, request: IsolationRunRequest) -> IsolationRunResult:
        if request.adapter != self.profile.adapter:
            raise CodingIsolationError("isolated run adapter differs from the bound profile")
        if (
            request.timeout_seconds != self.profile.timeout_seconds
            or request.max_output_bytes != self.profile.max_output_bytes
            or request.max_workspace_export_bytes != self.profile.max_workspace_export_bytes
            or request.max_workspace_entries != self.profile.max_workspace_entries
        ):
            raise CodingIsolationError("isolated run limits differ from the bound profile")
        if set(request.environment) - set(self.profile.environment_names):
            raise CodingIsolationError("isolated run environment differs from the bound profile")
        if any(
            not isinstance(value, str) or "\x00" in value or len(value) > 4096
            for value in request.environment.values()
        ):
            raise CodingIsolationError("isolated run environment value is invalid")
        self.broker._require_canonical_command(self.profile, request)
        try:
            workspace_details = request.workspace.lstat()
            workspace = request.workspace.resolve(strict=True)
        except OSError as error:
            raise CodingIsolationError("isolated run workspace is unavailable") from error
        if stat.S_ISLNK(workspace_details.st_mode) or not stat.S_ISDIR(workspace_details.st_mode):
            raise CodingIsolationError("isolated run workspace must be a real directory")
        if workspace == self.workspace_root or not workspace.is_relative_to(self.workspace_root):
            raise CodingIsolationError("isolated run workspace is outside the bound workspace_root")
        if workspace == self.state_root or workspace.is_relative_to(self.state_root):
            raise CodingIsolationError("isolated run workspace overlaps isolation state")
        self.broker._require_identity(self.identity)
        limits = WorkspaceArchiveLimits(
            max_raw_bytes=request.max_workspace_export_bytes,
            max_entries=request.max_workspace_entries,
            max_regular_payload_bytes=request.max_workspace_export_bytes,
        )
        state_root = self.broker._state_directory()
        sandbox = f"oracle-lab-run-{uuid.uuid4().hex}"
        worker: CommandResult | None = None
        worker_failed = False
        exported: Any = None
        primary_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        owns_sandbox = False
        try:
            with tempfile.TemporaryDirectory(prefix="run-", dir=state_root) as raw_root:
                root = Path(raw_root)
                carrier = root / "carrier"
                self.broker._copy_carrier(workspace, carrier)
                self.broker._require_sandbox_name_available(
                    self.identity,
                    sandbox=sandbox,
                )
                self.broker._create_sandbox(self.identity, sandbox=sandbox, carrier=carrier)
                owns_sandbox = True
                self.broker._configure_sandbox_policy(
                    self.identity,
                    sandbox,
                    self.profile.allowed_hosts,
                )
                env_arguments: list[str] = []
                for name in sorted(request.environment):
                    env_arguments.extend(("-e", f"{name}={request.environment[name]}"))
                arguments = (
                    "exec",
                    "-i",
                    "-w",
                    str(carrier),
                    *env_arguments,
                    sandbox,
                    *request.command,
                )
                worker = self.broker._run(
                    self.identity.executable_path,
                    arguments,
                    input_bytes=request.input_bytes,
                    timeout_seconds=request.timeout_seconds,
                    max_output_bytes=request.max_output_bytes,
                )
                if worker.timed_out or worker.output_limited or worker.exit_code != 0:
                    worker_failed = True
                    self.broker._verify_post_policy(
                        self.identity,
                        sandbox=sandbox,
                        hosts=self.profile.allowed_hosts,
                    )
                else:
                    exported = self.broker._export_guest_workspace(
                        self.identity,
                        sandbox=sandbox,
                        carrier=carrier,
                        limits=limits,
                    )
                    self.broker._verify_post_policy(
                        self.identity,
                        sandbox=sandbox,
                        hosts=self.profile.allowed_hosts,
                    )
        except BaseException as error:
            primary_error = error
        finally:
            if owns_sandbox:
                try:
                    self.broker._cleanup_and_confirm(self.identity, sandbox=sandbox)
                except BaseException as error:
                    cleanup_error = error
        if cleanup_error is not None:
            raise CodingIsolationError(
                "isolated worker cleanup was not confirmed"
            ) from cleanup_error
        if owns_sandbox:
            try:
                self.broker._require_identity(self.identity)
            except CodingIsolationError as error:
                raise CodingIsolationError(
                    "isolated worker runtime identity drifted during execution"
                ) from error
        if primary_error is not None:
            if isinstance(primary_error, CodingIsolationError):
                raise primary_error
            raise CodingIsolationError("isolated worker lifecycle failed") from primary_error
        if worker_failed:
            if worker is None:
                raise CodingIsolationError("isolated worker failure evidence is incomplete")
            raise IsolationRunFailed(
                exit_code=worker.exit_code,
                stdout=worker.stdout,
                stderr=worker.stderr,
                timed_out=worker.timed_out,
                output_limited=worker.output_limited,
                actual_command=request.command,
                guest_executable_path=None,
                guest_executable_version=None,
                guest_executable_version_status="unknown",
                sandbox_id=sandbox,
                cleanup_confirmed=True,
                attestation=self.attestation,
                max_output_bytes=request.max_output_bytes,
            )
        if worker is None or exported is None:
            raise CodingIsolationError("isolated worker result is incomplete")

        return IsolationRunResult(
            exit_code=worker.exit_code,
            stdout=worker.stdout,
            stderr=worker.stderr,
            timed_out=False,
            output_limited=False,
            actual_command=request.command,
            guest_executable_path=None,
            guest_executable_version=None,
            guest_executable_version_status="unknown",
            sandbox_id=sandbox,
            workspace_export=exported.data,
            workspace_export_sha256=exported.sha256,
            workspace_export_bytes=exported.size_bytes,
            workspace_export_entries=exported.entry_count,
            cleanup_confirmed=True,
            attestation=self.attestation,
        )


def build_coding_worker_isolation_broker(
    config: Any,
    *,
    state_root: str | Path,
    workspace_root: str | Path | None = None,
) -> CodingWorkerIsolationBroker | None:
    """Build a side-effect-free broker factory from runtime configuration."""

    backend = getattr(config, "isolation_backend", "disabled")
    if backend == "disabled":
        return None
    if backend == _BACKEND_ID:
        return DockerSbxIsolationBroker(
            executable=str(getattr(config, "isolation_broker_executable", "sbx")),
            state_root=state_root,
            workspace_root=workspace_root,
        )
    raise CodingIsolationError(f"unsupported coding-worker isolation backend: {backend}")


__all__ = [
    "CommandResult",
    "CommandRunner",
    "DockerSbxIsolationBroker",
    "SubprocessCommandRunner",
    "build_coding_worker_isolation_broker",
]
