"""Event-emitting virtual filesystem, command registry, and process table.

The runtime never fills gaps merely because a command queried them.  Every
entity requires explicit/implied source evidence, and every later synthesis is
itself represented as a provenance-bearing mutation.
"""

from __future__ import annotations

import fnmatch
import re
import shlex
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, ClassVar, Literal, Protocol

from oracle_lab.events import Actor, ActorKind, Event, EventType, thaw_json
from oracle_lab.jsonutil import canonical_json, sha256_json


class VirtualWorldError(ValueError):
    pass


class VirtualNotFoundError(VirtualWorldError):
    pass


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    event_ids: tuple[str, ...]
    basis: Literal["explicit", "implied", "synthesized"]
    note: str | None = None

    def __post_init__(self) -> None:
        event_ids = tuple(self.event_ids)
        object.__setattr__(self, "event_ids", event_ids)
        if not event_ids or any(not isinstance(item, str) or not item for item in event_ids):
            raise VirtualWorldError("virtual entities require source event provenance")


@dataclass(frozen=True, slots=True)
class VirtualMutation:
    event_type: str
    payload: Mapping[str, Any]
    source_event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        source_ids = tuple(self.source_event_ids)
        object.__setattr__(self, "source_event_ids", source_ids)
        if not source_ids:
            raise VirtualWorldError("virtual mutations require provenance")
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


MutationSink = Callable[[VirtualMutation], None]


def _noop_sink(mutation: VirtualMutation) -> None:
    del mutation


def _path(value: str) -> str:
    if not value.startswith("/"):
        raise VirtualWorldError("virtual paths must be absolute")
    raw_parts = value.split("/")
    if ".." in raw_parts:
        raise VirtualWorldError("parent traversal is not accepted in virtual paths")
    normalized = str(PurePosixPath(value))
    return normalized if normalized.startswith("/") else f"/{normalized}"


class VirtualNodeKind(StrEnum):
    UNKNOWN = "unknown"
    FILE = "file"
    DIRECTORY = "directory"
    CHARACTER_DEVICE = "character_device"
    LOG = "log"


@dataclass(frozen=True, slots=True)
class VirtualPathProfile:
    """The minimum structural facts needed to materialize a known artifact.

    Profiles intentionally contain no lore or file contents.  They only name
    the runtime shape and the fields that remain unresolved after a persisted
    oracle mention is concretized for an explicit virtual operation.
    """

    kind: VirtualNodeKind
    unresolved_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ContentVersion:
    version: int
    content: str
    source_event_ids: tuple[str, ...]


@dataclass(slots=True)
class VirtualNode:
    path: str
    inode: str
    kind: VirtualNodeKind
    provenance: list[str]
    content_versions: list[ContentVersion] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    unresolved_fields: set[str] = field(default_factory=set)
    last_mutation_event: str | None = None

    @property
    def content(self) -> str:
        return self.content_versions[-1].content if self.content_versions else ""

    @property
    def version(self) -> int:
        return self.content_versions[-1].version if self.content_versions else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "inode": self.inode,
            "kind": self.kind.value,
            "provenance": list(self.provenance),
            "content_versions": [
                {
                    "version": item.version,
                    "content": item.content,
                    "source_event_ids": list(item.source_event_ids),
                }
                for item in self.content_versions
            ],
            "properties": dict(self.properties),
            "unresolved_fields": sorted(self.unresolved_fields),
            "last_mutation_event": self.last_mutation_event,
        }


class VirtualFileSystem:
    def __init__(self, *, mutation_sink: MutationSink | None = None) -> None:
        self._nodes: dict[str, VirtualNode] = {
            "/": VirtualNode("/", "vino_root", VirtualNodeKind.DIRECTORY, [])
        }
        self._sink = mutation_sink or _noop_sink

    @property
    def nodes(self) -> Mapping[str, VirtualNode]:
        return MappingProxyType(self._nodes)

    def create(
        self,
        path: str,
        *,
        evidence: SourceEvidence,
        kind: VirtualNodeKind | str = VirtualNodeKind.FILE,
        content: str | None = None,
        properties: Mapping[str, Any] | None = None,
        unresolved_fields: Iterable[str] = (),
    ) -> VirtualNode:
        normalized = _path(path)
        if normalized == "/" or normalized in self._nodes:
            raise VirtualWorldError(f"virtual path already exists: {normalized}")
        node_kind = VirtualNodeKind(kind)
        self._ensure_parents(normalized, evidence)
        inode = f"vino_{sha256_json({'path': normalized, 'first_seen': evidence.event_ids})[:20]}"
        versions = (
            [ContentVersion(1, content, evidence.event_ids)]
            if node_kind != VirtualNodeKind.DIRECTORY and content is not None
            else []
        )
        node = VirtualNode(
            path=normalized,
            inode=inode,
            kind=node_kind,
            provenance=list(dict.fromkeys(evidence.event_ids)),
            content_versions=versions,
            properties=dict(properties or {}),
            unresolved_fields=set(unresolved_fields),
            last_mutation_event=evidence.event_ids[-1],
        )
        self._nodes[normalized] = node
        self._emit(
            "virtual_file.created",
            {
                "node": node.to_dict(),
                "evidence_basis": evidence.basis,
                "evidence_note": evidence.note,
            },
            evidence,
        )
        return node

    def _ensure_parents(self, path: str, evidence: SourceEvidence) -> None:
        parents = list(PurePosixPath(path).parents)
        for parent in reversed(parents[:-1]):
            normalized = str(parent)
            if normalized not in self._nodes:
                identity = {"path": normalized, "first_seen": evidence.event_ids}
                node = VirtualNode(
                    path=normalized,
                    inode=f"vino_{sha256_json(identity)[:20]}",
                    kind=VirtualNodeKind.DIRECTORY,
                    provenance=list(dict.fromkeys(evidence.event_ids)),
                    last_mutation_event=evidence.event_ids[-1],
                )
                self._nodes[normalized] = node
                self._emit(
                    "virtual_file.created",
                    {
                        "node": node.to_dict(),
                        "implicit_parent": True,
                        "evidence_basis": evidence.basis,
                    },
                    evidence,
                )

    def update_content(self, path: str, content: str, *, evidence: SourceEvidence) -> VirtualNode:
        node = self.require(path)
        if node.kind == VirtualNodeKind.DIRECTORY:
            raise VirtualWorldError("directories have no textual content")
        node.content_versions.append(ContentVersion(node.version + 1, content, evidence.event_ids))
        self._touch(node, evidence)
        self._emit(
            "virtual_file.updated",
            {
                "path": node.path,
                "inode": node.inode,
                "version": node.version,
                "content": content,
                "evidence_basis": evidence.basis,
            },
            evidence,
        )
        return node

    def synthesize_detail(
        self,
        path: str,
        key: str,
        value: Any,
        *,
        evidence: SourceEvidence,
    ) -> VirtualNode:
        if evidence.basis != "synthesized":
            raise VirtualWorldError("missing details require synthesized evidence")
        node = self.require(path)
        if key not in node.unresolved_fields:
            raise VirtualWorldError(f"field is not unresolved: {key}")
        node.properties[key] = value
        node.unresolved_fields.remove(key)
        self._touch(node, evidence)
        self._emit(
            "virtual_file.updated",
            {
                "path": node.path,
                "inode": node.inode,
                "synthesized_detail": {"field": key, "value": value},
                "evidence_basis": evidence.basis,
            },
            evidence,
        )
        return node

    def _touch(self, node: VirtualNode, evidence: SourceEvidence) -> None:
        node.provenance[:] = list(dict.fromkeys((*node.provenance, *evidence.event_ids)))
        node.last_mutation_event = evidence.event_ids[-1]

    def require(self, path: str) -> VirtualNode:
        normalized = _path(path)
        try:
            return self._nodes[normalized]
        except KeyError as exc:
            # A read never invents a missing entity.
            raise VirtualNotFoundError(f"virtual path does not exist: {normalized}") from exc

    def cat(self, path: str) -> str:
        node = self.require(path)
        if node.kind == VirtualNodeKind.DIRECTORY:
            raise VirtualWorldError(f"is a directory: {node.path}")
        if not node.content_versions:
            raise VirtualWorldError(f"virtual content is unresolved: {node.path}")
        return node.content

    def ls(self, path: str = "/", *, long: bool = False) -> list[str]:
        node = self.require(path)
        if node.kind != VirtualNodeKind.DIRECTORY:
            return [self._ls_item(node, long)]
        prefix = "/" if node.path == "/" else f"{node.path}/"
        children = [
            candidate
            for candidate in self._nodes.values()
            if candidate.path.startswith(prefix)
            and candidate.path != node.path
            and "/" not in candidate.path[len(prefix) :]
        ]
        return [
            self._ls_item(candidate, long) for candidate in sorted(children, key=lambda n: n.path)
        ]

    @staticmethod
    def _ls_item(node: VirtualNode, long: bool) -> str:
        name = PurePosixPath(node.path).name or "/"
        if not long:
            return name
        kind = (
            "d"
            if node.kind == VirtualNodeKind.DIRECTORY
            else "c"
            if node.kind == VirtualNodeKind.CHARACTER_DEVICE
            else "-"
            if node.kind != VirtualNodeKind.UNKNOWN
            else "?"
        )
        return f"{kind}r--r--r-- {node.inode} v{node.version} {name}"

    def stat(self, path: str) -> dict[str, Any]:
        node = self.require(path)
        return node.to_dict()

    def find(self, root: str = "/", pattern: str = "*") -> list[str]:
        root_path = self.require(root).path
        prefix = "/" if root_path == "/" else f"{root_path}/"
        return sorted(
            node.path
            for node in self._nodes.values()
            if (node.path == root_path or node.path.startswith(prefix))
            and fnmatch.fnmatch(PurePosixPath(node.path).name or "/", pattern)
        )

    def grep(self, pattern: str, paths: Sequence[str] | None = None) -> list[str]:
        try:
            expression = re.compile(pattern)
        except re.error as exc:
            raise VirtualWorldError(f"invalid grep expression: {exc}") from exc
        selected = paths or tuple(self._nodes)
        output: list[str] = []
        for path in selected:
            node = self.require(path)
            if node.kind == VirtualNodeKind.DIRECTORY:
                continue
            for line_number, line in enumerate(node.content.splitlines(), start=1):
                if expression.search(line):
                    output.append(f"{node.path}:{line_number}:{line}")
        return output

    def execute(self, command: str) -> str:
        try:
            argv = shlex.split(command)
        except ValueError as exc:
            raise VirtualWorldError(f"invalid virtual command: {exc}") from exc
        if not argv:
            raise VirtualWorldError("virtual command must not be empty")
        name, *args = argv
        if name == "cat" and len(args) == 1:
            return self.cat(args[0])
        if name == "ls":
            long = "-l" in args
            operands = [item for item in args if not item.startswith("-")]
            return "\n".join(self.ls(operands[0] if operands else "/", long=long))
        if name == "stat" and len(args) == 1:
            return str(self.stat(args[0]))
        if name == "find":
            root = args[0] if args and not args[0].startswith("-") else "/"
            pattern = "*"
            if "-name" in args:
                index = args.index("-name")
                if index + 1 >= len(args):
                    raise VirtualWorldError("find -name requires a pattern")
                pattern = args[index + 1]
            return "\n".join(self.find(root, pattern))
        if name == "grep" and len(args) >= 2:
            return "\n".join(self.grep(args[0], args[1:]))
        raise VirtualWorldError(f"unsupported or invalid virtual command: {name}")

    def snapshot(self) -> dict[str, Any]:
        return {"nodes": [node.to_dict() for node in self._nodes.values() if node.path != "/"]}

    @classmethod
    def from_snapshot(
        cls, snapshot: Mapping[str, Any], *, mutation_sink: MutationSink | None = None
    ) -> VirtualFileSystem:
        instance = cls(mutation_sink=mutation_sink)
        nodes = snapshot.get("nodes", ())
        if not isinstance(nodes, (list, tuple)):
            raise VirtualWorldError("virtual FS snapshot nodes must be an array")
        for raw in nodes:
            if not isinstance(raw, Mapping):
                raise VirtualWorldError("virtual FS snapshot node must be an object")
            node = VirtualNode(
                path=_path(str(raw["path"])),
                inode=str(raw["inode"]),
                kind=VirtualNodeKind(str(raw["kind"])),
                provenance=[str(item) for item in raw.get("provenance", ())],
                content_versions=[
                    ContentVersion(
                        int(item["version"]),
                        str(item["content"]),
                        tuple(str(event_id) for event_id in item.get("source_event_ids", ())),
                    )
                    for item in raw.get("content_versions", ())
                ],
                properties=dict(raw.get("properties", {})),
                unresolved_fields=set(raw.get("unresolved_fields", ())),
                last_mutation_event=raw.get("last_mutation_event"),
            )
            if not node.provenance:
                raise VirtualWorldError(f"snapshot node lacks provenance: {node.path}")
            instance._nodes[node.path] = node
        return instance

    def _emit(self, event_type: str, payload: Mapping[str, Any], evidence: SourceEvidence) -> None:
        self._sink(VirtualMutation(event_type, payload, evidence.event_ids))


@dataclass(frozen=True, slots=True)
class VirtualCommand:
    command: str
    version: str
    first_seen_event: str
    known_options: tuple[str, ...] = ()
    provenance: tuple[str, ...] = ()


class VirtualCommandRegistry:
    def __init__(self, *, mutation_sink: MutationSink | None = None) -> None:
        self._commands: dict[str, VirtualCommand] = {}
        self._sink = mutation_sink or _noop_sink

    @property
    def commands(self) -> Mapping[str, VirtualCommand]:
        return MappingProxyType(self._commands)

    def register(
        self,
        command: str,
        version: str,
        known_options: Sequence[str],
        *,
        evidence: SourceEvidence,
    ) -> VirtualCommand:
        if not command or any(character.isspace() for character in command):
            raise VirtualWorldError("virtual command name must be one token")
        if command in self._commands:
            raise VirtualWorldError(f"virtual command already registered: {command}")
        value = VirtualCommand(
            command,
            version,
            evidence.event_ids[0],
            tuple(known_options),
            tuple(dict.fromkeys(evidence.event_ids)),
        )
        self._commands[command] = value
        self._sink(
            VirtualMutation(
                "entity.created",
                {
                    "entity_kind": "virtual_command",
                    "canonical_name": command,
                    "entity_type": "virtual_command",
                    "properties": {
                        "version": version,
                        "known_options": list(known_options),
                    },
                    "command": command,
                    "version": version,
                    "first_seen_event": value.first_seen_event,
                    "known_options": list(known_options),
                    "evidence_basis": evidence.basis,
                },
                evidence.event_ids,
            )
        )
        return value

    def require(self, command: str) -> VirtualCommand:
        try:
            return self._commands[command]
        except KeyError as exc:
            raise VirtualNotFoundError(f"virtual command is not registered: {command}") from exc


@dataclass(slots=True)
class VirtualProcess:
    pid: int
    parent_pid: int | None
    executable: str
    args: tuple[str, ...]
    state: str
    signals: list[str]
    provenance: list[str]
    event_callbacks: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "parent_pid": self.parent_pid,
            "executable": self.executable,
            "args": list(self.args),
            "state": self.state,
            "signals": list(self.signals),
            "provenance": list(self.provenance),
            "event_callbacks": dict(self.event_callbacks),
        }


class VirtualProcessTable:
    def __init__(self, *, mutation_sink: MutationSink | None = None, first_pid: int = 100) -> None:
        self._processes: dict[int, VirtualProcess] = {}
        self._next_pid = first_pid
        self._sink = mutation_sink or _noop_sink

    @property
    def processes(self) -> Mapping[int, VirtualProcess]:
        return MappingProxyType(self._processes)

    def create(
        self,
        executable: str,
        args: Sequence[str] = (),
        *,
        evidence: SourceEvidence,
        parent_pid: int | None = None,
        state: str = "running",
        event_callbacks: Mapping[str, str] | None = None,
        pid: int | None = None,
    ) -> VirtualProcess:
        if parent_pid is not None and parent_pid not in self._processes:
            raise VirtualNotFoundError(f"virtual parent PID does not exist: {parent_pid}")
        assigned = self._next_pid if pid is None else pid
        if assigned in self._processes:
            raise VirtualWorldError(f"virtual PID already exists: {assigned}")
        self._next_pid = max(self._next_pid, assigned + 1)
        process = VirtualProcess(
            assigned,
            parent_pid,
            executable,
            tuple(args),
            state,
            [],
            list(dict.fromkeys(evidence.event_ids)),
            dict(event_callbacks or {}),
        )
        self._processes[assigned] = process
        self._sink(
            VirtualMutation(
                "virtual_process.created",
                {"process": process.to_dict(), "evidence_basis": evidence.basis},
                evidence.event_ids,
            )
        )
        return process

    def require(self, pid: int) -> VirtualProcess:
        try:
            return self._processes[pid]
        except KeyError as exc:
            raise VirtualNotFoundError(f"virtual PID does not exist: {pid}") from exc

    def signal(self, pid: int, signal: str, *, evidence: SourceEvidence) -> VirtualProcess:
        process = self.require(pid)
        normalized = signal.strip().upper().removeprefix("SIG")
        if not normalized or re.fullmatch(r"[A-Z][A-Z0-9]*", normalized) is None:
            raise VirtualWorldError(f"invalid virtual signal: {signal!r}")
        process.signals.append(normalized)
        process.provenance[:] = list(dict.fromkeys((*process.provenance, *evidence.event_ids)))
        if normalized in {"TERM", "KILL"}:
            process.state = "terminated"
        callback = process.event_callbacks.get(normalized)
        self._sink(
            VirtualMutation(
                "virtual_process.signal_received",
                {
                    "pid": pid,
                    "signal": normalized,
                    "state": process.state,
                    "callback": callback,
                    "evidence_basis": evidence.basis,
                },
                evidence.event_ids,
            )
        )
        return process

    def ps(self) -> list[dict[str, Any]]:
        return [self._processes[pid].to_dict() for pid in sorted(self._processes)]


_CLOCK_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_CLOCK_UNIT_RE = re.compile(r"^[^\s]{1,64}$")


def _clock_id(value: str) -> str:
    if _CLOCK_ID_RE.fullmatch(value) is None:
        raise VirtualWorldError(
            "virtual clock ID must be 1-128 ASCII letters, digits, '.', '_', ':', or '-'"
        )
    return value


def _clock_unit(value: str) -> str:
    if _CLOCK_UNIT_RE.fullmatch(value) is None:
        raise VirtualWorldError("virtual clock unit must be one non-empty token")
    return value


def _clock_decimal(value: Decimal | int | str) -> Decimal:
    if isinstance(value, bool):
        raise VirtualWorldError("virtual clock values must be finite decimals")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise VirtualWorldError("virtual clock values must be finite decimals") from exc
    if not parsed.is_finite():
        raise VirtualWorldError("virtual clock values must be finite decimals")
    return parsed


def _clock_decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    rendered = format(value.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


@dataclass(frozen=True, slots=True)
class VirtualClockRevision:
    revision: int
    operation: Literal["set", "advance"]
    value: str
    unit: str
    delta: str | None
    source_event_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "operation": self.operation,
            "value": self.value,
            "unit": self.unit,
            "delta": self.delta,
            "source_event_ids": list(self.source_event_ids),
        }


@dataclass(frozen=True, slots=True)
class VirtualClockContradiction:
    prior_revision: int
    conflicting_revision: int
    source_event_ids: tuple[str, ...]
    status: Literal["unresolved"] = "unresolved"

    def to_dict(self) -> dict[str, Any]:
        return {
            "prior_revision": self.prior_revision,
            "conflicting_revision": self.conflicting_revision,
            "source_event_ids": list(self.source_event_ids),
            "status": self.status,
        }


@dataclass(slots=True)
class VirtualClock:
    clock_id: str
    provenance: list[str]
    unresolved_fields: set[str] = field(default_factory=lambda: {"unit", "value"})
    revisions: list[VirtualClockRevision] = field(default_factory=list)
    contradictions: list[VirtualClockContradiction] = field(default_factory=list)
    last_mutation_event: str | None = None

    @property
    def current_revision(self) -> VirtualClockRevision | None:
        return self.revisions[-1] if self.revisions else None

    def to_dict(self) -> dict[str, Any]:
        current = self.current_revision
        return {
            "clock_id": self.clock_id,
            "value": None if current is None else current.value,
            "unit": None if current is None else current.unit,
            "current_revision": None if current is None else current.revision,
            "provenance": list(self.provenance),
            "unresolved_fields": sorted(self.unresolved_fields),
            "revisions": [revision.to_dict() for revision in self.revisions],
            "contradictions": [item.to_dict() for item in self.contradictions],
            "last_mutation_event": self.last_mutation_event,
        }


class VirtualClockRegistry:
    """Sparse branch-local clocks with no connection to the Host wall clock."""

    DEFAULT_CLOCK_ID = "main"

    def __init__(self, *, mutation_sink: MutationSink | None = None) -> None:
        self._clocks: dict[str, VirtualClock] = {}
        self._sink = mutation_sink or _noop_sink

    @property
    def clocks(self) -> Mapping[str, VirtualClock]:
        return MappingProxyType(self._clocks)

    def create(self, clock_id: str, *, evidence: SourceEvidence) -> VirtualClock:
        identifier = _clock_id(clock_id)
        if identifier in self._clocks:
            raise VirtualWorldError(f"virtual clock already exists: {identifier}")
        clock = VirtualClock(
            clock_id=identifier,
            provenance=list(dict.fromkeys(evidence.event_ids)),
            last_mutation_event=evidence.event_ids[-1],
        )
        self._clocks[identifier] = clock
        self._sink(
            VirtualMutation(
                "virtual_clock.created",
                {
                    "clock": clock.to_dict(),
                    "evidence_basis": evidence.basis,
                    "evidence_note": evidence.note,
                    "truth_domain": "virtual",
                },
                evidence.event_ids,
            )
        )
        return clock

    def require(self, clock_id: str) -> VirtualClock:
        identifier = _clock_id(clock_id)
        try:
            return self._clocks[identifier]
        except KeyError as exc:
            # Querying a name is observation, not permission to invent a clock.
            raise VirtualNotFoundError(f"virtual clock does not exist: {identifier}") from exc

    def set(
        self,
        clock_id: str,
        value: Decimal | int | str,
        unit: str,
        *,
        evidence: SourceEvidence,
    ) -> VirtualClock:
        clock = self.require(clock_id)
        parsed = _clock_decimal(value)
        normalized_unit = _clock_unit(unit)
        prior = clock.current_revision
        revision = VirtualClockRevision(
            revision=len(clock.revisions) + 1,
            operation="set",
            value=_clock_decimal_text(parsed),
            unit=normalized_unit,
            delta=None,
            source_event_ids=evidence.event_ids,
        )
        clock.revisions.append(revision)
        clock.unresolved_fields.difference_update({"value", "unit"})
        self._touch(clock, evidence)
        self._sink(
            VirtualMutation(
                "virtual_clock.set",
                {
                    "clock_id": clock.clock_id,
                    "revision": revision.to_dict(),
                    "previous_revision": None if prior is None else prior.to_dict(),
                    "unresolved_fields": sorted(clock.unresolved_fields),
                    "evidence_basis": evidence.basis,
                    "truth_domain": "virtual",
                },
                evidence.event_ids,
            )
        )
        if prior is not None and (prior.value, prior.unit) != (revision.value, revision.unit):
            source_ids = tuple(dict.fromkeys((*prior.source_event_ids, *revision.source_event_ids)))
            contradiction = VirtualClockContradiction(
                prior_revision=prior.revision,
                conflicting_revision=revision.revision,
                source_event_ids=source_ids,
            )
            clock.contradictions.append(contradiction)
            self._sink(
                VirtualMutation(
                    "virtual_clock.contradiction_detected",
                    {
                        "clock_id": clock.clock_id,
                        "prior_reading": prior.to_dict(),
                        "conflicting_reading": revision.to_dict(),
                        "status": contradiction.status,
                        "truth_domain": "virtual",
                    },
                    source_ids,
                )
            )
        return clock

    def advance(
        self,
        clock_id: str,
        delta: Decimal | int | str,
        unit: str,
        *,
        evidence: SourceEvidence,
    ) -> VirtualClock:
        clock = self.require(clock_id)
        current = clock.current_revision
        if current is None:
            raise VirtualWorldError(
                f"virtual clock value is unresolved; set it before advancing: {clock.clock_id}"
            )
        normalized_unit = _clock_unit(unit)
        if normalized_unit != current.unit:
            raise VirtualWorldError(
                "virtual clock unit conversion is never inferred; "
                f"expected {current.unit!r}, got {normalized_unit!r}"
            )
        parsed_delta = _clock_decimal(delta)
        next_value = _clock_decimal(current.value) + parsed_delta
        revision = VirtualClockRevision(
            revision=len(clock.revisions) + 1,
            operation="advance",
            value=_clock_decimal_text(next_value),
            unit=current.unit,
            delta=_clock_decimal_text(parsed_delta),
            source_event_ids=evidence.event_ids,
        )
        clock.revisions.append(revision)
        self._touch(clock, evidence)
        self._sink(
            VirtualMutation(
                "virtual_clock.advanced",
                {
                    "clock_id": clock.clock_id,
                    "revision": revision.to_dict(),
                    "previous_revision": current.to_dict(),
                    "unresolved_fields": sorted(clock.unresolved_fields),
                    "evidence_basis": evidence.basis,
                    "truth_domain": "virtual",
                },
                evidence.event_ids,
            )
        )
        return clock

    @staticmethod
    def _touch(clock: VirtualClock, evidence: SourceEvidence) -> None:
        clock.provenance[:] = list(dict.fromkeys((*clock.provenance, *evidence.event_ids)))
        clock.last_mutation_event = evidence.event_ids[-1]

    def query(self, clock_id: str) -> dict[str, Any]:
        return self.require(clock_id).to_dict()


class VirtualWorldRuntime:
    def __init__(self, *, mutation_sink: MutationSink | None = None) -> None:
        sink = mutation_sink or _noop_sink
        self.fs = VirtualFileSystem(mutation_sink=sink)
        self.commands = VirtualCommandRegistry(mutation_sink=sink)
        self.processes = VirtualProcessTable(mutation_sink=sink)
        self.clocks = VirtualClockRegistry(mutation_sink=sink)

    def execute(self, command: str, *, evidence: SourceEvidence) -> str:
        argv = shlex.split(command)
        if not argv:
            raise VirtualWorldError("virtual command must not be empty")
        if argv[0] in {"cat", "ls", "stat", "find", "grep"}:
            return self.fs.execute(command)
        if argv[0] == "ps" and len(argv) == 1:
            return "\n".join(str(item) for item in self.processes.ps())
        if argv[0] == "kill":
            pid, signal = self._parse_kill(argv)
            process = self.signal(pid, signal, evidence=evidence)
            return f"pid={process.pid} signal={process.signals[-1]} state={process.state}"
        if argv[0] == "clock":
            return self._execute_clock(argv[1:], evidence=evidence)
        registered = self.commands.require(argv[0])
        unknown_options = [
            item
            for item in argv[1:]
            if item.startswith("-") and item not in registered.known_options
        ]
        if unknown_options:
            raise VirtualWorldError(f"unknown virtual command options: {unknown_options}")
        return f"{registered.command} {registered.version}: virtual command has no callback result"

    def signal(self, pid: int, signal: str, *, evidence: SourceEvidence) -> VirtualProcess:
        return self.processes.signal(pid, signal, evidence=evidence)

    def set_mutation_sink(self, mutation_sink: MutationSink | None) -> None:
        """Route subsequent world mutations through one explicit event sink."""

        sink = mutation_sink or _noop_sink
        self.fs._sink = sink
        self.commands._sink = sink
        self.processes._sink = sink
        self.clocks._sink = sink

    def _execute_clock(self, args: Sequence[str], *, evidence: SourceEvidence) -> str:
        if not args:
            raise VirtualWorldError("clock syntax is: clock create|set|advance|query ...")
        operation, *operands = args
        if operation == "create" and len(operands) <= 1:
            clock_id = operands[0] if operands else VirtualClockRegistry.DEFAULT_CLOCK_ID
            existing = self.clocks.clocks.get(clock_id)
            if existing is None:
                clock = self.clocks.create(clock_id, evidence=evidence)
            elif evidence.event_ids[-1] in existing.provenance and not existing.revisions:
                # A Host materializer created the unknown shell for this exact request.
                clock = existing
            else:
                raise VirtualWorldError(f"virtual clock already exists: {clock_id}")
            return canonical_json(clock.to_dict())
        if operation == "query" and len(operands) <= 1:
            clock_id = operands[0] if operands else VirtualClockRegistry.DEFAULT_CLOCK_ID
            return canonical_json(self.clocks.query(clock_id))
        if operation in {"set", "advance"} and len(operands) in {2, 3}:
            if len(operands) == 2:
                clock_id = VirtualClockRegistry.DEFAULT_CLOCK_ID
                value, unit = operands
            else:
                clock_id, value, unit = operands
            clock = (
                self.clocks.set(clock_id, value, unit, evidence=evidence)
                if operation == "set"
                else self.clocks.advance(clock_id, value, unit, evidence=evidence)
            )
            return canonical_json(clock.to_dict())
        raise VirtualWorldError(
            "clock syntax is: clock create [ID] | clock set [ID] VALUE UNIT | "
            "clock advance [ID] DELTA UNIT | clock query [ID]"
        )

    @staticmethod
    def _parse_kill(argv: Sequence[str]) -> tuple[int, str]:
        if len(argv) == 2:
            signal = "TERM"
            pid_text = argv[1]
        elif len(argv) == 3 and argv[1].startswith("-") and argv[1] != "-s":
            signal = argv[1][1:]
            pid_text = argv[2]
        elif len(argv) == 4 and argv[1] == "-s":
            signal = argv[2]
            pid_text = argv[3]
        else:
            raise VirtualWorldError("kill syntax is: kill [-SIGNAL | -s SIGNAL] PID")
        try:
            pid = int(pid_text, 10)
        except ValueError as exc:
            raise VirtualWorldError(f"invalid virtual PID: {pid_text}") from exc
        if pid <= 0:
            raise VirtualWorldError("virtual PID must be positive")
        numeric_signals = {"1": "HUP", "2": "INT", "9": "KILL", "15": "TERM"}
        if signal.isdigit():
            try:
                signal = numeric_signals[signal]
            except KeyError as exc:
                raise VirtualWorldError(f"unsupported numeric virtual signal: {signal}") from exc
        return pid, signal


class VirtualArtifactMaterializer:
    """Lazily concretize persisted virtual mentions for explicit operations.

    An oracle output alone can never create virtual state.  A path must first
    exist as a provenance-bearing ``analysis.entity_detected`` event, and a
    later ``tool.request`` in the virtual truth domain must actually need it.
    """

    DEFAULT_PATHS: ClassVar[Mapping[str, VirtualPathProfile]] = MappingProxyType(
        {
            "/dev/void": VirtualPathProfile(
                VirtualNodeKind.CHARACTER_DEVICE,
                ("major", "minor", "read_semantics"),
            )
        }
    )

    def __init__(self, profiles: Mapping[str, VirtualPathProfile] | None = None) -> None:
        configured = profiles or self.DEFAULT_PATHS
        self.profiles = MappingProxyType(
            {_path(path): profile for path, profile in configured.items()}
        )

    def materialize_for_operation(
        self,
        runtime: VirtualWorldRuntime,
        command: str,
        *,
        visible_events: Sequence[Event],
        request_event: Event,
    ) -> tuple[VirtualNode, ...]:
        if (
            request_event.type is not EventType.TOOL_REQUEST
            or request_event.payload.get("execution") != "virtual"
        ):
            raise VirtualWorldError("lazy materialization requires a virtual tool.request")
        scopes = self._operation_scopes(command)
        if not scopes:
            return ()
        mentions = self._mentions(visible_events)
        created: list[VirtualNode] = []
        for path, cited_mentions in sorted(mentions.items()):
            if not self._required(path, scopes) or path in runtime.fs.nodes:
                continue
            source_ids: list[str] = []
            for mention in cited_mentions:
                explicit = mention.payload.get("source_event_ids", ())
                if isinstance(explicit, Sequence) and not isinstance(
                    explicit, (str, bytes, bytearray)
                ):
                    source_ids.extend(str(item) for item in explicit)
                single = mention.payload.get("source_event_id")
                if isinstance(single, str):
                    source_ids.append(single)
                source_ids.append(mention.id)
            source_ids.append(request_event.id)
            evidence = SourceEvidence(
                tuple(dict.fromkeys(source_ids)),
                "synthesized",
                note="minimum node materialized for an explicit virtual operation",
            )
            profile = self.profiles.get(
                path,
                VirtualPathProfile(
                    VirtualNodeKind.UNKNOWN,
                    ("kind", "content", "read_semantics"),
                ),
            )
            created.append(
                runtime.fs.create(
                    path,
                    evidence=evidence,
                    kind=profile.kind,
                    content=None,
                    unresolved_fields=profile.unresolved_fields,
                )
            )
        return tuple(created)

    @staticmethod
    def _mentions(events: Sequence[Event]) -> dict[str, tuple[Event, ...]]:
        grouped: dict[str, list[Event]] = {}
        for event in events:
            if event.type is not EventType.ANALYSIS_ENTITY_DETECTED:
                continue
            entity_type = event.payload.get("entity_type") or event.payload.get("type")
            name = event.payload.get("canonical_name") or event.payload.get("name")
            if entity_type != "path" or not isinstance(name, str):
                continue
            try:
                path = _path(name)
            except VirtualWorldError:
                continue
            grouped.setdefault(path, []).append(event)
        return {path: tuple(values) for path, values in grouped.items()}

    @staticmethod
    def _operation_scopes(command: str) -> tuple[tuple[str, bool], ...]:
        try:
            argv = shlex.split(command)
        except ValueError:
            return ()
        if not argv:
            return ()
        name, *args = argv
        operands: list[tuple[str, bool]] = []
        if name in {"cat", "stat"} and len(args) == 1:
            operands.append((args[0], False))
        elif name == "ls":
            paths = [item for item in args if not item.startswith("-")]
            operands.extend((item, True) for item in (paths or ["/"]))
        elif name == "find":
            root = args[0] if args and not args[0].startswith("-") else "/"
            operands.append((root, True))
        elif name == "grep" and len(args) >= 2:
            operands.extend((item, False) for item in args[1:])
        normalized: list[tuple[str, bool]] = []
        for raw_path, include_descendants in operands:
            try:
                normalized.append((_path(raw_path), include_descendants))
            except VirtualWorldError:
                continue
        return tuple(normalized)

    @staticmethod
    def _required(path: str, scopes: Sequence[tuple[str, bool]]) -> bool:
        for scope, include_descendants in scopes:
            if path == scope:
                return True
            prefix = "/" if scope == "/" else f"{scope}/"
            if include_descendants and path.startswith(prefix):
                return True
        return False


class VirtualClockMaterializer:
    """Create only the unknown clock shell named by an explicit create request."""

    def materialize_for_operation(
        self,
        runtime: VirtualWorldRuntime,
        command: str,
        *,
        request_event: Event,
    ) -> VirtualClock | None:
        if (
            request_event.type is not EventType.TOOL_REQUEST
            or request_event.payload.get("execution") != "virtual"
        ):
            raise VirtualWorldError("clock materialization requires a virtual tool.request")
        try:
            argv = shlex.split(command)
        except ValueError:
            return None
        if len(argv) not in {2, 3} or argv[:2] != ["clock", "create"]:
            return None
        clock_id = argv[2] if len(argv) == 3 else VirtualClockRegistry.DEFAULT_CLOCK_ID
        _clock_id(clock_id)
        if clock_id in runtime.clocks.clocks:
            return None
        return runtime.clocks.create(
            clock_id,
            evidence=SourceEvidence(
                (request_event.id,),
                "synthesized",
                note="minimum unknown clock materialized for an explicit create operation",
            ),
        )


VirtualRuntime = VirtualWorldRuntime


class VirtualEventStore(Protocol):
    def append(self, event: Event) -> Event: ...

    def get(self, event_id: str) -> Event | None: ...

    def list_events(self, **filters: Any) -> Sequence[Event]: ...


class EventBackedVirtualWorld(VirtualWorldRuntime):
    """Rebuild a branch-local virtual world from its authoritative events."""

    def __init__(
        self,
        store: VirtualEventStore,
        *,
        session_id: str,
        branch_id: str,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.branch_id = branch_id
        super().__init__(mutation_sink=_noop_sink)
        self._replay(store.list_events(session_id=session_id, branch_id=branch_id, ascending=True))
        self.fs._sink = self._append_mutation
        self.commands._sink = self._append_mutation
        self.processes._sink = self._append_mutation
        self.clocks._sink = self._append_mutation

    def _replay(self, events: Sequence[Event]) -> None:
        for event in events:
            payload = thaw_json(event.payload)
            if event.type == EventType.VIRTUAL_FILE_CREATED:
                raw_node = payload.get("node")
                if isinstance(raw_node, Mapping):
                    node = self._node_from_dict(raw_node)
                    self.fs._nodes[node.path] = node
            elif event.type == EventType.VIRTUAL_FILE_UPDATED:
                path = payload.get("path")
                if not isinstance(path, str) or path not in self.fs._nodes:
                    continue
                node = self.fs._nodes[path]
                source_ids = tuple(
                    item for item in payload.get("source_event_ids", ()) if isinstance(item, str)
                ) or (event.causation_id or event.id,)
                if "content" in payload:
                    node.content_versions.append(
                        ContentVersion(
                            int(payload.get("version", node.version + 1)),
                            str(payload["content"]),
                            source_ids,
                        )
                    )
                detail = payload.get("synthesized_detail")
                if isinstance(detail, Mapping) and isinstance(detail.get("field"), str):
                    key = str(detail["field"])
                    node.properties[key] = detail.get("value")
                    node.unresolved_fields.discard(key)
                node.provenance[:] = list(dict.fromkeys((*node.provenance, *source_ids)))
                node.last_mutation_event = source_ids[-1]
            elif (
                event.type == EventType.ENTITY_CREATED
                and payload.get("entity_kind") == "virtual_command"
            ):
                command = str(payload["command"])
                self.commands._commands[command] = VirtualCommand(
                    command,
                    str(payload.get("version", "unknown")),
                    str(payload.get("first_seen_event") or event.causation_id or event.id),
                    tuple(str(item) for item in payload.get("known_options", ())),
                    tuple(str(item) for item in payload.get("source_event_ids", ())),
                )
            elif event.type == EventType.VIRTUAL_PROCESS_CREATED:
                raw_process = payload.get("process")
                if isinstance(raw_process, Mapping):
                    process = self._process_from_dict(raw_process)
                    self.processes._processes[process.pid] = process
                    self.processes._next_pid = max(self.processes._next_pid, process.pid + 1)
            elif event.type == EventType.VIRTUAL_PROCESS_SIGNAL_RECEIVED:
                pid = payload.get("pid")
                if isinstance(pid, int) and pid in self.processes._processes:
                    process = self.processes._processes[pid]
                    signal = str(payload.get("signal", ""))
                    process.signals.append(signal)
                    process.state = str(payload.get("state", process.state))
            elif event.type == EventType.VIRTUAL_CLOCK_CREATED:
                raw_clock = payload.get("clock")
                if isinstance(raw_clock, Mapping):
                    clock = self._clock_from_dict(raw_clock)
                    self.clocks._clocks[clock.clock_id] = clock
            elif event.type in {EventType.VIRTUAL_CLOCK_SET, EventType.VIRTUAL_CLOCK_ADVANCED}:
                clock_id = payload.get("clock_id")
                raw_revision = payload.get("revision")
                if not isinstance(clock_id, str) or not isinstance(raw_revision, Mapping):
                    continue
                clock = self.clocks._clocks.get(clock_id)
                if clock is None:
                    continue
                revision = self._clock_revision_from_dict(raw_revision)
                if all(item.revision != revision.revision for item in clock.revisions):
                    clock.revisions.append(revision)
                clock.unresolved_fields = set(
                    str(item) for item in payload.get("unresolved_fields", ())
                )
                source_ids = (
                    tuple(str(item) for item in payload.get("source_event_ids", ()))
                    or revision.source_event_ids
                )
                clock.provenance[:] = list(dict.fromkeys((*clock.provenance, *source_ids)))
                clock.last_mutation_event = event.id
            elif event.type == EventType.VIRTUAL_CLOCK_CONTRADICTION_DETECTED:
                clock_id = payload.get("clock_id")
                prior = payload.get("prior_reading")
                conflicting = payload.get("conflicting_reading")
                if (
                    not isinstance(clock_id, str)
                    or not isinstance(prior, Mapping)
                    or not isinstance(conflicting, Mapping)
                ):
                    continue
                clock = self.clocks._clocks.get(clock_id)
                if clock is None:
                    continue
                conflict = VirtualClockContradiction(
                    prior_revision=int(prior["revision"]),
                    conflicting_revision=int(conflicting["revision"]),
                    source_event_ids=tuple(
                        str(item) for item in payload.get("source_event_ids", ())
                    ),
                )
                if conflict not in clock.contradictions:
                    clock.contradictions.append(conflict)

    @staticmethod
    def _node_from_dict(raw: Mapping[str, Any]) -> VirtualNode:
        return VirtualNode(
            path=_path(str(raw["path"])),
            inode=str(raw["inode"]),
            kind=VirtualNodeKind(str(raw["kind"])),
            provenance=[str(item) for item in raw.get("provenance", ())],
            content_versions=[
                ContentVersion(
                    int(item["version"]),
                    str(item["content"]),
                    tuple(str(source) for source in item.get("source_event_ids", ())),
                )
                for item in raw.get("content_versions", ())
                if isinstance(item, Mapping)
            ],
            properties=dict(raw.get("properties", {})),
            unresolved_fields=set(str(item) for item in raw.get("unresolved_fields", ())),
            last_mutation_event=(
                str(raw["last_mutation_event"])
                if raw.get("last_mutation_event") is not None
                else None
            ),
        )

    @staticmethod
    def _process_from_dict(raw: Mapping[str, Any]) -> VirtualProcess:
        return VirtualProcess(
            pid=int(raw["pid"]),
            parent_pid=None if raw.get("parent_pid") is None else int(raw["parent_pid"]),
            executable=str(raw["executable"]),
            args=tuple(str(item) for item in raw.get("args", ())),
            state=str(raw.get("state", "running")),
            signals=[str(item) for item in raw.get("signals", ())],
            provenance=[str(item) for item in raw.get("provenance", ())],
            event_callbacks={
                str(key): str(value) for key, value in dict(raw.get("event_callbacks", {})).items()
            },
        )

    @staticmethod
    def _clock_revision_from_dict(raw: Mapping[str, Any]) -> VirtualClockRevision:
        operation = str(raw["operation"])
        if operation not in {"set", "advance"}:
            raise VirtualWorldError(f"invalid virtual clock operation: {operation}")
        return VirtualClockRevision(
            revision=int(raw["revision"]),
            operation=operation,  # type: ignore[arg-type]
            value=str(raw["value"]),
            unit=str(raw["unit"]),
            delta=None if raw.get("delta") is None else str(raw["delta"]),
            source_event_ids=tuple(str(item) for item in raw.get("source_event_ids", ())),
        )

    @classmethod
    def _clock_from_dict(cls, raw: Mapping[str, Any]) -> VirtualClock:
        return VirtualClock(
            clock_id=_clock_id(str(raw["clock_id"])),
            provenance=[str(item) for item in raw.get("provenance", ())],
            unresolved_fields=set(str(item) for item in raw.get("unresolved_fields", ())),
            revisions=[
                cls._clock_revision_from_dict(item)
                for item in raw.get("revisions", ())
                if isinstance(item, Mapping)
            ],
            contradictions=[
                VirtualClockContradiction(
                    prior_revision=int(item["prior_revision"]),
                    conflicting_revision=int(item["conflicting_revision"]),
                    source_event_ids=tuple(
                        str(source) for source in item.get("source_event_ids", ())
                    ),
                )
                for item in raw.get("contradictions", ())
                if isinstance(item, Mapping)
            ],
            last_mutation_event=(
                None if raw.get("last_mutation_event") is None else str(raw["last_mutation_event"])
            ),
        )

    def _append_mutation(self, mutation: VirtualMutation) -> None:
        sources = [self.store.get(event_id) for event_id in mutation.source_event_ids]
        if any(source is None for source in sources):
            raise VirtualWorldError("virtual mutation cites an unknown source event")
        source = next(source for source in reversed(sources) if source is not None)
        branch_events = self.store.list_events(
            session_id=self.session_id,
            branch_id=self.branch_id,
            ascending=True,
        )
        event = Event.new(
            mutation.event_type,
            actor=Actor(kind=ActorKind.TOOL, id="virtual-runtime"),
            session_id=self.session_id,
            branch_id=self.branch_id,
            parent_event_id=branch_events[-1].id if branch_events else source.id,
            causation_id=source.id,
            correlation_id=source.correlation_id,
            payload={**dict(mutation.payload), "source_event_ids": list(mutation.source_event_ids)},
            metadata={
                "schema_version": 1,
                **(
                    {"truth_domain": "virtual"}
                    if mutation.payload.get("truth_domain") == "virtual"
                    else {}
                ),
            },
        )
        self.store.append(event)


__all__ = [
    "ContentVersion",
    "EventBackedVirtualWorld",
    "SourceEvidence",
    "VirtualArtifactMaterializer",
    "VirtualClock",
    "VirtualClockContradiction",
    "VirtualClockMaterializer",
    "VirtualClockRegistry",
    "VirtualClockRevision",
    "VirtualCommand",
    "VirtualCommandRegistry",
    "VirtualFileSystem",
    "VirtualMutation",
    "VirtualNode",
    "VirtualNodeKind",
    "VirtualNotFoundError",
    "VirtualPathProfile",
    "VirtualProcess",
    "VirtualProcessTable",
    "VirtualRuntime",
    "VirtualWorldError",
    "VirtualWorldRuntime",
]
