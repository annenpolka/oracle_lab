"""Strict, non-authorizing decoders for observed standalone ``sbx v0.39`` data.

This module only turns bounded control-plane bytes into immutable observation
types.  It does not join the UUID returned by ``ls`` to name-selected
``inspect`` fields, run ``sbx``, archive evidence, start a model, or import or
construct an isolation attestation.  The missing atomic instance join is
deliberate: separate control-plane calls cannot rule out name reuse or ABA.

The decoders reject schema drift and never include input bytes, JSON keys, or
field values in an exception.  Callers that archive an observation must retain
the exact raw bytes separately and attach the appropriate ``truth_domain`` and
provenance at that boundary; decoded values are derived data.
"""

from __future__ import annotations

import ipaddress
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, NoReturn

_MAX_JSON_BYTES = 1024 * 1024
_VERSION_OUTPUT = re.compile(r"\Asbx version: (v0\.39\.0) ([0-9a-f]{40})\n\Z")
_SAFE_NAME = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{1,199}\Z")
_SAFE_WORD = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")
_SHA256 = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_DURATION = re.compile(r"\A(?:0|(?:[0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s|m|h))+)[ ]?\Z")
_INSPECT_PORT = re.compile(
    r"\A(?P<host>[^:\s]+):(?P<host_port>[0-9]{1,5})"
    r"->(?P<sandbox_port>[0-9]{1,5})/(?P<protocol>tcp|udp)\Z"
)

_INVENTORY_ROOT_KEYS = frozenset({"sandboxes"})
_INVENTORY_ENTRY_KEYS = frozenset({"name", "id", "agent", "status", "ports", "workspaces"})
_INVENTORY_PORT_KEYS = frozenset({"host_ip", "host_port", "sandbox_port", "protocol"})
_INSPECT_KEYS = frozenset(
    {
        "name",
        "agent",
        "kits",
        "state",
        "uptime",
        "image",
        "image_digest",
        "workspace",
        "network",
        "network_policy",
        "proxy",
        "secrets",
        "mcp_gateway",
        "ports",
        "sessions",
        "daemon_version",
        "daemon_uptime",
    }
)
_NETWORK_POLICY_KEYS = frozenset({"scope"})
_SECRET_REFERENCE_KEYS = frozenset({"name", "source"})


class SbxObservationError(ValueError):
    """Secret-free failure with a stable machine-readable reason ID."""

    __slots__ = ("reason_id",)

    def __init__(self, reason_id: str) -> None:
        self.reason_id = reason_id
        super().__init__(reason_id)


class _DuplicateJsonKey(ValueError):
    """Private sentinel; deliberately carries no duplicate key value."""


class _InvalidJsonConstant(ValueError):
    """Private sentinel for JSON extensions such as NaN and Infinity."""


@dataclass(frozen=True, slots=True)
class SbxV039Version:
    version: str
    commit_sha: str


@dataclass(frozen=True, slots=True)
class SbxV039Port:
    host_ip: str
    host_port: int
    sandbox_port: int
    protocol: str


@dataclass(frozen=True, slots=True)
class SbxV039Sandbox:
    name: str
    server_uuid: str
    agent: str
    status: str
    ports: tuple[SbxV039Port, ...]
    workspaces: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SbxV039Inventory:
    sandboxes: tuple[SbxV039Sandbox, ...]


@dataclass(frozen=True, slots=True)
class SbxV039Inspect:
    """Redacted inspect view.

    Secret reference names are validated for schema purposes and then
    discarded.  Only their count and non-secret source classifications remain
    in the public derived representation.
    """

    name: str
    agent: str
    kits: tuple[str, ...]
    state: str
    uptime: str
    image: str
    image_digest: str
    workspace: str
    network: str
    network_policy_scope: str
    proxy: str
    secret_count: int
    secret_sources: tuple[str, ...]
    mcp_gateway: bool
    ports: tuple[str, ...]
    sessions: int
    daemon_version: str
    daemon_uptime: str


def _fail(reason_id: str) -> NoReturn:
    raise SbxObservationError(reason_id) from None


def _pairs_to_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey from None
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> NoReturn:
    raise _InvalidJsonConstant from None


def _decode_json(raw: bytes, *, reason_id: str) -> dict[str, Any]:
    if type(raw) is not bytes or not raw or len(raw) > _MAX_JSON_BYTES:
        _fail(reason_id)
    try:
        text = raw.decode("utf-8", "strict")
        value = json.loads(
            text,
            object_pairs_hook=_pairs_to_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError):
        _fail(reason_id)
    if type(value) is not dict:
        _fail(reason_id)
    return value


def _require_exact_keys(value: Any, expected: frozenset[str], *, reason_id: str) -> dict[str, Any]:
    if type(value) is not dict or frozenset(value) != expected:
        _fail(reason_id)
    return value


def _require_array(value: Any, *, reason_id: str) -> list[Any]:
    if type(value) is not list:
        _fail(reason_id)
    return value


def _require_string(value: Any, *, reason_id: str, allow_empty: bool = False) -> str:
    if type(value) is not str or (not allow_empty and not value) or _has_control_character(value):
        _fail(reason_id)
    return value


def _has_control_character(value: str) -> bool:
    return any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)


def _require_safe_word(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    if _SAFE_WORD.fullmatch(text) is None:
        _fail(reason_id)
    return text


def _require_name(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    if _SAFE_NAME.fullmatch(text) is None:
        _fail(reason_id)
    return text


def _require_uuid(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    try:
        parsed = uuid.UUID(text)
    except (AttributeError, ValueError):
        _fail(reason_id)
    if str(parsed) != text:
        _fail(reason_id)
    return text


def _require_port(value: Any, *, reason_id: str) -> int:
    if type(value) is not int or not 1 <= value <= 65535:
        _fail(reason_id)
    return value


def _require_absolute_path(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    path = PurePosixPath(text)
    if not path.is_absolute() or text == "/" or str(path) != text:
        _fail(reason_id)
    return text


def _require_duration(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    if _DURATION.fullmatch(text) is None:
        _fail(reason_id)
    return text


def _require_image_digest(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    if _SHA256.fullmatch(text) is None:
        _fail(reason_id)
    return text


def _decode_inventory_port(value: Any) -> SbxV039Port:
    reason_id = "sbx_v039_inventory_schema_invalid"
    item = _require_exact_keys(value, _INVENTORY_PORT_KEYS, reason_id=reason_id)
    host_ip = _require_string(item["host_ip"], reason_id=reason_id)
    try:
        parsed_ip = ipaddress.ip_address(host_ip)
    except ValueError:
        _fail(reason_id)
    if parsed_ip.compressed != host_ip:
        _fail(reason_id)
    protocol = _require_safe_word(item["protocol"], reason_id=reason_id)
    if protocol not in {"tcp", "udp"}:
        _fail(reason_id)
    return SbxV039Port(
        host_ip=host_ip,
        host_port=_require_port(item["host_port"], reason_id=reason_id),
        sandbox_port=_require_port(item["sandbox_port"], reason_id=reason_id),
        protocol=protocol,
    )


def _decode_inventory_entry(value: Any) -> SbxV039Sandbox:
    reason_id = "sbx_v039_inventory_schema_invalid"
    item = _require_exact_keys(value, _INVENTORY_ENTRY_KEYS, reason_id=reason_id)
    ports = tuple(
        sorted(
            (
                _decode_inventory_port(port)
                for port in _require_array(item["ports"], reason_id=reason_id)
            ),
            key=lambda port: (port.host_ip, port.host_port, port.sandbox_port, port.protocol),
        )
    )
    if len(ports) != len(set(ports)):
        _fail(reason_id)
    workspaces = tuple(
        sorted(
            _require_absolute_path(workspace, reason_id=reason_id)
            for workspace in _require_array(item["workspaces"], reason_id=reason_id)
        )
    )
    if not workspaces or len(workspaces) != len(set(workspaces)):
        _fail(reason_id)
    status = _require_safe_word(item["status"], reason_id=reason_id)
    if status not in {"running", "stopped"}:
        _fail(reason_id)
    return SbxV039Sandbox(
        name=_require_name(item["name"], reason_id=reason_id),
        server_uuid=_require_uuid(item["id"], reason_id=reason_id),
        agent=_require_safe_word(item["agent"], reason_id=reason_id),
        status=status,
        ports=ports,
        workspaces=workspaces,
    )


def decode_v039_version(raw: bytes) -> SbxV039Version:
    """Decode exactly ``sbx version: v0.39.0 <40 lowercase hex>\\n``."""

    reason_id = "sbx_v039_version_schema_invalid"
    if type(raw) is not bytes or len(raw) > 256:
        _fail(reason_id)
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError:
        _fail(reason_id)
    matched = _VERSION_OUTPUT.fullmatch(text)
    if matched is None:
        _fail(reason_id)
    return SbxV039Version(version=matched.group(1), commit_sha=matched.group(2))


def decode_v039_inventory(raw: bytes) -> SbxV039Inventory:
    """Decode an exact standalone-v0.39 sandbox inventory document."""

    reason_id = "sbx_v039_inventory_schema_invalid"
    root = _require_exact_keys(
        _decode_json(raw, reason_id=reason_id),
        _INVENTORY_ROOT_KEYS,
        reason_id=reason_id,
    )
    sandboxes = tuple(
        sorted(
            (
                _decode_inventory_entry(item)
                for item in _require_array(root["sandboxes"], reason_id=reason_id)
            ),
            key=lambda sandbox: (sandbox.name, sandbox.server_uuid),
        )
    )
    names = tuple(item.name for item in sandboxes)
    server_uuids = tuple(item.server_uuid for item in sandboxes)
    if len(names) != len(set(names)) or len(server_uuids) != len(set(server_uuids)):
        _fail(reason_id)
    return SbxV039Inventory(sandboxes=sandboxes)


def _decode_inspect_port(value: Any, *, reason_id: str) -> str:
    text = _require_string(value, reason_id=reason_id)
    matched = _INSPECT_PORT.fullmatch(text)
    if matched is None:
        _fail(reason_id)
    try:
        parsed_ip = ipaddress.ip_address(matched.group("host"))
    except ValueError:
        _fail(reason_id)
    if parsed_ip.compressed != matched.group("host"):
        _fail(reason_id)
    _require_port(int(matched.group("host_port")), reason_id=reason_id)
    _require_port(int(matched.group("sandbox_port")), reason_id=reason_id)
    return text


def decode_v039_inspect(raw: bytes) -> SbxV039Inspect:
    """Decode and redact an exact standalone-v0.39 inspect document."""

    reason_id = "sbx_v039_inspect_schema_invalid"
    root = _require_exact_keys(
        _decode_json(raw, reason_id=reason_id),
        _INSPECT_KEYS,
        reason_id=reason_id,
    )
    kits = tuple(
        _require_string(item, reason_id=reason_id)
        for item in _require_array(root["kits"], reason_id=reason_id)
    )
    if len(kits) != len(set(kits)):
        _fail(reason_id)

    policy = _require_exact_keys(
        root["network_policy"],
        _NETWORK_POLICY_KEYS,
        reason_id=reason_id,
    )
    policy_scope = _require_safe_word(policy["scope"], reason_id=reason_id)
    if policy_scope not in {"global", "scoped"}:
        _fail(reason_id)

    secret_names: list[str] = []
    secret_sources: list[str] = []
    for value in _require_array(root["secrets"], reason_id=reason_id):
        secret = _require_exact_keys(value, _SECRET_REFERENCE_KEYS, reason_id=reason_id)
        secret_names.append(_require_safe_word(secret["name"], reason_id=reason_id))
        secret_sources.append(_require_safe_word(secret["source"], reason_id=reason_id))
    if len(secret_names) != len(set(secret_names)):
        _fail(reason_id)

    ports = tuple(
        _decode_inspect_port(value, reason_id=reason_id)
        for value in _require_array(root["ports"], reason_id=reason_id)
    )
    if len(ports) != len(set(ports)):
        _fail(reason_id)

    sessions = root["sessions"]
    if type(sessions) is not int or sessions < 0:
        _fail(reason_id)
    mcp_gateway = root["mcp_gateway"]
    if type(mcp_gateway) is not bool:
        _fail(reason_id)
    daemon_version = _require_string(root["daemon_version"], reason_id=reason_id)
    if daemon_version != "v0.39.0":
        _fail(reason_id)

    proxy = _require_string(root["proxy"], reason_id=reason_id)
    proxy_match = re.fullmatch(r"([^:\s]+):([0-9]{1,5})", proxy)
    if proxy_match is None:
        _fail(reason_id)
    try:
        parsed_proxy_ip = ipaddress.ip_address(proxy_match.group(1))
    except ValueError:
        _fail(reason_id)
    if parsed_proxy_ip.compressed != proxy_match.group(1):
        _fail(reason_id)
    _require_port(int(proxy_match.group(2)), reason_id=reason_id)

    image = _require_string(root["image"], reason_id=reason_id)
    if any(character.isspace() for character in image):
        _fail(reason_id)

    state = _require_safe_word(root["state"], reason_id=reason_id)
    if state not in {"running", "stopped"}:
        _fail(reason_id)

    return SbxV039Inspect(
        name=_require_name(root["name"], reason_id=reason_id),
        agent=_require_safe_word(root["agent"], reason_id=reason_id),
        kits=kits,
        state=state,
        uptime=_require_duration(root["uptime"], reason_id=reason_id),
        image=image,
        image_digest=_require_image_digest(root["image_digest"], reason_id=reason_id),
        workspace=_require_absolute_path(root["workspace"], reason_id=reason_id),
        network=_require_name(root["network"], reason_id=reason_id),
        network_policy_scope=policy_scope,
        proxy=proxy,
        secret_count=len(secret_names),
        secret_sources=tuple(secret_sources),
        mcp_gateway=mcp_gateway,
        ports=ports,
        sessions=sessions,
        daemon_version=daemon_version,
        daemon_uptime=_require_duration(root["daemon_uptime"], reason_id=reason_id),
    )


__all__ = [
    "SbxObservationError",
    "SbxV039Inspect",
    "SbxV039Inventory",
    "SbxV039Port",
    "SbxV039Sandbox",
    "SbxV039Version",
    "decode_v039_inspect",
    "decode_v039_inventory",
    "decode_v039_version",
]
