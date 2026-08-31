from __future__ import annotations

import json
import traceback
from dataclasses import asdict

import pytest

from oracle_lab.sbx_observation import (
    SbxObservationError,
    decode_v039_inspect,
    decode_v039_inventory,
    decode_v039_version,
)

_NAME = "oracle-lab-observe-0123456789abcdef"
_UUID = "13a6f276-18fc-4358-8a02-d257962b61cb"
_OTHER_UUID = "1a6f2761-18fc-4358-8a02-d257962b61cb"
_WORKSPACE = "/private/tmp/oracle-lab-real-observe.fixture"
_IMAGE = "docker.io/docker/sandbox-templates:shell-docker"
_DIGEST = "sha256:" + "5" * 64


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _entry(
    *,
    name: str = _NAME,
    server_uuid: str = _UUID,
    workspace: str = _WORKSPACE,
) -> dict[str, object]:
    return {
        "name": name,
        "id": server_uuid,
        "agent": "shell",
        "status": "running",
        "ports": [
            {
                "host_ip": "127.0.0.1",
                "host_port": 49152,
                "sandbox_port": 9418,
                "protocol": "tcp",
            }
        ],
        "workspaces": [workspace],
    }


def _inventory(*entries: dict[str, object]):
    return decode_v039_inventory(_json_bytes({"sandboxes": list(entries)}))


def _inspect_document(
    *,
    name: str = _NAME,
    workspace: str = _WORKSPACE,
    image: str = _IMAGE,
    digest: str = _DIGEST,
) -> dict[str, object]:
    return {
        "name": name,
        "agent": "shell",
        "kits": [],
        "state": "running",
        "uptime": "22s",
        "image": image,
        "image_digest": digest,
        "workspace": workspace,
        "network": name,
        "network_policy": {"scope": "global"},
        "proxy": "172.17.0.1:3128",
        "secrets": [{"name": "private-service-name", "source": "uploaded"}],
        "mcp_gateway": True,
        "ports": ["127.0.0.1:49152->9418/tcp"],
        "sessions": 0,
        "daemon_version": "v0.39.0",
        "daemon_uptime": "3m",
    }


def _inspect(**overrides: str):
    return decode_v039_inspect(_json_bytes(_inspect_document(**overrides)))


def test_real_v039_version_inventory_and_inspect_shapes_decode() -> None:
    version = decode_v039_version(
        b"sbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\n"
    )
    inventory = _inventory(_entry())
    inspected = _inspect()

    assert version.version == "v0.39.0"
    assert version.commit_sha == "def8cb0523a77e757bdd6ef52b459fe374f3783e"
    assert inventory.sandboxes[0].server_uuid == _UUID
    assert inventory.sandboxes[0].name == _NAME
    assert inspected.image_digest == _DIGEST
    assert inspected.secret_count == 1
    assert inspected.secret_sources == ("uploaded",)
    assert "private-service-name" not in repr(inspected)
    assert "private-service-name" not in json.dumps(asdict(inspected), sort_keys=True)


@pytest.mark.parametrize(
    "raw",
    [
        b"sbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e",
        b"banner\nsbx version: v0.39.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\n",
        b"sbx version: v0.40.0 def8cb0523a77e757bdd6ef52b459fe374f3783e\n",
        b"\xff",
    ],
)
def test_version_decoder_fails_closed_on_unobserved_grammar(raw: bytes) -> None:
    with pytest.raises(SbxObservationError) as captured:
        decode_v039_version(raw)

    assert captured.value.reason_id == "sbx_v039_version_schema_invalid"


def test_json_decoder_rejects_duplicate_and_unknown_secret_shaped_keys_without_leak() -> None:
    secret = "SECRET_KEY_NAME_MUST_NOT_APPEAR"
    duplicate = b'{"sandboxes":[],"sandboxes":[]}'
    unknown = _json_bytes({"sandboxes": [], secret: "value"})

    for raw in (duplicate, unknown):
        with pytest.raises(SbxObservationError) as captured:
            decode_v039_inventory(raw)
        rendered = "".join(
            traceback.format_exception(
                type(captured.value), captured.value, captured.value.__traceback__
            )
        )
        assert captured.value.reason_id == "sbx_v039_inventory_schema_invalid"
        assert secret not in str(captured.value)
        assert secret not in repr(captured.value)
        assert secret not in rendered


@pytest.mark.parametrize(
    "raw",
    [
        b'{"sandboxes":' + b"9" * 5000 + b"}",
        b'{"sandboxes":' + b"[" * 2000 + b"]" * 2000 + b"}",
    ],
)
def test_json_decoder_normalizes_integer_limit_and_recursion_failures(raw: bytes) -> None:
    with pytest.raises(SbxObservationError) as captured:
        decode_v039_inventory(raw)

    assert captured.value.reason_id == "sbx_v039_inventory_schema_invalid"
    assert captured.value.__cause__ is None


def test_inventory_is_canonical_across_entry_and_nested_order() -> None:
    first = _entry(name="sandbox-b", server_uuid=_OTHER_UUID)
    second = _entry(name="sandbox-a", server_uuid=_UUID)
    second["ports"] = [
        {
            "host_ip": "127.0.0.1",
            "host_port": 49153,
            "sandbox_port": 9419,
            "protocol": "tcp",
        },
        {
            "host_ip": "127.0.0.1",
            "host_port": 49152,
            "sandbox_port": 9418,
            "protocol": "tcp",
        },
    ]

    left = _inventory(first, second)
    second["ports"] = list(reversed(second["ports"]))  # type: ignore[arg-type]
    right = _inventory(second, first)

    assert left == right
    assert tuple(item.name for item in left.sandboxes) == ("sandbox-a", "sandbox-b")


def test_inventory_rejects_bool_port_unknown_status_and_noncanonical_uuid() -> None:
    invalid_entries = []
    bool_port = _entry()
    bool_port["ports"] = [
        {
            "host_ip": "127.0.0.1",
            "host_port": True,
            "sandbox_port": 9418,
            "protocol": "tcp",
        }
    ]
    invalid_entries.append(bool_port)
    unknown_status = _entry()
    unknown_status["status"] = "mysterious"
    invalid_entries.append(unknown_status)
    noncanonical_uuid = _entry(server_uuid=_UUID.upper())
    invalid_entries.append(noncanonical_uuid)

    for entry in invalid_entries:
        with pytest.raises(SbxObservationError, match="sbx_v039_inventory_schema_invalid"):
            _inventory(entry)
