from __future__ import annotations

from pathlib import Path
from typing import Any

from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.jsonutil import sha256_bytes, sha256_json, sha256_text


def historical_oracle_fixture(
    content: str,
    *,
    source_path: Path,
    actor_id: str = "historical-fixture",
    context_messages: list[dict[str, Any]] | None = None,
    payload_extra: dict[str, Any] | None = None,
    **envelope: Any,
) -> Event:
    """Create an explicitly sourced historical fixture with unknowns intact."""

    raw = source_path.read_bytes()
    unknown_fields = [
        "requested_model_profile_id",
        "requested_model_slug",
        "model_family",
        "checkpoint",
        "runtime",
        "quantization",
        "requested_provider_id",
        "provider_routing",
        "actual_provider",
        "actual_model_identifier",
        "fallback_occurred",
        "sampling",
        "api_response_metadata",
    ]
    return Event.new(
        EventType.ORACLE_OUTPUT,
        actor=Actor(kind=ActorKind.MODEL, id=actor_id),
        payload={
            "content": content,
            "material_origin": "historical_fixture",
            "historical_fixture": True,
            "model_identity": {
                "requested_model_profile_id": None,
                "requested_model_slug": None,
                "model_family": None,
                "checkpoint": None,
                "runtime": None,
                "quantization": None,
                "requested_provider_id": None,
                "provider_routing": None,
                "actual_provider": None,
                "actual_model_identifier": None,
                "fallback_occurred": None,
                "unknown_fields": unknown_fields,
            },
            "sampling": None,
            "api_response_metadata": None,
            "context_hash": sha256_json(context_messages or []),
            "raw_sha256": sha256_text(content),
            "source_fixture": {
                "path": str(source_path),
                "sha256": sha256_bytes(raw),
                "size_bytes": len(raw),
            },
            **dict(payload_extra or {}),
        },
        metadata={
            "schema_version": 1,
            "material_origin": "historical_fixture",
            "historical_fixture": True,
        },
        **envelope,
    )


__all__ = ["historical_oracle_fixture"]
