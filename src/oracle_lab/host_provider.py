"""Host-model HTTP boundary kept deliberately separate from OracleProvider.

This module implements only the Direct Host analysis wire contract.  It never
creates ``oracle.*`` events, never reads ``config/models.toml``, and never
assigns an Oracle material origin.  Credential values are resolved immediately
before the HTTP request and are not retained on the call object or response.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import httpx

from oracle_lab.jsonutil import canonical_json, sha256_text

HOST_PROMPT_CONTRACT_VERSION = "oracle-lab-direct-host-v1"
_SENSITIVE_HEADER_NAMES = frozenset(
    {"authorization", "cookie", "proxy-authorization", "set-cookie", "x-api-key"}
)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _json_object(value: Mapping[str, Any]) -> dict[str, Any]:
    copied = json.loads(canonical_json(value))
    if not isinstance(copied, dict):  # pragma: no cover - Mapping encodes as object
        raise ValueError("Host provider metadata must be a JSON object")
    return copied


def _redact_known_values(value: str, known_credentials: Sequence[str]) -> str:
    redacted = value
    for credential in sorted(
        (item for item in known_credentials if item),
        key=len,
        reverse=True,
    ):
        redacted = redacted.replace(credential, "[redacted]")
    return redacted


def _redacted_headers(
    headers: Mapping[str, str],
    *,
    known_credentials: Sequence[str] = (),
) -> dict[str, str]:
    return {
        str(name): (
            "[redacted]"
            if str(name).casefold() in _SENSITIVE_HEADER_NAMES
            else _redact_known_values(str(value), known_credentials)
        )
        for name, value in headers.items()
    }


def _credential_encodings(known_credentials: Sequence[str]) -> tuple[bytes, ...]:
    values: set[bytes] = set()
    for credential in known_credentials:
        if not credential:
            continue
        values.add(credential.encode("utf-8"))
        # A provider may reflect a credential inside JSON using escaped Unicode.
        values.add(json.dumps(credential, ensure_ascii=True)[1:-1].encode("ascii"))
    return tuple(sorted((value for value in values if value), key=len, reverse=True))


def _contains_credential(raw: bytes, known_credentials: Sequence[str]) -> bool:
    if any(value in raw for value in _credential_encodings(known_credentials)):
        return True
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return False
    pending = [parsed]
    while pending:
        value = pending.pop()
        if isinstance(value, str):
            if any(credential and credential in value for credential in known_credentials):
                return True
        elif isinstance(value, Mapping):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, list | tuple):
            pending.extend(value)
    return False


async def _bounded_response_bytes(
    response: httpx.Response,
    *,
    max_output_bytes: int,
) -> tuple[bytes, bool]:
    """Read at most ``max_output_bytes`` while still detecting one-byte overflow."""

    # ``httpx.MockTransport`` permits handlers to return an already-consumed
    # in-memory response.  Keep that deterministic injection seam compatible,
    # while the real HTTP transport always takes the streaming branch below.
    if response.is_stream_consumed:
        content = response.content
        return content[:max_output_bytes], len(content) > max_output_bytes
    captured = bytearray()
    output_limited = False
    chunk_size = max(1, min(64 * 1024, max_output_bytes + 1))
    async for chunk in response.aiter_raw(chunk_size=chunk_size):
        remaining = max_output_bytes - len(captured)
        if remaining > 0:
            captured.extend(chunk[:remaining])
        if len(chunk) > remaining:
            output_limited = True
            break
    return bytes(captured), output_limited


def _provider_name(value: Any) -> str | None:
    if isinstance(value, str) and value.strip() and "[redacted]" not in value:
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("actual_provider", "selected_provider", "provider", "id", "name"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


def _response_routing(
    parsed: Mapping[str, Any],
    header_lookup: Mapping[str, str],
) -> tuple[str | None, bool | None, Mapping[str, Any]]:
    routing = parsed.get("routing")
    routing_object = routing if isinstance(routing, Mapping) else {}
    actual_provider = _provider_name(parsed.get("provider")) or _provider_name(routing_object)
    if actual_provider is None:
        for header_name in (
            "x-routed-provider",
            "x-openrouter-provider",
            "x-provider",
        ):
            actual_provider = _provider_name(header_lookup.get(header_name))
            if actual_provider is not None:
                break

    explicit_fallback: bool | None = None
    for container in (parsed, routing_object):
        for key in ("fallback_occurred", "fallback", "is_fallback"):
            candidate = container.get(key)
            if isinstance(candidate, bool):
                explicit_fallback = candidate
                break
        if explicit_fallback is not None:
            break
    if explicit_fallback is None:
        header_value = header_lookup.get("x-provider-fallback")
        if isinstance(header_value, str):
            if header_value.strip().casefold() == "true":
                explicit_fallback = True
            elif header_value.strip().casefold() == "false":
                explicit_fallback = False
    return actual_provider, explicit_fallback, _json_object(routing_object)


class HostProviderError(RuntimeError):
    """Safe Host-call failure carrying any exact provider response bytes."""

    def __init__(
        self,
        message: str,
        *,
        raw_response: bytes = b"",
        api_response_metadata: Mapping[str, Any] | None = None,
        output_limited: bool = False,
        requested_provider_id: str | None = None,
        requested_model: str | None = None,
        actual_provider: str | None = None,
        returned_model: str | None = None,
        routing_settings: Mapping[str, Any] | None = None,
        sampling_settings: Mapping[str, Any] | None = None,
        usage: Mapping[str, Any] | None = None,
        elapsed_ms: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.raw_response = bytes(raw_response)
        self.api_response_metadata = _freeze(_json_object(api_response_metadata or {}))
        self.output_limited = bool(output_limited)
        self.requested_provider_id = requested_provider_id
        self.requested_model = requested_model
        self.actual_provider = actual_provider
        self.returned_model = returned_model
        self.routing_settings = _freeze(_json_object(routing_settings or {}))
        self.sampling_settings = _freeze(_json_object(sampling_settings or {}))
        self.usage = _freeze(_json_object(usage or {}))
        self.elapsed_ms = float(elapsed_ms)


@dataclass(frozen=True, slots=True)
class HostProviderResponse:
    """Lossless response plus the explicit non-Oracle Host identity envelope."""

    output: Mapping[str, Any]
    raw_response: bytes
    requested_provider_id: str
    requested_model: str
    actual_provider: str | None
    returned_model: str | None
    routing_settings: Mapping[str, Any] = field(default_factory=dict)
    sampling_settings: Mapping[str, Any] = field(default_factory=dict)
    api_response_metadata: Mapping[str, Any] = field(default_factory=dict)
    usage: Mapping[str, Any] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", _freeze(_json_object(self.output)))
        object.__setattr__(self, "raw_response", bytes(self.raw_response))
        object.__setattr__(
            self,
            "routing_settings",
            _freeze(_json_object(self.routing_settings)),
        )
        object.__setattr__(
            self,
            "sampling_settings",
            _freeze(_json_object(self.sampling_settings)),
        )
        object.__setattr__(
            self,
            "api_response_metadata",
            _freeze(_json_object(self.api_response_metadata)),
        )
        object.__setattr__(self, "usage", _freeze(_json_object(self.usage)))


class OpenAICompatibleHostCall:
    """OpenAI chat-completions transport for Host analysis only.

    ``client`` is an injection seam for deterministic ``httpx.MockTransport``
    tests.  A supplied client remains owned by the caller.
    """

    adapter_version = HOST_PROMPT_CONTRACT_VERSION

    def __init__(
        self,
        *,
        provider_id: str,
        base_url: str,
        api_key_env: str | None,
        model: str,
        temperature: float | None,
        top_p: float | None,
        max_tokens: int | None,
        allow_fallback: bool,
        timeout_seconds: float,
        max_output_bytes: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if max_output_bytes <= 0:
            raise ValueError("Direct Host max_output_bytes must be positive")
        self.provider_id = provider_id
        self.base_url = base_url.rstrip("/")
        self.api_key_env = api_key_env
        self.model = model
        self.temperature = temperature
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.allow_fallback = allow_fallback
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self._client = client

    def _request_payload(
        self,
        *,
        task_type: str,
        prompt: str,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "provider": {
                "order": [self.provider_id],
                "allow_fallbacks": self.allow_fallback,
            },
        }
        if self.temperature is not None:
            request["temperature"] = self.temperature
        if self.top_p is not None:
            request["top_p"] = self.top_p
        if self.max_tokens is not None:
            request["max_tokens"] = self.max_tokens
        request["metadata"] = {
            "host_prompt_contract": HOST_PROMPT_CONTRACT_VERSION,
            "task_type": task_type,
            "prompt_sha256": sha256_text(prompt),
            "idempotency_key": idempotency_key,
        }
        return request

    async def __call__(
        self,
        task_type: str,
        payload: Mapping[str, Any],
    ) -> HostProviderResponse:
        prompt = payload.get("prompt")
        if not isinstance(prompt, str) or not prompt:
            raise HostProviderError("Direct Host payload requires the exact rendered prompt")
        raw_idempotency_key = payload.get("idempotency_key")
        idempotency_key = raw_idempotency_key if isinstance(raw_idempotency_key, str) else None
        request_payload = self._request_payload(
            task_type=task_type,
            prompt=prompt,
            idempotency_key=idempotency_key,
        )
        headers = {
            "accept": "application/json",
            "accept-encoding": "identity",
            "content-type": "application/json",
        }
        if idempotency_key:
            headers["idempotency-key"] = idempotency_key
        # Resolve, use, and immediately discard the credential value.  Only the
        # environment-variable name exists in durable configuration snapshots.
        credential = os.environ.get(self.api_key_env) if self.api_key_env else None
        known_credentials = (credential,) if credential else ()
        if credential:
            headers["authorization"] = f"Bearer {credential}"
        started = time.monotonic()
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            try:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=request_payload,
                    timeout=self.timeout_seconds,
                ) as response:
                    response_headers = dict(response.headers.items())
                    status_code = response.status_code
                    content_encoding = response.headers.get("content-encoding")
                    unsupported_content_encoding = bool(
                        content_encoding and content_encoding.strip().casefold() != "identity"
                    )
                    if unsupported_content_encoding:
                        raw, output_limited = b"", False
                    else:
                        raw, output_limited = await _bounded_response_bytes(
                            response,
                            max_output_bytes=self.max_output_bytes,
                        )
            except httpx.HTTPError as error:
                raise HostProviderError(
                    f"Host provider transport failed: {type(error).__name__}",
                    requested_provider_id=self.provider_id,
                    requested_model=self.model,
                ) from error
        finally:
            credential = None
            if owns_client:
                await client.aclose()
        elapsed_ms = (time.monotonic() - started) * 1000
        header_lookup = {key.casefold(): value for key, value in response_headers.items()}
        redacted_response_headers = _redacted_headers(
            response_headers,
            known_credentials=known_credentials,
        )
        safe_header_lookup = {
            key.casefold(): value for key, value in redacted_response_headers.items()
        }
        response_metadata: dict[str, Any] = {
            "status_code": status_code,
            "headers": redacted_response_headers,
            "request_id": _redact_known_values(
                header_lookup.get("x-request-id", ""), known_credentials
            )
            or None,
            "api_revision": _redact_known_values(
                header_lookup.get("x-api-version") or header_lookup.get("openai-version") or "",
                known_credentials,
            )
            or None,
        }
        sampling_settings = {
            key: request_payload[key]
            for key in ("model", "temperature", "top_p", "max_tokens")
            if key in request_payload
        }
        requested_routing = {
            "requested_provider_id": self.provider_id,
            "allow_fallback": self.allow_fallback,
            "request_provider_routing": request_payload["provider"],
            "fallback_status": None,
        }
        if unsupported_content_encoding:
            response_metadata.update(
                {
                    "raw_response_disposition": "quarantined_content_encoding",
                    "captured_bytes": 0,
                    "content_encoding": content_encoding,
                    "max_output_bytes": self.max_output_bytes,
                    "output_limited": False,
                }
            )
            raise HostProviderError(
                "Host provider ignored identity encoding; encoded response was quarantined",
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        if _contains_credential(raw, known_credentials):
            response_metadata.update(
                {
                    "raw_response_disposition": "quarantined_credential",
                    "captured_bytes": 0,
                    "max_output_bytes": self.max_output_bytes,
                    "output_limited": output_limited,
                }
            )
            raise HostProviderError(
                "Host provider response contained a configured credential and was quarantined",
                api_response_metadata=response_metadata,
                output_limited=output_limited,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        if output_limited:
            response_metadata.update(
                {
                    "raw_response_disposition": "bounded_prefix",
                    "captured_bytes": len(raw),
                    "max_output_bytes": self.max_output_bytes,
                    "output_limited": True,
                }
            )
            raise HostProviderError(
                f"Host provider response exceeded {self.max_output_bytes} bytes",
                raw_response=raw,
                api_response_metadata=response_metadata,
                output_limited=True,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        if not 200 <= status_code < 300:
            raise HostProviderError(
                f"Host provider returned HTTP {status_code}",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        try:
            parsed = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HostProviderError(
                "Host provider returned non-JSON success response",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            ) from error
        if not isinstance(parsed, Mapping):
            raise HostProviderError(
                "Host provider response must be an object",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        choices = parsed.get("choices")
        choice = choices[0] if isinstance(choices, list) and choices else None
        message = choice.get("message") if isinstance(choice, Mapping) else None
        content = message.get("content") if isinstance(message, Mapping) else None
        if not isinstance(content, str):
            raise HostProviderError(
                "Host provider response has no textual choices[0].message.content",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        try:
            output = json.loads(content)
        except json.JSONDecodeError as error:
            raise HostProviderError(
                "Host provider assistant content is not JSON",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            ) from error
        if not isinstance(output, Mapping):
            raise HostProviderError(
                "Host provider assistant content must be a JSON object",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                routing_settings=requested_routing,
                sampling_settings=sampling_settings,
                elapsed_ms=elapsed_ms,
            )
        actual_provider, explicit_fallback, provider_routing = _response_routing(
            parsed,
            safe_header_lookup,
        )
        returned_model = parsed.get("model")
        returned_model = str(returned_model) if returned_model is not None else None
        provider_mismatch = (
            actual_provider is not None
            and actual_provider.strip().casefold() != self.provider_id.strip().casefold()
        )
        if explicit_fallback is True or provider_mismatch:
            fallback_status: bool | None = True
        elif actual_provider is not None or explicit_fallback is False:
            fallback_status = False
        else:
            fallback_status = None
        routing_settings = {
            **requested_routing,
            "fallback_status": fallback_status,
            "provider_response_routing": provider_routing,
        }
        response_metadata["finish_reason"] = (
            str(choice["finish_reason"])
            if isinstance(choice, Mapping) and choice.get("finish_reason") is not None
            else None
        )
        usage = parsed.get("usage")
        usage_object = usage if isinstance(usage, Mapping) else {}
        if fallback_status is True and not self.allow_fallback:
            raise HostProviderError(
                "Host provider used fallback routing while fallback is disabled",
                raw_response=raw,
                api_response_metadata=response_metadata,
                requested_provider_id=self.provider_id,
                requested_model=self.model,
                actual_provider=actual_provider,
                returned_model=returned_model,
                routing_settings=routing_settings,
                sampling_settings=sampling_settings,
                usage=usage_object,
                elapsed_ms=elapsed_ms,
            )
        return HostProviderResponse(
            output=output,
            raw_response=raw,
            requested_provider_id=self.provider_id,
            requested_model=self.model,
            actual_provider=actual_provider,
            returned_model=returned_model,
            routing_settings=routing_settings,
            sampling_settings=sampling_settings,
            api_response_metadata=response_metadata,
            usage=usage_object,
            elapsed_ms=elapsed_ms,
        )


__all__ = [
    "HOST_PROMPT_CONTRACT_VERSION",
    "HostProviderError",
    "HostProviderResponse",
    "OpenAICompatibleHostCall",
]
