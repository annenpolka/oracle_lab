"""Provider-neutral oracle generation contracts and HTTP adapters."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Literal, Protocol, runtime_checkable

import httpx

from oracle_lab.config import ModelProfile, ProviderConfig
from oracle_lab.jsonutil import canonical_json, sha256_text


class ProviderError(RuntimeError):
    """Base provider failure safe to record in an ``oracle.error`` event."""


class ProviderConfigurationError(ProviderError):
    pass


class ProviderHTTPError(ProviderError):
    """An HTTP failure that retains the unmodified response for archiving."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        raw_bytes: bytes,
        headers: Mapping[str, str],
        elapsed_ms: float,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.raw_bytes = raw_bytes
        self.headers = MappingProxyType(dict(headers))
        self.elapsed_ms = elapsed_ms


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def thaw_provider_value(value: Any) -> Any:
    """Return ordinary JSON containers from a frozen provider boundary value."""
    if isinstance(value, Mapping):
        return {str(key): thaw_provider_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_provider_value(item) for item in value]
    return value


def _copy_json_object(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    # JSON round-tripping prevents callers from mutating a nested request after
    # its hash has been used as an experimental identity.
    try:
        copied = json.loads(canonical_json(thaw_provider_value(value)))
    except (TypeError, ValueError) as exc:
        raise ValueError("provider metadata must be JSON serializable") from exc
    if not isinstance(copied, dict):  # pragma: no cover - Mapping always encodes as object
        raise ValueError("provider metadata must be an object")
    return copied


def _copy_messages(messages: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    copied: list[Mapping[str, Any]] = []
    for index, message in enumerate(messages):
        role = message.get("role")
        if not isinstance(role, str) or not role:
            raise ValueError(f"messages[{index}].role must be a non-empty string")
        if "content" not in message:
            raise ValueError(f"messages[{index}] must contain content")
        materialized = _copy_json_object(message)
        copied.append(_deep_freeze(materialized))
    return tuple(copied)


@dataclass(frozen=True, slots=True, init=False)
class OracleGenerateRequest:
    model_profile_id: str
    messages: tuple[Mapping[str, Any], ...]
    temperature: float | None
    top_p: float | None
    max_tokens: int | None
    provider_pin: str | None
    seed: int | None
    metadata: Mapping[str, Any]

    def __init__(
        self,
        model_profile_id: str,
        messages: Sequence[Mapping[str, Any]],
        *,
        temperature: float | None = None,
        top_p: float | None = None,
        max_tokens: int | None = None,
        provider_pin: str | None = None,
        seed: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not model_profile_id:
            raise ValueError("model_profile_id must not be empty")
        if temperature is not None and not 0 <= temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        if top_p is not None and not 0 < top_p <= 1:
            raise ValueError("top_p must be in (0, 1]")
        if max_tokens is not None and max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        object.__setattr__(self, "model_profile_id", model_profile_id)
        object.__setattr__(self, "messages", _copy_messages(messages))
        object.__setattr__(self, "temperature", temperature)
        object.__setattr__(self, "top_p", top_p)
        object.__setattr__(self, "max_tokens", max_tokens)
        object.__setattr__(self, "provider_pin", provider_pin)
        object.__setattr__(self, "seed", seed)
        object.__setattr__(self, "metadata", _deep_freeze(_copy_json_object(metadata)))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> OracleGenerateRequest:
        model_profile_id = value.get("modelProfileId", value.get("model_profile_id"))
        if not isinstance(model_profile_id, str):
            raise ValueError("modelProfileId is required")
        messages = value.get("messages")
        if not isinstance(messages, list) or not all(
            isinstance(item, Mapping) for item in messages
        ):
            raise ValueError("messages must be an array of objects")
        return cls(
            model_profile_id,
            messages,
            temperature=value.get("temperature"),
            top_p=value.get("topP", value.get("top_p")),
            max_tokens=value.get("maxTokens", value.get("max_tokens")),
            provider_pin=value.get("providerPin", value.get("provider_pin")),
            seed=value.get("seed"),
            metadata=value.get("metadata"),
        )

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "modelProfileId": self.model_profile_id,
            "messages": [thaw_provider_value(message) for message in self.messages],
        }
        optional = {
            "temperature": self.temperature,
            "topP": self.top_p,
            "maxTokens": self.max_tokens,
            "providerPin": self.provider_pin,
            "seed": self.seed,
        }
        value.update({key: item for key, item in optional.items() if item is not None})
        if self.metadata:
            value["metadata"] = thaw_provider_value(self.metadata)
        return value

    @property
    def request_hash(self) -> str:
        return sha256_text(canonical_json(self.to_dict()))


@dataclass(frozen=True, slots=True)
class OracleGenerateResponse:
    """Lossless raw response plus a deliberately small normalized view."""

    raw_bytes: bytes
    status_code: int
    headers: Mapping[str, str]
    provider_name: str
    provider_model_id: str | None
    content: str
    routed_provider_name: str | None = None
    reasoning: Any = None
    finish_reason: str | None = None
    usage: Mapping[str, Any] = field(default_factory=dict)
    elapsed_ms: float = 0.0
    request_id: str | None = None
    api_revision: str | None = None
    generation_settings: Mapping[str, Any] = field(default_factory=dict)
    parsed: Mapping[str, Any] = field(default_factory=dict)
    material_origin: Literal["oracle_generated", "historical_fixture", "synthetic_fixture"] = (
        "synthetic_fixture"
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_bytes", bytes(self.raw_bytes))
        object.__setattr__(self, "headers", _deep_freeze(dict(self.headers)))
        object.__setattr__(self, "usage", _deep_freeze(_copy_json_object(self.usage)))
        object.__setattr__(
            self,
            "reasoning",
            _deep_freeze(thaw_provider_value(self.reasoning)),
        )
        object.__setattr__(
            self,
            "generation_settings",
            _deep_freeze(_copy_json_object(self.generation_settings)),
        )
        object.__setattr__(self, "parsed", _deep_freeze(_copy_json_object(self.parsed)))


@runtime_checkable
class OracleProvider(Protocol):
    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse: ...


ProfileResolver = Mapping[str, ModelProfile] | Callable[[str], ModelProfile]


def _resolve_profile(resolver: ProfileResolver, profile_id: str) -> ModelProfile:
    try:
        profile = resolver(profile_id) if callable(resolver) else resolver[profile_id]
    except (KeyError, LookupError) as exc:
        raise ProviderConfigurationError(f"unknown model profile: {profile_id}") from exc
    if not isinstance(profile, ModelProfile):
        raise ProviderConfigurationError("profile resolver must return ModelProfile")
    return profile


class OpenAICompatibleProvider:
    """Adapter for the OpenAI chat-completions wire format.

    A caller may inject an ``httpx.AsyncClient`` (normally with MockTransport)
    for contract tests.  An injected client is owned by the caller and is not
    closed by this adapter.
    """

    provider_name = "openai_compatible"

    def __init__(
        self,
        config: ProviderConfig,
        profiles: ProfileResolver,
        *,
        client: httpx.AsyncClient | None = None,
        api_key: str | None = None,
    ) -> None:
        self.config = config
        self.profiles = profiles
        self._client = client
        self._explicit_api_key = api_key

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json", "content-type": "application/json"}
        headers.update(self.config.headers)
        key = self._explicit_api_key
        if key is None and self.config.api_key_env:
            key = os.environ.get(self.config.api_key_env)
        if key:
            headers["authorization"] = f"Bearer {key}"
        return headers

    def _payload(self, request: OracleGenerateRequest, profile: ModelProfile) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": profile.slug,
            "messages": [thaw_provider_value(message) for message in request.messages],
            "temperature": (
                profile.temperature if request.temperature is None else request.temperature
            ),
            "top_p": profile.top_p if request.top_p is None else request.top_p,
        }
        max_tokens = profile.max_tokens if request.max_tokens is None else request.max_tokens
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if request.seed is not None:
            payload["seed"] = request.seed
        return payload

    def _normalize(
        self,
        *,
        raw_bytes: bytes,
        status_code: int,
        headers: Mapping[str, str],
        elapsed_ms: float,
    ) -> OracleGenerateResponse:
        try:
            parsed = json.loads(raw_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProviderError("provider returned a non-JSON success response") from exc
        if not isinstance(parsed, dict):
            raise ProviderError("provider response must be a JSON object")
        choices = parsed.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise ProviderError("provider response has no choices[0]")
        choice = choices[0]
        message = choice.get("message")
        if not isinstance(message, dict):
            raise ProviderError("provider response has no choices[0].message")
        content = message.get("content", "")
        if content is None:
            content = ""
        if not isinstance(content, str):
            # Preserve the full shape in parsed/raw, while exposing a stable
            # textual view to the current oracle event schema.
            content = canonical_json(content)
        reasoning = message.get(
            "reasoning",
            message.get("reasoning_content", parsed.get("reasoning")),
        )
        usage = parsed.get("usage") if isinstance(parsed.get("usage"), dict) else {}
        header_lookup = {key.lower(): value for key, value in headers.items()}
        return OracleGenerateResponse(
            raw_bytes=raw_bytes,
            status_code=status_code,
            headers=headers,
            provider_name=self.provider_name,
            provider_model_id=(str(parsed["model"]) if parsed.get("model") is not None else None),
            content=content,
            routed_provider_name=(
                str(parsed["provider"]) if parsed.get("provider") is not None else None
            ),
            reasoning=reasoning,
            finish_reason=(
                str(choice["finish_reason"]) if choice.get("finish_reason") is not None else None
            ),
            usage=usage,
            elapsed_ms=elapsed_ms,
            request_id=(
                str(parsed.get("id") or header_lookup.get("x-request-id"))
                if parsed.get("id") or header_lookup.get("x-request-id")
                else None
            ),
            api_revision=header_lookup.get("x-api-version") or header_lookup.get("openai-version"),
            parsed=parsed,
            # Only a production HTTP adapter may claim a fresh oracle call by
            # default. Custom/in-memory providers remain synthetic unless they
            # explicitly declare a sourced historical fixture.
            material_origin="oracle_generated",
        )

    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        profile = _resolve_profile(self.profiles, request.model_profile_id)
        url = f"{self.config.base_url.rstrip('/')}/chat/completions"
        payload = self._payload(request, profile)
        start = time.monotonic()
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self.config.timeout_seconds)
        try:
            response = await client.post(url, headers=self._headers(), json=payload)
        except httpx.HTTPError as exc:
            raise ProviderError(f"provider transport failed: {type(exc).__name__}") from exc
        finally:
            if owns_client:
                await client.aclose()
        elapsed_ms = (time.monotonic() - start) * 1000
        raw_bytes = bytes(response.content)
        response_headers = dict(response.headers.items())
        if not 200 <= response.status_code < 300:
            raise ProviderHTTPError(
                f"provider returned HTTP {response.status_code}",
                status_code=response.status_code,
                raw_bytes=raw_bytes,
                headers=response_headers,
                elapsed_ms=elapsed_ms,
            )
        try:
            normalized = self._normalize(
                raw_bytes=raw_bytes,
                status_code=response.status_code,
                headers=response_headers,
                elapsed_ms=elapsed_ms,
            )
        except ProviderError as exc:
            # A syntactically/structurally malformed 2xx response is still a
            # provider response and must cross the lossless archive boundary.
            raise ProviderHTTPError(
                f"provider response normalization failed: {exc}",
                status_code=response.status_code,
                raw_bytes=raw_bytes,
                headers=response_headers,
                elapsed_ms=elapsed_ms,
            ) from exc
        settings = {
            key: value
            for key, value in payload.items()
            if key in {"model", "temperature", "top_p", "max_tokens", "seed", "provider"}
        }
        return replace(normalized, generation_settings=settings)


class OpenRouterProvider(OpenAICompatibleProvider):
    provider_name = "openrouter"

    def _headers(self) -> dict[str, str]:
        headers = super()._headers()
        if "authorization" not in headers:
            raise ProviderConfigurationError(
                f"OpenRouter credential is missing ({self.config.api_key_env or 'api_key'})"
            )
        return headers

    def _payload(self, request: OracleGenerateRequest, profile: ModelProfile) -> dict[str, Any]:
        payload = super()._payload(request, profile)
        pin = request.provider_pin or profile.pin_provider
        if pin:
            payload["provider"] = {
                "order": [pin],
                "allow_fallbacks": profile.allow_fallback,
            }
        elif not profile.allow_fallback:
            # No route can be pinned without a provider name, but preserve the
            # explicit no-fallback intent in the native OpenRouter field.
            payload["provider"] = {"allow_fallbacks": False}
        return payload


class LocalMLXProvider(OpenAICompatibleProvider):
    """OpenAI-compatible local MLX endpoint kept as a distinct provider type."""

    provider_name = "local_mlx"


ReplayFixture = OracleGenerateResponse | bytes | Mapping[str, Any]


class ReplayProvider:
    """Deterministic provider keyed by the canonical request hash."""

    def __init__(
        self,
        fixtures: Mapping[str, ReplayFixture],
        *,
        provider_name: str = "replay",
        fixture_origin: Literal["historical_fixture", "synthetic_fixture"] = ("synthetic_fixture"),
    ) -> None:
        self._fixtures = dict(fixtures)
        self.provider_name = provider_name
        self.fixture_origin = fixture_origin

    @staticmethod
    def key_for(request: OracleGenerateRequest) -> str:
        return request.request_hash

    async def generate(self, request: OracleGenerateRequest) -> OracleGenerateResponse:
        key = self.key_for(request)
        try:
            fixture = self._fixtures[key]
        except KeyError as exc:
            raise ProviderError(f"no replay fixture for request {key}") from exc
        if isinstance(fixture, OracleGenerateResponse):
            return replace(
                fixture,
                raw_bytes=bytes(fixture.raw_bytes),
                elapsed_ms=0.0,
                material_origin=self.fixture_origin,
            )
        if isinstance(fixture, Mapping):
            raw_bytes = canonical_json(fixture).encode("utf-8")
        else:
            raw_bytes = bytes(fixture)
        # Reuse the production normalizer so replay exercises the same lossless
        # boundary and fails on malformed historical fixtures.
        normalizer = OpenAICompatibleProvider.__new__(OpenAICompatibleProvider)
        normalizer.provider_name = self.provider_name
        try:
            return replace(
                normalizer._normalize(
                    raw_bytes=raw_bytes,
                    status_code=200,
                    headers={},
                    elapsed_ms=0.0,
                ),
                material_origin=self.fixture_origin,
            )
        except ProviderError as exc:
            raise ProviderHTTPError(
                f"replay response normalization failed: {exc}",
                status_code=200,
                raw_bytes=raw_bytes,
                headers={},
                elapsed_ms=0.0,
            ) from exc


def create_provider(
    config: ProviderConfig,
    profiles: ProfileResolver,
    *,
    client: httpx.AsyncClient | None = None,
    api_key: str | None = None,
) -> OracleProvider:
    if config.kind == "openrouter":
        return OpenRouterProvider(config, profiles, client=client, api_key=api_key)
    if config.kind == "openai_compatible":
        return OpenAICompatibleProvider(config, profiles, client=client, api_key=api_key)
    if config.kind == "local_mlx":
        return LocalMLXProvider(config, profiles, client=client, api_key=api_key)
    raise ProviderConfigurationError("ReplayProvider requires explicit fixtures")


__all__ = [
    "LocalMLXProvider",
    "OpenAICompatibleProvider",
    "OpenRouterProvider",
    "OracleGenerateRequest",
    "OracleGenerateResponse",
    "OracleProvider",
    "ProviderConfigurationError",
    "ProviderError",
    "ProviderHTTPError",
    "ReplayProvider",
    "create_provider",
    "thaw_provider_value",
]
