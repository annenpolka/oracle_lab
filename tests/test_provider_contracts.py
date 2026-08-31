from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from oracle_lab.config import ModelProfile, ProviderConfig, load_models, load_runtime_config
from oracle_lab.providers import (
    OpenRouterProvider,
    OracleGenerateRequest,
    OracleGenerateResponse,
    ProviderHTTPError,
    ReplayProvider,
)


def _profile() -> ModelProfile:
    return ModelProfile(
        id="r1",
        slug="deepseek/deepseek-r1",
        provider="openrouter",
        pin_provider="novita",
        allow_fallback=False,
    )


def test_request_hash_and_nested_values_are_defensively_frozen() -> None:
    messages = [{"role": "user", "content": [{"type": "text", "text": "before"}]}]
    metadata = {"experiment": {"tags": ["original"]}}
    request = OracleGenerateRequest("r1", messages, metadata=metadata)
    digest = request.request_hash

    messages[0]["content"][0]["text"] = "after"
    metadata["experiment"]["tags"].append("mutated")

    assert request.request_hash == digest
    assert request.to_dict()["messages"][0]["content"][0]["text"] == "before"
    assert request.to_dict()["metadata"] == {"experiment": {"tags": ["original"]}}
    with pytest.raises(TypeError):
        request.messages[0]["content"][0]["text"] = "forbidden"


def test_openrouter_contract_pins_provider_and_retains_raw_fields() -> None:
    seen: dict[str, object] = {}
    raw = (
        b'{"id":"req-1","model":"deepseek-r1","provider":"Novita",'
        b'"choices":[{"message":{"content":"answer","reasoning":{"steps":[1,2]}},'
        b'"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"extension":{"x":1}},'
        b'"future_field":{"nested":["kept"]}}'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        seen["authorization"] = request.headers.get("authorization")
        seen["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            content=raw,
            headers={"x-api-version": "2026-08-30"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenRouterProvider(
        ProviderConfig(
            id="openrouter",
            kind="openrouter",
            base_url="https://openrouter.test/v1",
            api_key_env="OPENROUTER_API_KEY",
        ),
        {"r1": _profile()},
        client=client,
        api_key="secret-for-test",
    )
    request = OracleGenerateRequest("r1", [{"role": "user", "content": "go"}])
    response = asyncio.run(provider.generate(request))
    asyncio.run(client.aclose())

    assert seen["authorization"] == "Bearer secret-for-test"
    assert seen["payload"]["provider"] == {
        "order": ["novita"],
        "allow_fallbacks": False,
    }
    assert response.raw_bytes == raw
    assert response.provider_name == "openrouter"
    assert response.routed_provider_name == "Novita"
    assert response.generation_settings == {
        "model": "deepseek/deepseek-r1",
        "temperature": 0.6,
        "top_p": 0.95,
        "provider": {"order": ("novita",), "allow_fallbacks": False},
    }
    assert response.parsed["future_field"]["nested"] == ("kept",)
    assert response.reasoning["steps"] == (1, 2)
    assert response.material_origin == "oracle_generated"
    with pytest.raises(TypeError):
        response.parsed["future_field"]["nested"][0] = "lost"


def test_replay_provider_is_keyed_by_canonical_request() -> None:
    request = OracleGenerateRequest("r1", [{"role": "user", "content": "same"}])
    fixture = {
        "model": "historical",
        "choices": [{"message": {"content": "deterministic"}, "finish_reason": "stop"}],
        "extra": {"preserved": True},
    }
    provider = ReplayProvider({request.request_hash: fixture})

    first = asyncio.run(provider.generate(request))
    second = asyncio.run(provider.generate(request))

    assert first.raw_bytes == second.raw_bytes
    assert first.content == "deterministic"
    assert first.elapsed_ms == 0
    assert first.material_origin == "synthetic_fixture"


def test_replay_provider_enforces_declared_origin_for_response_objects() -> None:
    request = OracleGenerateRequest("r1", [{"role": "user", "content": "fixture"}])
    response = OracleGenerateResponse(
        raw_bytes=b'{"fixture":true}',
        status_code=200,
        headers={},
        provider_name="fixture",
        provider_model_id=None,
        content="synthetic",
    )
    provider = ReplayProvider(
        {request.request_hash: response},
        fixture_origin="synthetic_fixture",
    )

    result = asyncio.run(provider.generate(request))

    assert result.material_origin == "synthetic_fixture"


def test_repository_runtime_config_has_provider_independent_profiles() -> None:
    config = load_runtime_config("config")
    assert {provider.kind for provider in config.providers.values()} >= {
        "openrouter",
        "openai_compatible",
        "local_mlx",
    }
    assert config.provider_for(config.model("r1-initial-openrouter")).kind == "openrouter"
    assert config.tools.sandbox.backend == "docker"
    assert config.tools.sandbox.network is False
    assert config.agents.enabled is False
    assert config.agents.workers["codex"].allowed_environment_names == (
        "LANG",
        "LC_ALL",
        "TERM",
    )


def test_nested_spec_model_profile_format_is_supported(tmp_path) -> None:
    path = tmp_path / "profile.toml"
    path.write_text(
        """
[id]
name = "nested-r1"
[model]
slug = "deepseek/deepseek-r1"
provider = "openrouter"
[sampling]
temperature = 0.7
top_p = 0.9
[conversation]
system_prompt = ""
include_reasoning_in_next_turn = false
[routing]
pin_provider = "novita"
allow_fallback = false
""",
        encoding="utf-8",
    )
    profile = load_models(path)["nested-r1"]
    assert profile.slug == "deepseek/deepseek-r1"
    assert profile.temperature == 0.7
    assert profile.pin_provider == "novita"


def test_malformed_success_response_remains_available_for_raw_archive() -> None:
    raw = b'{"future_shape":"has no choices yet"}'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=raw)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenRouterProvider(
        ProviderConfig(
            id="openrouter",
            kind="openrouter",
            base_url="https://openrouter.test/v1",
        ),
        {"r1": _profile()},
        client=client,
        api_key="test",
    )
    with pytest.raises(ProviderHTTPError) as captured:
        asyncio.run(
            provider.generate(OracleGenerateRequest("r1", [{"role": "user", "content": "go"}]))
        )
    asyncio.run(client.aclose())
    assert captured.value.status_code == 200
    assert captured.value.raw_bytes == raw
