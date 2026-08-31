"""Typed, side-effect-free loading of Oracle Lab TOML configuration.

Configuration objects intentionally retain environment-variable *names*, never
resolved credential values.  Credentials are resolved at the provider boundary
so they cannot accidentally enter events, archives, or debug representations.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from oracle_lab.coding_isolation import SAFE_ISOLATED_ENVIRONMENT_NAMES


class ConfigError(ValueError):
    """Raised when a configuration file violates a runtime contract."""


ProviderKind = Literal["openrouter", "openai_compatible", "local_mlx", "replay"]
ToolApproval = Literal["auto", "ask", "deny"]
AgentAdapterKind = Literal["codex", "opencode", "direct"]
HostProviderKind = Literal["openai_compatible"]


def _table(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{path} must be a TOML table")
    return value


def _required_str(table: Mapping[str, Any], key: str, path: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path}.{key} must be a non-empty string")
    return value


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file does not exist: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    id: str
    kind: ProviderKind
    base_url: str
    api_key_env: str | None = None
    timeout_seconds: float = 120.0
    max_retries: int = 2
    retry_base_seconds: float = 1.0
    max_concurrency: int = 4
    requests_per_minute: int | None = None
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in {"openrouter", "openai_compatible", "local_mlx", "replay"}:
            raise ConfigError(f"unsupported provider kind: {self.kind}")
        if self.kind != "replay" and not self.base_url.startswith(("http://", "https://")):
            raise ConfigError(f"provider {self.id} base_url must use HTTP(S)")
        if self.timeout_seconds <= 0:
            raise ConfigError(f"provider {self.id} timeout_seconds must be positive")
        if self.max_retries < 0 or self.retry_base_seconds < 0:
            raise ConfigError(f"provider {self.id} retry settings must not be negative")
        # Keep a defensive copy without ``MappingProxyType``: callers commonly
        # serialize the frozen dataclass with ``dataclasses.asdict()``, whose
        # deepcopy step cannot pickle a mapping proxy.
        object.__setattr__(self, "headers", dict(self.headers))
        if self.max_concurrency < 1:
            raise ConfigError(f"provider {self.id} max_concurrency must be positive")
        if self.requests_per_minute is not None and self.requests_per_minute < 1:
            raise ConfigError(f"provider {self.id} requests_per_minute must be positive")

    @classmethod
    def from_mapping(cls, provider_id: str, value: Mapping[str, Any]) -> ProviderConfig:
        kind = _required_str(value, "kind", provider_id)
        api_key_env = value.get("api_key_env")
        if api_key_env is not None and not isinstance(api_key_env, str):
            raise ConfigError(f"{provider_id}.api_key_env must be a string")
        raw_headers = _table(value.get("headers", {}), f"{provider_id}.headers")
        headers = {str(key): str(item) for key, item in raw_headers.items()}
        return cls(
            id=provider_id,
            kind=kind,  # type: ignore[arg-type]
            base_url=str(value.get("base_url", "")),
            api_key_env=api_key_env,
            timeout_seconds=float(value.get("timeout_seconds", 120)),
            max_retries=int(value.get("max_retries", 2)),
            retry_base_seconds=float(value.get("retry_base_seconds", 1)),
            max_concurrency=int(value.get("max_concurrency", 4)),
            requests_per_minute=(
                None
                if value.get("requests_per_minute") is None
                else int(value["requests_per_minute"])
            ),
            headers=headers,
        )


@dataclass(frozen=True, slots=True)
class ModelProfile:
    id: str
    slug: str
    provider: str
    model_family: str = ""
    checkpoint: str = ""
    runtime: str = ""
    quantization: str = ""
    temperature: float = 0.6
    top_p: float = 0.95
    max_tokens: int | None = None
    max_context_messages: int | None = None
    system_prompt: str = ""
    include_reasoning_in_next_turn: bool = False
    pin_provider: str | None = None
    allow_fallback: bool = False

    def __post_init__(self) -> None:
        if not self.slug.strip() or not self.provider.strip():
            raise ConfigError(f"model {self.id} requires slug and provider")
        if not 0 <= self.temperature <= 2:
            raise ConfigError(f"model {self.id} temperature must be between 0 and 2")
        if not 0 < self.top_p <= 1:
            raise ConfigError(f"model {self.id} top_p must be in (0, 1]")
        if self.max_tokens is not None and self.max_tokens <= 0:
            raise ConfigError(f"model {self.id} max_tokens must be positive")
        if self.max_context_messages is not None and self.max_context_messages < 1:
            raise ConfigError(f"model {self.id} max_context_messages must be positive")

    @classmethod
    def from_mapping(cls, profile_id: str, value: Mapping[str, Any]) -> ModelProfile:
        model = _table(value.get("model", {}), f"{profile_id}.model")
        sampling = _table(value.get("sampling", {}), f"{profile_id}.sampling")
        conversation = _table(value.get("conversation", {}), f"{profile_id}.conversation")
        routing = _table(value.get("routing", {}), f"{profile_id}.routing")

        def configured(key: str, table: Mapping[str, Any], default: Any = None) -> Any:
            return value[key] if key in value else table.get(key, default)

        max_tokens = configured("max_tokens", sampling)
        max_context_messages = configured("max_context_messages", conversation)
        pin_provider = configured("pin_provider", routing)
        return cls(
            id=profile_id,
            slug=str(configured("slug", model, "")),
            provider=str(configured("provider", model, "")),
            model_family=str(configured("model_family", model, "")),
            checkpoint=str(configured("checkpoint", model, "")),
            runtime=str(configured("runtime", model, "")),
            quantization=str(configured("quantization", model, "")),
            temperature=float(configured("temperature", sampling, 0.6)),
            top_p=float(configured("top_p", sampling, 0.95)),
            max_tokens=None if max_tokens is None else int(max_tokens),
            max_context_messages=(
                None if max_context_messages is None else int(max_context_messages)
            ),
            system_prompt=str(configured("system_prompt", conversation, "")),
            include_reasoning_in_next_turn=bool(
                configured("include_reasoning_in_next_turn", conversation, False)
            ),
            pin_provider=None if pin_provider is None else str(pin_provider),
            allow_fallback=bool(configured("allow_fallback", routing, False)),
        )


@dataclass(frozen=True, slots=True)
class SandboxConfig:
    backend: Literal["docker"] = "docker"
    image: str = "python:3.13-alpine"
    network: bool = False
    read_only_root: bool = True
    timeout_ms: int = 5_000
    memory_mb: int = 256
    cpus: float = 1.0
    pids_limit: int = 64
    max_output_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        if self.backend != "docker":
            raise ConfigError("real shell backend must be docker; host fallback is forbidden")
        if self.network:
            raise ConfigError("sandbox network must be disabled by default")
        if not self.read_only_root:
            raise ConfigError("sandbox root filesystem must be read-only")
        numeric = {
            "timeout_ms": self.timeout_ms,
            "memory_mb": self.memory_mb,
            "cpus": self.cpus,
            "pids_limit": self.pids_limit,
            "max_output_bytes": self.max_output_bytes,
        }
        if any(value <= 0 for value in numeric.values()):
            raise ConfigError("sandbox limits must all be positive")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SandboxConfig:
        return cls(
            backend=str(value.get("backend", "docker")),  # type: ignore[arg-type]
            image=str(value.get("image", "python:3.13-alpine")),
            network=bool(value.get("network", False)),
            read_only_root=bool(value.get("read_only_root", True)),
            timeout_ms=int(value.get("timeout_ms", 5_000)),
            memory_mb=int(value.get("memory_mb", 256)),
            cpus=float(value.get("cpus", 1.0)),
            pids_limit=int(value.get("pids_limit", 64)),
            max_output_bytes=int(value.get("max_output_bytes", 1_048_576)),
        )


@dataclass(frozen=True, slots=True)
class ToolConfig:
    sandbox: SandboxConfig
    allowed_virtual_commands: tuple[str, ...]
    verification_allowed_hosts: tuple[str, ...] = ()
    verification_max_output_bytes: int = 1_048_576


@dataclass(frozen=True, slots=True)
class AgentWorkerConfig:
    id: str
    enabled: bool
    adapter: AgentAdapterKind
    executable: str
    model: str | None
    timeout_seconds: float
    max_output_bytes: int
    sandbox_profile: str
    allowed_environment_names: tuple[str, ...]
    fallback_adapter: str | None = None
    max_retries: int = 0
    validation_commands: tuple[str, ...] = ()
    host_provider_kind: HostProviderKind | None = None
    host_provider_id: str | None = None
    host_base_url: str | None = None
    host_api_key_env: str | None = None
    host_temperature: float | None = None
    host_top_p: float | None = None
    host_max_tokens: int | None = None
    host_allow_fallback: bool = False
    isolation_template_reference: str | None = None
    isolation_allowed_hosts: tuple[str, ...] = ()
    max_workspace_export_bytes: int = 64 * 1024 * 1024
    max_workspace_entries: int = 100_000

    def __post_init__(self) -> None:
        if self.adapter not in {"codex", "opencode", "direct"}:
            raise ConfigError(f"unsupported worker adapter: {self.adapter}")
        if not self.executable.strip():
            raise ConfigError(f"worker {self.id} executable must not be blank")
        if self.timeout_seconds <= 0 or self.max_output_bytes <= 0:
            raise ConfigError(f"worker {self.id} limits must be positive")
        if self.max_retries < 0:
            raise ConfigError(f"worker {self.id} max_retries must not be negative")
        if not self.sandbox_profile.strip():
            raise ConfigError(f"worker {self.id} sandbox_profile must not be blank")
        invalid_names = [
            name
            for name in self.allowed_environment_names
            if not name or not name.replace("_", "A").isalnum() or name[0].isdigit()
        ]
        if invalid_names:
            raise ConfigError(f"worker {self.id} has invalid environment names: {invalid_names}")
        if len(set(self.allowed_environment_names)) != len(self.allowed_environment_names):
            raise ConfigError(f"worker {self.id} environment names must be unique")
        if self.adapter in {"codex", "opencode"} and self.sandbox_profile == "external-broker":
            unsafe_environment = sorted(
                set(self.allowed_environment_names) - SAFE_ISOLATED_ENVIRONMENT_NAMES
            )
            if unsafe_environment:
                raise ConfigError(
                    f"worker {self.id} external broker environment contains unsafe names: "
                    f"{unsafe_environment}"
                )
            if self.enabled and self.isolation_template_reference is None:
                raise ConfigError(
                    f"enabled worker {self.id} external broker requires a pinned "
                    "isolation_template_reference"
                )
        if any(not command.strip() for command in self.validation_commands):
            raise ConfigError(f"worker {self.id} validation commands must not be blank")
        if self.max_workspace_export_bytes <= 0 or self.max_workspace_entries <= 0:
            raise ConfigError(f"worker {self.id} workspace export limits must be positive")
        normalized_hosts = tuple(host.lower().rstrip(".") for host in self.isolation_allowed_hosts)
        invalid_hosts = [
            host
            for host in normalized_hosts
            if not host
            or len(host) > 253
            or "*" in host
            or "/" in host
            or ":" in host
            or host.startswith(".")
            or any(
                re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None
                for label in host.split(".")
            )
        ]
        if invalid_hosts:
            raise ConfigError(
                f"worker {self.id} isolation_allowed_hosts must be exact DNS names: {invalid_hosts}"
            )
        if len(set(normalized_hosts)) != len(normalized_hosts):
            raise ConfigError(f"worker {self.id} isolation_allowed_hosts must be unique")
        if self.isolation_template_reference is not None:
            digest = self.isolation_template_reference.rsplit("@sha256:", 1)
            if len(digest) != 2 or re.fullmatch(r"[0-9a-f]{64}", digest[1]) is None:
                raise ConfigError(
                    f"worker {self.id} isolation_template_reference must pin a lowercase "
                    "64-character sha256 digest"
                )
        object.__setattr__(self, "isolation_allowed_hosts", normalized_hosts)
        if self.adapter == "direct" and self.enabled:
            required = {
                "model": self.model,
                "host_provider_kind": self.host_provider_kind,
                "host_provider_id": self.host_provider_id,
                "host_base_url": self.host_base_url,
            }
            missing = [key for key, value in required.items() if not str(value or "").strip()]
            if missing:
                raise ConfigError(
                    f"enabled direct worker {self.id} requires explicit Host settings: {missing}"
                )
        if self.host_provider_kind not in {None, "openai_compatible"}:
            raise ConfigError(
                f"worker {self.id} has unsupported host_provider_kind: {self.host_provider_kind}"
            )
        if self.host_base_url is not None and not self.host_base_url.startswith(
            ("http://", "https://")
        ):
            raise ConfigError(f"worker {self.id} host_base_url must use HTTP(S)")
        if self.host_base_url is not None:
            parsed_host_url = urlsplit(self.host_base_url)
            if (
                not parsed_host_url.hostname
                or parsed_host_url.username is not None
                or parsed_host_url.password is not None
                or parsed_host_url.query
                or parsed_host_url.fragment
            ):
                raise ConfigError(
                    f"worker {self.id} host_base_url must not contain credentials, query, "
                    "or fragment"
                )
        if self.host_api_key_env is not None and (
            not self.host_api_key_env
            or not self.host_api_key_env.replace("_", "A").isalnum()
            or self.host_api_key_env[0].isdigit()
        ):
            raise ConfigError(f"worker {self.id} host_api_key_env must be an environment name")
        if self.host_temperature is not None and not 0 <= self.host_temperature <= 2:
            raise ConfigError(f"worker {self.id} host_temperature must be between 0 and 2")
        if self.host_top_p is not None and not 0 < self.host_top_p <= 1:
            raise ConfigError(f"worker {self.id} host_top_p must be in (0, 1]")
        if self.host_max_tokens is not None and self.host_max_tokens <= 0:
            raise ConfigError(f"worker {self.id} host_max_tokens must be positive")
        if self.adapter != "direct" and (
            self.host_allow_fallback
            or any(
                value is not None
                for value in (
                    self.host_provider_kind,
                    self.host_provider_id,
                    self.host_base_url,
                    self.host_api_key_env,
                    self.host_temperature,
                    self.host_top_p,
                    self.host_max_tokens,
                )
            )
        ):
            raise ConfigError(f"worker {self.id} Host provider settings require adapter=direct")

    @classmethod
    def from_mapping(cls, worker_id: str, value: Mapping[str, Any]) -> AgentWorkerConfig:
        environment_names = value.get("allowed_environment_names", ())
        if not isinstance(environment_names, list) or not all(
            isinstance(name, str) for name in environment_names
        ):
            raise ConfigError(
                f"workers.{worker_id}.allowed_environment_names must be an array of strings"
            )
        validation_commands = value.get("validation_commands", ())
        if not isinstance(validation_commands, list) or not all(
            isinstance(command, str) for command in validation_commands
        ):
            raise ConfigError(
                f"workers.{worker_id}.validation_commands must be an array of strings"
            )
        model = value.get("model")
        fallback = value.get("fallback_adapter")
        host_provider_kind = value.get("host_provider_kind")
        host_provider_id = value.get("host_provider_id")
        host_base_url = value.get("host_base_url")
        host_api_key_env = value.get("host_api_key_env")
        host_temperature = value.get("host_temperature")
        host_top_p = value.get("host_top_p")
        host_max_tokens = value.get("host_max_tokens")
        isolation_template_reference = value.get("isolation_template_reference")
        isolation_allowed_hosts = value.get("isolation_allowed_hosts", [])
        if not isinstance(isolation_allowed_hosts, list) or not all(
            isinstance(host, str) for host in isolation_allowed_hosts
        ):
            raise ConfigError(
                f"workers.{worker_id}.isolation_allowed_hosts must be an array of strings"
            )
        return cls(
            id=worker_id,
            enabled=bool(value.get("enabled", False)),
            adapter=str(value.get("adapter", worker_id)),  # type: ignore[arg-type]
            executable=str(value.get("executable", worker_id)),
            model=None if model is None else str(model),
            timeout_seconds=float(value.get("timeout_seconds", 300)),
            max_output_bytes=int(value.get("max_output_bytes", 4 * 1024 * 1024)),
            sandbox_profile=str(value.get("sandbox_profile", "workspace-write")),
            allowed_environment_names=tuple(environment_names),
            fallback_adapter=None if fallback is None else str(fallback),
            max_retries=int(value.get("max_retries", 0)),
            validation_commands=tuple(validation_commands),
            host_provider_kind=(
                None if host_provider_kind is None else str(host_provider_kind)  # type: ignore[arg-type]
            ),
            host_provider_id=(None if host_provider_id is None else str(host_provider_id)),
            host_base_url=(None if host_base_url is None else str(host_base_url)),
            host_api_key_env=(None if host_api_key_env is None else str(host_api_key_env)),
            host_temperature=(None if host_temperature is None else float(host_temperature)),
            host_top_p=None if host_top_p is None else float(host_top_p),
            host_max_tokens=None if host_max_tokens is None else int(host_max_tokens),
            host_allow_fallback=bool(value.get("host_allow_fallback", False)),
            isolation_template_reference=(
                None
                if isolation_template_reference is None
                or not str(isolation_template_reference).strip()
                else str(isolation_template_reference)
            ),
            isolation_allowed_hosts=tuple(isolation_allowed_hosts),
            max_workspace_export_bytes=int(
                value.get("max_workspace_export_bytes", 64 * 1024 * 1024)
            ),
            max_workspace_entries=int(value.get("max_workspace_entries", 100_000)),
        )


@dataclass(frozen=True, slots=True)
class AgentRuntimeConfig:
    enabled: bool = False
    prefer_coding_agent: str = "codex"
    workers: Mapping[str, AgentWorkerConfig] = field(default_factory=dict)
    isolation_backend: str = "disabled"
    isolation_broker_executable: str = "sbx"

    def __post_init__(self) -> None:
        if self.prefer_coding_agent not in {"codex", "opencode"}:
            raise ConfigError("agents prefer_coding_agent must be codex or opencode")
        if self.isolation_backend not in {"disabled", "docker-sbx-microvm"}:
            raise ConfigError(
                f"unsupported coding-worker isolation backend: {self.isolation_backend}"
            )
        if not self.isolation_broker_executable.strip():
            raise ConfigError("coding-worker isolation broker executable must not be blank")
        values = dict(self.workers)
        for worker in values.values():
            fallback = worker.fallback_adapter
            if fallback is not None and fallback not in values:
                raise ConfigError(
                    f"worker {worker.id} references unknown fallback_adapter {fallback}"
                )
            if fallback == worker.id:
                raise ConfigError(f"worker {worker.id} may not fall back to itself")
        object.__setattr__(self, "workers", values)


@dataclass(frozen=True, slots=True)
class PolicyConfig:
    auto_continue_after_tool_result: bool = True
    max_auto_depth: int = 4
    max_auto_budget: int = 16
    analysis: Mapping[str, bool] = field(default_factory=dict)
    human_gate: Mapping[str, bool] = field(default_factory=dict)
    tools: Mapping[str, ToolApproval] = field(default_factory=dict)
    hard_limit_usd_per_day: float | None = None
    warn_limit_usd_per_session: float | None = None

    def __post_init__(self) -> None:
        if self.max_auto_depth < 0:
            raise ConfigError("oracle.max_auto_depth must not be negative")
        if self.max_auto_budget < 1:
            raise ConfigError("oracle.max_auto_budget must be positive")
        invalid = {value for value in self.tools.values() if value not in {"auto", "ask", "deny"}}
        if invalid:
            raise ConfigError(f"invalid tool approval modes: {sorted(invalid)}")
        object.__setattr__(self, "analysis", dict(self.analysis))
        object.__setattr__(self, "human_gate", dict(self.human_gate))
        object.__setattr__(self, "tools", dict(self.tools))


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    providers: Mapping[str, ProviderConfig]
    models: Mapping[str, ModelProfile]
    policies: PolicyConfig
    tools: ToolConfig
    agents: AgentRuntimeConfig = field(default_factory=AgentRuntimeConfig)

    def __post_init__(self) -> None:
        object.__setattr__(self, "providers", dict(self.providers))
        object.__setattr__(self, "models", dict(self.models))

    def model(self, profile_id: str) -> ModelProfile:
        try:
            return self.models[profile_id]
        except KeyError as exc:
            raise ConfigError(f"unknown model profile: {profile_id}") from exc

    def provider_for(self, profile: ModelProfile) -> ProviderConfig:
        try:
            return self.providers[profile.provider]
        except KeyError as exc:
            raise ConfigError(
                f"model profile {profile.id} references unknown provider {profile.provider}"
            ) from exc


def load_providers(path: str | Path) -> dict[str, ProviderConfig]:
    raw = _read_toml(Path(path))
    return {
        provider_id: ProviderConfig.from_mapping(provider_id, _table(value, provider_id))
        for provider_id, value in raw.items()
    }


def load_models(path: str | Path) -> dict[str, ModelProfile]:
    raw = _read_toml(Path(path))
    if "model" in raw and isinstance(raw.get("model"), Mapping):
        identity = _table(raw.get("id", {}), "id")
        profile_id = str(identity.get("name") or Path(path).stem)
        return {profile_id: ModelProfile.from_mapping(profile_id, raw)}
    return {
        profile_id: ModelProfile.from_mapping(profile_id, _table(value, profile_id))
        for profile_id, value in raw.items()
    }


def load_policies(path: str | Path) -> PolicyConfig:
    raw = _read_toml(Path(path))
    oracle = _table(raw.get("oracle", {}), "oracle")
    analysis = _table(raw.get("analysis", {}), "analysis")
    human_gate = _table(raw.get("human_gate", {}), "human_gate")
    tools = _table(raw.get("tools", {}), "tools")
    cost = _table(raw.get("cost", {}), "cost")
    hard_limit = cost.get("hard_limit_usd_per_day")
    warn_limit = cost.get("warn_limit_usd_per_session")
    return PolicyConfig(
        auto_continue_after_tool_result=bool(oracle.get("auto_continue_after_tool_result", True)),
        max_auto_depth=int(oracle.get("max_auto_depth", 4)),
        max_auto_budget=int(oracle.get("max_auto_budget", oracle.get("max_auto_events", 16))),
        analysis={str(key): bool(value) for key, value in analysis.items()},
        human_gate={str(key): bool(value) for key, value in human_gate.items()},
        tools={str(key): str(value) for key, value in tools.items()},  # type: ignore[arg-type]
        hard_limit_usd_per_day=None if hard_limit is None else float(hard_limit),
        warn_limit_usd_per_session=None if warn_limit is None else float(warn_limit),
    )


def load_tools(path: str | Path) -> ToolConfig:
    raw = _read_toml(Path(path))
    sandbox = SandboxConfig.from_mapping(_table(raw.get("sandbox", {}), "sandbox"))
    virtual = _table(raw.get("virtual", {}), "virtual")
    commands = virtual.get("allowed_commands", ())
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ConfigError("virtual.allowed_commands must be an array of strings")
    verification = _table(raw.get("verification", {}), "verification")
    allowed_hosts = verification.get("allowed_hosts", ())
    if not isinstance(allowed_hosts, list) or not all(
        isinstance(host, str) and host.strip() for host in allowed_hosts
    ):
        raise ConfigError("verification.allowed_hosts must be an array of host names")
    max_output_bytes = int(verification.get("max_output_bytes", 1_048_576))
    if max_output_bytes <= 0:
        raise ConfigError("verification.max_output_bytes must be positive")
    return ToolConfig(
        sandbox=sandbox,
        allowed_virtual_commands=tuple(commands),
        verification_allowed_hosts=tuple(host.lower().rstrip(".") for host in allowed_hosts),
        verification_max_output_bytes=max_output_bytes,
    )


def load_agents(path: str | Path) -> AgentRuntimeConfig:
    config_path = Path(path)
    if not config_path.exists():
        return AgentRuntimeConfig()
    raw = _read_toml(config_path)
    router = _table(raw.get("router", {}), "router")
    workers = _table(raw.get("workers", {}), "workers")
    return AgentRuntimeConfig(
        enabled=bool(router.get("enabled", False)),
        prefer_coding_agent=str(router.get("prefer_coding_agent", "codex")),
        isolation_backend=str(router.get("isolation_backend", "disabled")),
        isolation_broker_executable=str(router.get("isolation_broker_executable", "sbx")),
        workers={
            str(worker_id): AgentWorkerConfig.from_mapping(
                str(worker_id),
                _table(value, f"workers.{worker_id}"),
            )
            for worker_id, value in workers.items()
        },
    )


def load_runtime_config(config_dir: str | Path) -> RuntimeConfig:
    root = Path(config_dir)
    providers = load_providers(root / "providers.toml")
    models = load_models(root / "models.toml")
    config = RuntimeConfig(
        providers=providers,
        models=models,
        policies=load_policies(root / "policies.toml"),
        tools=load_tools(root / "tools.toml"),
        agents=load_agents(root / "agents.toml"),
    )
    for profile in models.values():
        config.provider_for(profile)
    return config
