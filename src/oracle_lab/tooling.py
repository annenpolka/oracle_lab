"""Mandatory tool broker with a safe calculator and Docker-only real shell."""

from __future__ import annotations

import ast
import base64
import csv
import hashlib
import io
import ipaddress
import json
import math
import os
import re
import selectors
import shlex
import shutil
import socket
import subprocess
import time
import tomllib
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, ClassVar, Protocol

import httpx

from oracle_lab.config import SandboxConfig, ToolApproval
from oracle_lab.events import Actor, ActorKind, Event, EventType
from oracle_lab.ids import new_id
from oracle_lab.jsonutil import canonical_json
from oracle_lab.virtual import SourceEvidence, VirtualWorldRuntime


class ToolPolicyError(ValueError):
    pass


def _freeze_tool_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_tool_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_tool_value(item) for item in value)
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def _thaw_tool_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_tool_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_tool_value(item) for item in value]
    return value


class ToolExecution(StrEnum):
    REAL_DETERMINISTIC = "real_deterministic"
    REAL_SANDBOX = "real_sandbox"
    VIRTUAL = "virtual"
    VERIFICATION = "verification"


class ToolStatus(StrEnum):
    OK = "ok"
    ERROR = "error"
    DENIED = "denied"
    PENDING_APPROVAL = "pending_approval"
    TIMEOUT = "timeout"
    OUTPUT_LIMIT = "output_limit"


class TruthDomain(StrEnum):
    REAL = "real"
    SANDBOX = "sandbox"
    VIRTUAL = "virtual"
    RETRIEVED = "retrieved"
    SYNTHETIC = "synthetic"


def _truth_domain_for(execution: ToolExecution) -> TruthDomain:
    return {
        ToolExecution.REAL_DETERMINISTIC: TruthDomain.REAL,
        ToolExecution.REAL_SANDBOX: TruthDomain.SANDBOX,
        ToolExecution.VIRTUAL: TruthDomain.VIRTUAL,
        ToolExecution.VERIFICATION: TruthDomain.RETRIEVED,
    }[execution]


@dataclass(frozen=True, slots=True)
class ToolRequest:
    tool: str
    execution: ToolExecution | str
    input: Mapping[str, Any]
    source_event_id: str
    resume_oracle: bool = False
    timeout_ms: int = 5_000
    id: str = field(default_factory=lambda: new_id("tlr"))

    def __post_init__(self) -> None:
        if not self.tool or not self.source_event_id:
            raise ToolPolicyError("tool and source_event_id are required")
        try:
            execution = ToolExecution(self.execution)
        except ValueError as exc:
            raise ToolPolicyError(f"unknown execution class: {self.execution}") from exc
        if self.timeout_ms <= 0:
            raise ToolPolicyError("timeout_ms must be positive")
        object.__setattr__(self, "execution", execution)
        object.__setattr__(self, "input", _freeze_tool_value(self.input))

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any], *, source_event_id: str | None = None
    ) -> ToolRequest:
        raw_source = source_event_id or value.get("source_event_id")
        if not isinstance(raw_source, str):
            raise ToolPolicyError("source_event_id is required")
        raw_input = value.get("input", {})
        if not isinstance(raw_input, Mapping):
            raise ToolPolicyError("tool input must be an object")
        return cls(
            tool=str(value.get("tool", "")),
            execution=str(value.get("execution", "")),
            input=raw_input,
            source_event_id=raw_source,
            resume_oracle=bool(value.get("resume_oracle", False)),
            timeout_ms=int(value.get("timeout_ms", 5_000)),
            id=str(value.get("id") or new_id("tlr")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tool": self.tool,
            "execution": self.execution.value,
            "input": _thaw_tool_value(self.input),
            "source_event_id": self.source_event_id,
            "resume_oracle": self.resume_oracle,
            "timeout_ms": self.timeout_ms,
        }


@dataclass(frozen=True, slots=True)
class ToolResult:
    request_id: str
    status: ToolStatus
    output: str = ""
    error: str | None = None
    exit_code: int | None = None
    elapsed_ms: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)
    raw_stdout: bytes = b""
    raw_stderr: bytes = b""

    def __post_init__(self) -> None:
        domain = self.metadata.get("truth_domain")
        if domain is not None:
            try:
                TruthDomain(str(domain))
            except ValueError as exc:
                raise ToolPolicyError(f"invalid tool truth domain: {domain}") from exc
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        object.__setattr__(self, "raw_stdout", bytes(self.raw_stdout))
        object.__setattr__(self, "raw_stderr", bytes(self.raw_stderr))

    def to_event(
        self,
        request: ToolRequest,
        *,
        session_id: str | None = None,
        branch_id: str | None = None,
        correlation_id: str | None = None,
        parent_event_id: str | None = None,
    ) -> Event:
        if self.status == ToolStatus.PENDING_APPROVAL:
            raise ToolPolicyError("pending approval is not a denial/result event")
        event_type = {
            ToolStatus.OK: EventType.TOOL_OUTPUT,
            ToolStatus.TIMEOUT: EventType.TOOL_TIMEOUT,
            ToolStatus.DENIED: EventType.TOOL_DENIED,
        }.get(self.status, EventType.TOOL_ERROR)
        result_metadata = dict(self.metadata)
        result_metadata.setdefault("truth_domain", _truth_domain_for(request.execution).value)
        return Event.new(
            event_type,
            actor=Actor(kind=ActorKind.TOOL, id=request.tool),
            session_id=session_id,
            branch_id=branch_id,
            parent_event_id=parent_event_id,
            causation_id=request.source_event_id,
            correlation_id=correlation_id,
            payload={
                "request_id": request.id,
                "status": self.status.value,
                "output": self.output,
                "error": self.error,
                "exit_code": self.exit_code,
                "elapsed_ms": self.elapsed_ms,
                **result_metadata,
            },
            metadata={
                "schema_version": 1,
                "resume_oracle": request.resume_oracle,
                "truth_domain": result_metadata["truth_domain"],
            },
        )


@dataclass(frozen=True, slots=True)
class ToolAuditRecord:
    request_id: str
    source_event_id: str
    tool: str
    execution: str
    status: str
    started_at: float
    elapsed_ms: float
    truth_domain: str | None = None
    command: str | None = None
    detail: str | None = None


AuditSink = Callable[[ToolAuditRecord], None]
VirtualOperationPreparer = Callable[[str], None]


class SafeCalculator:
    """Arithmetic expression evaluator with no names, calls, or object access."""

    _binary: ClassVar[Mapping[type[ast.operator], Callable[[Any, Any], Any]]] = {
        ast.Add: lambda left, right: left + right,
        ast.Sub: lambda left, right: left - right,
        ast.Mult: lambda left, right: left * right,
        ast.Div: lambda left, right: left / right,
        ast.FloorDiv: lambda left, right: left // right,
        ast.Mod: lambda left, right: left % right,
        ast.Pow: lambda left, right: left**right,
    }
    _unary: ClassVar[Mapping[type[ast.unaryop], Callable[[Any], Any]]] = {
        ast.UAdd: lambda value: value,
        ast.USub: lambda value: -value,
    }

    def __init__(self, *, max_expression_length: int = 2_000, max_nodes: int = 256) -> None:
        self.max_expression_length = max_expression_length
        self.max_nodes = max_nodes

    def evaluate(self, expression: str) -> int | float:
        if not expression or len(expression) > self.max_expression_length:
            raise ToolPolicyError("calculator expression is empty or too long")
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            raise ToolPolicyError("invalid calculator expression") from exc
        if sum(1 for _ in ast.walk(tree)) > self.max_nodes:
            raise ToolPolicyError("calculator expression is too complex")
        value = self._eval(tree.body)
        if isinstance(value, float) and not math.isfinite(value):
            raise ToolPolicyError("calculator result must be finite")
        if abs(value) > 10**100:
            raise ToolPolicyError("calculator result exceeds magnitude limit")
        return value

    def _eval(self, node: ast.AST) -> int | float:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ToolPolicyError("calculator accepts numbers only")
            return node.value
        if isinstance(node, ast.UnaryOp) and type(node.op) in self._unary:
            return self._unary[type(node.op)](self._eval(node.operand))
        if isinstance(node, ast.BinOp) and type(node.op) in self._binary:
            left = self._eval(node.left)
            right = self._eval(node.right)
            if isinstance(node.op, ast.Pow) and (abs(right) > 100 or abs(left) > 10**6):
                raise ToolPolicyError("calculator exponent is outside safety limits")
            try:
                return self._binary[type(node.op)](left, right)
            except (ArithmeticError, OverflowError) as exc:
                raise ToolPolicyError(
                    f"calculator arithmetic failed: {type(exc).__name__}"
                ) from exc
        raise ToolPolicyError(f"calculator syntax is forbidden: {type(node).__name__}")


class SafeDeterministicTools:
    """Bounded, side-effect-free conversion, text, checksum, and parsing tools."""

    _unit_factors: ClassVar[Mapping[str, tuple[str, Decimal]]] = MappingProxyType(
        {
            "ms": ("time", Decimal("0.001")),
            "s": ("time", Decimal("1")),
            "min": ("time", Decimal("60")),
            "h": ("time", Decimal("3600")),
            "day": ("time", Decimal("86400")),
            "mm": ("length", Decimal("0.001")),
            "cm": ("length", Decimal("0.01")),
            "m": ("length", Decimal("1")),
            "km": ("length", Decimal("1000")),
            "in": ("length", Decimal("0.0254")),
            "ft": ("length", Decimal("0.3048")),
            "mg": ("mass", Decimal("0.000001")),
            "g": ("mass", Decimal("0.001")),
            "kg": ("mass", Decimal("1")),
            "lb": ("mass", Decimal("0.45359237")),
        }
    )
    _unit_aliases: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "millisecond": "ms",
            "milliseconds": "ms",
            "second": "s",
            "seconds": "s",
            "minute": "min",
            "minutes": "min",
            "hour": "h",
            "hours": "h",
            "days": "day",
            "meter": "m",
            "meters": "m",
            "metre": "m",
            "metres": "m",
            "kilometer": "km",
            "kilometers": "km",
            "kilometre": "km",
            "kilometres": "km",
            "gram": "g",
            "grams": "g",
            "kilogram": "kg",
            "kilograms": "kg",
        }
    )

    def unit_convert(self, values: Mapping[str, Any]) -> str:
        raw_value = values.get("value")
        if isinstance(raw_value, bool):
            raise ToolPolicyError("unit conversion value must be numeric")
        try:
            value = Decimal(str(raw_value))
        except (InvalidOperation, ValueError) as exc:
            raise ToolPolicyError("unit conversion value must be numeric") from exc
        if not value.is_finite():
            raise ToolPolicyError("unit conversion value must be finite")
        source = self._unit(str(values.get("from_unit", "")))
        target = self._unit(str(values.get("to_unit", "")))
        source_dimension, source_factor = self._unit_factors[source]
        target_dimension, target_factor = self._unit_factors[target]
        if source_dimension != target_dimension:
            raise ToolPolicyError("unit conversion dimensions do not match")
        converted = value * source_factor / target_factor
        return canonical_json(
            {
                "from_unit": source,
                "input": self._decimal_text(value),
                "to_unit": target,
                "value": self._decimal_text(converted),
            }
        )

    def regex(self, values: Mapping[str, Any]) -> str:
        text = values.get("text")
        pattern = values.get("pattern")
        operation = str(values.get("operation", "findall"))
        if not isinstance(text, str) or not isinstance(pattern, str):
            raise ToolPolicyError("regex requires text and pattern strings")
        if len(text.encode("utf-8")) > 1_000_000 or len(pattern) > 1_000:
            raise ToolPolicyError("regex input exceeds deterministic limits")
        # Keep execution in-process only for a deliberately small safe subset:
        # no grouping, lookaround, backreferences, or counted/nested repeats.
        if any(token in pattern for token in ("(", ")", "{", "}")) or re.search(
            r"\\[1-9]", pattern
        ):
            raise ToolPolicyError("regex pattern uses unsupported complex constructs")
        try:
            expression = re.compile(pattern)
        except re.error as exc:
            raise ToolPolicyError(f"invalid regex pattern: {exc}") from exc
        if operation == "findall":
            result: Any = expression.findall(text)
        elif operation == "search":
            match = expression.search(text)
            result = (
                None if match is None else {"match": match.group(0), "span": list(match.span())}
            )
        elif operation == "split":
            result = expression.split(text, maxsplit=int(values.get("maxsplit", 0)))
        elif operation == "sub":
            replacement = values.get("replacement")
            if not isinstance(replacement, str):
                raise ToolPolicyError("regex sub requires a replacement string")
            result = expression.sub(replacement, text, count=int(values.get("count", 0)))
        else:
            raise ToolPolicyError(f"unsupported regex operation: {operation}")
        return canonical_json({"operation": operation, "result": result})

    def checksum(self, values: Mapping[str, Any]) -> str:
        algorithm = str(values.get("algorithm", "sha256")).lower()
        if algorithm not in {"sha256", "sha512", "blake2b"}:
            raise ToolPolicyError(f"unsupported checksum algorithm: {algorithm}")
        content = values.get("content")
        encoding = str(values.get("encoding", "utf-8"))
        if not isinstance(content, str):
            raise ToolPolicyError("checksum content must be a string")
        if encoding == "utf-8":
            raw = content.encode("utf-8")
        elif encoding == "base64":
            try:
                raw = base64.b64decode(content, validate=True)
            except ValueError as exc:
                raise ToolPolicyError("checksum content is not valid base64") from exc
        else:
            raise ToolPolicyError("checksum encoding must be utf-8 or base64")
        if len(raw) > 8 * 1024 * 1024:
            raise ToolPolicyError("checksum content exceeds deterministic limits")
        digest = hashlib.new(algorithm, raw).hexdigest()
        return canonical_json({"algorithm": algorithm, "bytes": len(raw), "digest": digest})

    def file_parse(self, values: Mapping[str, Any]) -> str:
        content = values.get("content")
        file_format = str(values.get("format", "json")).lower()
        if not isinstance(content, str):
            raise ToolPolicyError("file parsing requires explicit textual content")
        if len(content.encode("utf-8")) > 1_000_000:
            raise ToolPolicyError("file content exceeds deterministic limits")
        try:
            if file_format == "json":
                parsed: Any = json.loads(content)
            elif file_format == "toml":
                parsed = tomllib.loads(content)
            elif file_format == "csv":
                parsed = list(csv.DictReader(io.StringIO(content)))
            elif file_format == "text":
                parsed = {"lines": content.splitlines(), "trailing_newline": content.endswith("\n")}
            else:
                raise ToolPolicyError(f"unsupported file format: {file_format}")
        except (csv.Error, json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
            raise ToolPolicyError(f"invalid {file_format} content: {exc}") from exc
        return canonical_json({"format": file_format, "parsed": parsed})

    def _unit(self, value: str) -> str:
        normalized = value.strip().lower()
        normalized = self._unit_aliases.get(normalized, normalized)
        if normalized not in self._unit_factors:
            raise ToolPolicyError(f"unsupported unit: {value}")
        return normalized

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        normalized = value.normalize()
        return format(normalized, "f") if normalized else "0"


AddressResolver = Callable[[str], Iterable[str]]


class HttpVerificationTool:
    """Explicit, allowlisted HTTPS retrieval with no ambient credentials."""

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str],
        max_output_bytes: int = 1_048_576,
        resolver: AddressResolver | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.allowed_hosts = frozenset(host.lower().rstrip(".") for host in allowed_hosts)
        if not self.allowed_hosts or any(not host for host in self.allowed_hosts):
            raise ToolPolicyError("verification requires at least one explicit allowed host")
        if max_output_bytes <= 0:
            raise ToolPolicyError("verification output limit must be positive")
        self.max_output_bytes = max_output_bytes
        self.resolver = resolver or self._resolve_addresses
        self.client = client

    def fetch(self, request: ToolRequest) -> ToolResult:
        started = time.monotonic()
        raw_url = request.input.get("url")
        if not isinstance(raw_url, str):
            return ToolResult(request.id, ToolStatus.ERROR, error="verification URL is required")
        try:
            url = self._validate_url(raw_url)
        except ToolPolicyError as exc:
            return ToolResult(request.id, ToolStatus.DENIED, error=str(exc))
        owns_client = self.client is None
        client = self.client or httpx.Client(
            timeout=request.timeout_ms / 1000,
            follow_redirects=False,
            trust_env=False,
        )
        try:
            response = client.get(
                url,
                headers={"accept": "text/plain, application/json;q=0.9, */*;q=0.1"},
                follow_redirects=False,
                timeout=request.timeout_ms / 1000,
            )
            raw = bytes(response.content)
        except httpx.HTTPError as exc:
            return ToolResult(
                request.id,
                ToolStatus.ERROR,
                error=f"verification transport failed: {type(exc).__name__}",
                elapsed_ms=(time.monotonic() - started) * 1000,
                metadata={"url": url},
            )
        finally:
            if owns_client:
                client.close()
        metadata = {
            "url": url,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        }
        elapsed_ms = (time.monotonic() - started) * 1000
        if 300 <= response.status_code < 400:
            return ToolResult(
                request.id,
                ToolStatus.DENIED,
                error="verification redirects are disabled",
                elapsed_ms=elapsed_ms,
                metadata=metadata,
            )
        if len(raw) > self.max_output_bytes:
            return ToolResult(
                request.id,
                ToolStatus.OUTPUT_LIMIT,
                output=raw[: self.max_output_bytes].decode("utf-8", "replace"),
                error="verification output limit exceeded",
                elapsed_ms=elapsed_ms,
                metadata=metadata,
            )
        return ToolResult(
            request.id,
            ToolStatus.OK if 200 <= response.status_code < 300 else ToolStatus.ERROR,
            output=raw.decode("utf-8", "replace"),
            error=(
                None
                if 200 <= response.status_code < 300
                else f"verification returned HTTP {response.status_code}"
            ),
            elapsed_ms=elapsed_ms,
            metadata=metadata,
        )

    def _validate_url(self, raw_url: str) -> str:
        parsed = urllib.parse.urlsplit(raw_url)
        if parsed.scheme.lower() != "https":
            raise ToolPolicyError("verification permits HTTPS URLs only")
        if parsed.username is not None or parsed.password is not None:
            raise ToolPolicyError("verification URL credentials are forbidden")
        host = (parsed.hostname or "").lower().rstrip(".")
        if host not in self.allowed_hosts:
            raise ToolPolicyError(f"verification host is not allowlisted: {host or 'missing'}")
        if parsed.port not in {None, 443}:
            raise ToolPolicyError("verification permits HTTPS port 443 only")
        addresses = tuple(self.resolver(host))
        if not addresses:
            raise ToolPolicyError("verification host did not resolve")
        for address in addresses:
            try:
                resolved = ipaddress.ip_address(address)
            except ValueError as exc:
                raise ToolPolicyError("verification resolver returned an invalid address") from exc
            if not resolved.is_global:
                raise ToolPolicyError("verification rejects private or non-global addresses")
        return urllib.parse.urlunsplit(parsed)

    @staticmethod
    def _resolve_addresses(host: str) -> tuple[str, ...]:
        try:
            values = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise ToolPolicyError(f"verification DNS lookup failed: {type(exc).__name__}") from exc
        return tuple(dict.fromkeys(str(value[4][0]) for value in values))


_BOOTSTRAP = r"""
import base64, json, os, pathlib, sys
payload = json.loads(base64.b64decode(sys.argv[1]))
root = pathlib.Path('/workspace')
for name, encoded in payload['files'].items():
    target = root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(base64.b64decode(encoded))
    target.chmod(payload['modes'].get(name, 0o644))
os.chdir(root)
os.execv('/bin/sh', ['/bin/sh', '-lc', payload['command']])
""".strip()


class DockerShellSandbox:
    """Execute inside a locked-down Docker container, with no host fallback."""

    def __init__(
        self,
        config: SandboxConfig,
        *,
        docker_executable: str = "docker",
        audit_sink: AuditSink | None = None,
    ) -> None:
        if config.backend != "docker":
            raise ToolPolicyError("real shell requires the Docker backend")
        self.config = config
        self.docker_executable = docker_executable
        self.audit_sink = audit_sink

    def _client_environment(self) -> dict[str, str]:
        # These variables let the Docker client find its daemon.  No environment
        # is forwarded into the container itself.
        allowed = ("PATH", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "LANG", "LC_ALL")
        return {key: os.environ[key] for key in allowed if key in os.environ}

    @staticmethod
    def _materialize(files: Mapping[str, bytes | str] | None) -> dict[str, str]:
        encoded: dict[str, str] = {}
        total = 0
        for name, raw in (files or {}).items():
            candidate = PurePosixPath(name)
            if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
                raise ToolPolicyError(f"unsafe sandbox artifact path: {name}")
            content = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
            total += len(content)
            if total > 8 * 1024 * 1024:
                raise ToolPolicyError("sandbox artifacts exceed materialization limit")
            encoded[str(candidate)] = base64.b64encode(content).decode("ascii")
        return encoded

    def _command(
        self,
        *,
        container_name: str,
        command: str,
        files: Mapping[str, bytes | str] | None,
        file_modes: Mapping[str, int] | None = None,
        image: str | None = None,
    ) -> list[str]:
        encoded_files = self._materialize(files)
        modes: dict[str, int] = {}
        for name, mode in (file_modes or {}).items():
            if name not in encoded_files or mode not in {0o644, 0o755}:
                raise ToolPolicyError(f"unsafe sandbox artifact mode: {name}={mode!r}")
            modes[name] = mode
        payload = base64.b64encode(
            json.dumps(
                {"command": command, "files": encoded_files, "modes": modes},
                separators=(",", ":"),
            ).encode("utf-8")
        ).decode("ascii")
        argv = [
            self.docker_executable,
            "run",
            "--rm",
            "--pull",
            "never",
            "--name",
            container_name,
            "--network",
            "none",
            "--read-only",
            "--memory",
            f"{self.config.memory_mb}m",
            "--cpus",
            str(self.config.cpus),
            "--pids-limit",
            str(self.config.pids_limit),
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--user",
            "65534:65534",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=16m",
            "--tmpfs",
            "/workspace:rw,nosuid,nodev,size=32m",
            "--workdir",
            "/workspace",
            image or self.config.image,
            "python",
            "-c",
            _BOOTSTRAP,
            payload,
        ]
        return argv

    def _resolve_image_identifier(self, docker_executable: str) -> str | None:
        """Resolve a local tag to Docker's immutable image ID without pulling."""

        try:
            inspected = subprocess.run(
                [
                    docker_executable,
                    "image",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    self.config.image,
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                env=self._client_environment(),
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        try:
            identifier = inspected.stdout.decode("ascii", "strict").strip()
        except UnicodeDecodeError:
            return None
        if inspected.returncode != 0 or not identifier.startswith("sha256:"):
            return None
        digest = identifier.removeprefix("sha256:")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            return None
        return identifier

    def run(
        self,
        command: str,
        *,
        request_id: str,
        source_event_id: str,
        timeout_ms: int | None = None,
        files: Mapping[str, bytes | str] | None = None,
        file_modes: Mapping[str, int] | None = None,
    ) -> ToolResult:
        started = time.monotonic()
        timeout = min(timeout_ms or self.config.timeout_ms, self.config.timeout_ms)
        if not command.strip():
            return ToolResult(request_id, ToolStatus.ERROR, error="shell command is empty")
        executable = shutil.which(self.docker_executable, path=os.environ.get("PATH"))
        if executable is None:
            result = ToolResult(
                request_id,
                ToolStatus.ERROR,
                error="Docker is unavailable; host execution fallback is forbidden",
                metadata={
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": None,
                },
            )
            self._audit(result, source_event_id, command, started)
            return result
        container_name = f"oracle-lab-{request_id.lower().replace('_', '-')[:40]}"
        actual_image = self._resolve_image_identifier(executable)
        try:
            argv = self._command(
                container_name=container_name,
                command=command,
                files=files,
                file_modes=file_modes,
                image=actual_image,
            )
        except ToolPolicyError as exc:
            result = ToolResult(
                request_id,
                ToolStatus.ERROR,
                error=str(exc),
                metadata={
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": actual_image,
                },
            )
            self._audit(result, source_event_id, command, started)
            return result
        argv[0] = executable
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._client_environment(),
                start_new_session=True,
            )
        except OSError as exc:
            result = ToolResult(
                request_id,
                ToolStatus.ERROR,
                error=f"failed to start Docker client: {type(exc).__name__}",
                metadata={
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": actual_image,
                },
            )
            self._audit(result, source_event_id, command, started)
            return result
        stdout, stderr, terminal_status = self._collect(
            process,
            container_name=container_name,
            timeout_ms=timeout,
        )
        elapsed = (time.monotonic() - started) * 1000
        exit_code = process.poll()
        if terminal_status == ToolStatus.TIMEOUT:
            result = ToolResult(
                request_id,
                ToolStatus.TIMEOUT,
                output=stdout.decode("utf-8", "replace"),
                error="sandbox timeout",
                exit_code=exit_code,
                elapsed_ms=elapsed,
                metadata={
                    "stderr": stderr.decode("utf-8", "replace"),
                    "contained": True,
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": actual_image,
                },
                raw_stdout=stdout,
                raw_stderr=stderr,
            )
        elif terminal_status == ToolStatus.OUTPUT_LIMIT:
            result = ToolResult(
                request_id,
                ToolStatus.OUTPUT_LIMIT,
                output=stdout.decode("utf-8", "replace"),
                error="sandbox output limit exceeded",
                exit_code=exit_code,
                elapsed_ms=elapsed,
                metadata={
                    "stderr": stderr.decode("utf-8", "replace"),
                    "contained": True,
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": actual_image,
                },
                raw_stdout=stdout,
                raw_stderr=stderr,
            )
        else:
            result = ToolResult(
                request_id,
                ToolStatus.OK if exit_code == 0 else ToolStatus.ERROR,
                output=stdout.decode("utf-8", "replace"),
                error=None if exit_code == 0 else "sandbox command failed",
                exit_code=exit_code,
                elapsed_ms=elapsed,
                metadata={
                    "stderr": stderr.decode("utf-8", "replace"),
                    "contained": True,
                    "sandbox_image_requested": self.config.image,
                    "sandbox_image_actual": actual_image,
                },
                raw_stdout=stdout,
                raw_stderr=stderr,
            )
        self._audit(result, source_event_id, command, started)
        return result

    def _collect(
        self,
        process: subprocess.Popen[bytes],
        *,
        container_name: str,
        timeout_ms: int,
    ) -> tuple[bytes, bytes, ToolStatus | None]:
        assert process.stdout is not None and process.stderr is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        chunks: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
        size = 0
        deadline = time.monotonic() + timeout_ms / 1000
        terminal_status: ToolStatus | None = None
        try:
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    terminal_status = ToolStatus.TIMEOUT
                    break
                ready = selector.select(min(remaining, 0.1))
                if not ready and process.poll() is not None:
                    # One final pass drains EOF and unregisters both descriptors.
                    ready = selector.select(0)
                    if not ready:
                        break
                for key, _ in ready:
                    block = os.read(key.fileobj.fileno(), 65_536)
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    remaining_capacity = self.config.max_output_bytes - size
                    chunks[key.data].append(block[: max(0, remaining_capacity)])
                    size += len(block)
                    if size > self.config.max_output_bytes:
                        terminal_status = ToolStatus.OUTPUT_LIMIT
                        break
                if terminal_status is not None:
                    break
            if terminal_status is not None:
                process.kill()
                self._remove_container(container_name)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        finally:
            selector.close()
        return b"".join(chunks["stdout"]), b"".join(chunks["stderr"]), terminal_status

    def _remove_container(self, container_name: str) -> None:
        subprocess.run(
            [self.docker_executable, "rm", "-f", container_name],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=self._client_environment(),
            timeout=5,
            check=False,
        )

    def _audit(
        self,
        result: ToolResult,
        source_event_id: str,
        command: str,
        started: float,
    ) -> None:
        if self.audit_sink:
            self.audit_sink(
                ToolAuditRecord(
                    result.request_id,
                    source_event_id,
                    "shell",
                    ToolExecution.REAL_SANDBOX.value,
                    result.status.value,
                    started,
                    result.elapsed_ms,
                    truth_domain=TruthDomain.SANDBOX.value,
                    command=command,
                    detail=result.error,
                )
            )


@dataclass(frozen=True, slots=True)
class ToolPolicy:
    modes: Mapping[str, ToolApproval] = field(
        default_factory=lambda: {
            "calculator": "auto",
            "unit_convert": "auto",
            "regex": "auto",
            "checksum": "auto",
            "file_parse": "auto",
            "shell": "ask",
            "virtual": "auto",
            "web_verify": "ask",
        }
    )

    def mode_for(self, tool: str) -> ToolApproval:
        value = self.modes.get(tool, "deny")
        if value not in {"auto", "ask", "deny"}:
            raise ToolPolicyError(f"invalid policy for {tool}: {value}")
        return value

    def __post_init__(self) -> None:
        object.__setattr__(self, "modes", MappingProxyType(dict(self.modes)))

    @classmethod
    def from_config(cls, values: Mapping[str, ToolApproval | str]) -> ToolPolicy:
        aliases = {
            "calculator": "calculator",
            "unit_conversion": "unit_convert",
            "regex_text": "regex",
            "checksum": "checksum",
            "file_parsing": "file_parse",
            "python_sandbox": "python",
            "shell_sandbox": "shell",
            "virtual_world": "virtual",
            "web_verify": "web_verify",
        }
        modes: dict[str, ToolApproval] = {}
        for config_name, broker_name in aliases.items():
            if config_name in values:
                modes[broker_name] = str(values[config_name])  # type: ignore[assignment]
        return cls(modes)


class ToolBroker:
    """The sole router from a ``tool.request`` into any execution backend."""

    expected_execution: ClassVar[Mapping[str, ToolExecution]] = {
        "calculator": ToolExecution.REAL_DETERMINISTIC,
        "unit_convert": ToolExecution.REAL_DETERMINISTIC,
        "regex": ToolExecution.REAL_DETERMINISTIC,
        "checksum": ToolExecution.REAL_DETERMINISTIC,
        "file_parse": ToolExecution.REAL_DETERMINISTIC,
        "python": ToolExecution.REAL_SANDBOX,
        "shell": ToolExecution.REAL_SANDBOX,
        "virtual": ToolExecution.VIRTUAL,
        "web_verify": ToolExecution.VERIFICATION,
    }

    def __init__(
        self,
        *,
        policy: ToolPolicy | None = None,
        calculator: SafeCalculator | None = None,
        deterministic: SafeDeterministicTools | None = None,
        shell: DockerShellSandbox | None = None,
        virtual: VirtualWorldRuntime | None = None,
        verification: HttpVerificationTool | None = None,
        allowed_virtual_commands: Iterable[str] | None = None,
        virtual_operation_preparer: VirtualOperationPreparer | None = None,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self.policy = policy or ToolPolicy()
        self.calculator = calculator or SafeCalculator()
        self.deterministic = deterministic or SafeDeterministicTools()
        self.shell = shell
        self.virtual = virtual or VirtualWorldRuntime()
        self.verification = verification
        self.allowed_virtual_commands = (
            None
            if allowed_virtual_commands is None
            else frozenset(str(command) for command in allowed_virtual_commands)
        )
        if self.allowed_virtual_commands is not None and any(
            not command or any(character.isspace() for character in command)
            for command in self.allowed_virtual_commands
        ):
            raise ToolPolicyError("virtual command allowlist entries must be one token")
        self.virtual_operation_preparer = virtual_operation_preparer
        self.audit_sink = audit_sink
        self.audit_log: list[ToolAuditRecord] = []

    def execute(self, request: ToolRequest, *, approved: bool = False) -> ToolResult:
        started = time.monotonic()
        expected = self.expected_execution.get(request.tool)
        if expected is None or request.execution != expected:
            return self._finish(
                request,
                ToolResult(
                    request.id,
                    ToolStatus.DENIED,
                    error="unknown tool or execution-class mismatch",
                ),
                started,
            )
        mode = self.policy.mode_for(request.tool)
        if mode == "deny":
            return self._finish(
                request,
                ToolResult(request.id, ToolStatus.DENIED, error="tool denied by policy"),
                started,
            )
        if mode == "ask" and not approved:
            return self._finish(
                request,
                ToolResult(
                    request.id,
                    ToolStatus.PENDING_APPROVAL,
                    error="explicit human approval required",
                ),
                started,
            )
        if request.tool == "calculator":
            expression = request.input.get("expression")
            if not isinstance(expression, str):
                result = ToolResult(
                    request.id, ToolStatus.ERROR, error="calculator expression required"
                )
            else:
                try:
                    value = self.calculator.evaluate(expression)
                    result = ToolResult(request.id, ToolStatus.OK, output=str(value))
                except ToolPolicyError as exc:
                    result = ToolResult(request.id, ToolStatus.ERROR, error=str(exc))
            return self._finish(request, result, started)
        if request.tool in {"unit_convert", "regex", "checksum", "file_parse"}:
            operation = {
                "unit_convert": self.deterministic.unit_convert,
                "regex": self.deterministic.regex,
                "checksum": self.deterministic.checksum,
                "file_parse": self.deterministic.file_parse,
            }[request.tool]
            try:
                output = operation(request.input)
                result = ToolResult(request.id, ToolStatus.OK, output=output)
            except ToolPolicyError as exc:
                result = ToolResult(request.id, ToolStatus.ERROR, error=str(exc))
            return self._finish(request, result, started)
        if request.tool in {"shell", "python"}:
            if self.shell is None:
                result = ToolResult(
                    request.id,
                    ToolStatus.ERROR,
                    error="Docker sandbox is not configured; host fallback is forbidden",
                )
            else:
                command = request.input.get("command")
                if request.tool == "python" and not isinstance(command, str):
                    code = request.input.get("code")
                    if isinstance(code, str):
                        command = "python - <<'PY'\n" + code + "\nPY"
                files = request.input.get("files")
                if not isinstance(command, str):
                    result = ToolResult(
                        request.id, ToolStatus.ERROR, error="shell command required"
                    )
                elif files is not None and not isinstance(files, Mapping):
                    result = ToolResult(
                        request.id, ToolStatus.ERROR, error="files must be an object"
                    )
                else:
                    result = self.shell.run(
                        command,
                        request_id=request.id,
                        source_event_id=request.source_event_id,
                        timeout_ms=request.timeout_ms,
                        files=files,
                    )
            return self._finish(request, result, started)
        if request.tool == "virtual":
            command = request.input.get("command")
            if not isinstance(command, str):
                result = ToolResult(request.id, ToolStatus.ERROR, error="virtual command required")
            else:
                try:
                    argv = shlex.split(command)
                    if (
                        self.allowed_virtual_commands is not None
                        and argv
                        and argv[0] not in self.allowed_virtual_commands
                    ):
                        result = ToolResult(
                            request.id,
                            ToolStatus.DENIED,
                            error=(f"virtual command is not allowed by policy: {argv[0]}"),
                            metadata={"virtual": True},
                        )
                        return self._finish(request, result, started)
                    if self.virtual_operation_preparer is not None:
                        self.virtual_operation_preparer(command)
                    output = self.virtual.execute(
                        command,
                        evidence=SourceEvidence((request.source_event_id,), "explicit"),
                    )
                    result = ToolResult(
                        request.id, ToolStatus.OK, output=output, metadata={"virtual": True}
                    )
                except ValueError as exc:
                    result = ToolResult(
                        request.id, ToolStatus.ERROR, error=str(exc), metadata={"virtual": True}
                    )
            return self._finish(request, result, started)
        if self.verification is None:
            result = ToolResult(
                request.id,
                ToolStatus.ERROR,
                error="verification requires an explicitly configured allowlisted adapter",
            )
        else:
            result = self.verification.fetch(request)
        return self._finish(request, result, started)

    def _finish(self, request: ToolRequest, result: ToolResult, started: float) -> ToolResult:
        truth_domain = _truth_domain_for(request.execution).value
        metadata = {**dict(result.metadata), "truth_domain": truth_domain}
        if truth_domain == "virtual":
            metadata["virtual"] = True
        if dict(result.metadata) != metadata:
            result = ToolResult(
                result.request_id,
                result.status,
                result.output,
                result.error,
                result.exit_code,
                result.elapsed_ms,
                metadata,
            )
        if result.elapsed_ms == 0:
            result = ToolResult(
                result.request_id,
                result.status,
                result.output,
                result.error,
                result.exit_code,
                (time.monotonic() - started) * 1000,
                result.metadata,
            )
        record = ToolAuditRecord(
            request.id,
            request.source_event_id,
            request.tool,
            request.execution.value,
            result.status.value,
            started,
            result.elapsed_ms,
            truth_domain=truth_domain,
            command=(
                request.input.get("command")
                if isinstance(request.input.get("command"), str)
                else None
            ),
            detail=result.error,
        )
        self.audit_log.append(record)
        if self.audit_sink:
            self.audit_sink(record)
        return result


class ToolApprovalRequired(ToolPolicyError):
    """Raised when a durable request exists but its human gate is unresolved."""


class ToolEventStore(Protocol):
    def append(self, event: Event) -> Event: ...

    def append_many(self, events: tuple[Event, ...]) -> tuple[Event, ...]: ...

    def get(self, event_id: str) -> Event | None: ...

    def list_events(self, **filters: Any) -> list[Event]: ...


class ToolDispatcher(Protocol):
    def dispatch(self, event: Event) -> tuple[Any, ...]: ...


class ToolWorker:
    """Persist a broker call from approval through result and usage events."""

    def __init__(
        self,
        broker: ToolBroker,
        store: ToolEventStore,
        *,
        dispatcher: ToolDispatcher | None = None,
    ) -> None:
        self.broker = broker
        self.store = store
        self.dispatcher = dispatcher

    def run(
        self,
        request_event: Event,
        *,
        approved: bool = False,
        approval_event: Event | None = None,
    ) -> Event:
        if request_event.type != EventType.TOOL_REQUEST:
            raise ToolPolicyError("ToolWorker requires a tool.request event")
        if self.store.get(request_event.id) is None:
            raise ToolPolicyError("tool.request must be appended before execution")
        request_payload = _thaw_tool_value(request_event.payload)
        request_payload.setdefault("id", f"tlr_{request_event.id.removeprefix('evt_')}")
        request = ToolRequest.from_dict(request_payload, source_event_id=request_event.id)
        expected = self.broker.expected_execution.get(request.tool)
        mode = self.broker.policy.mode_for(request.tool)
        if mode == "ask" and not approved and expected == request.execution:
            # EventDispatcher persists an await_human_approval job.  Never turn
            # this waiting state into a misleading tool.denied event.
            raise ToolApprovalRequired("explicit human approval is still pending")
        if mode == "ask" and approved and expected == request.execution:
            persisted = None if approval_event is None else self.store.get(approval_event.id)
            target = (
                None
                if approval_event is None
                else (
                    approval_event.payload.get("request_event_id")
                    or approval_event.payload.get("request_id")
                )
            )
            if (
                approval_event is None
                or persisted is None
                or approval_event.type != EventType.TOOL_APPROVED
                or approval_event.actor.kind is not ActorKind.HUMAN
                or approval_event.causation_id != request_event.id
                or target != request_event.id
            ):
                raise ToolApprovalRequired(
                    "approved execution requires a persisted matching human approval event"
                )
        if expected != request.execution or mode == "deny":
            result = self.broker.execute(request, approved=approved)
            denied = result.to_event(
                request,
                session_id=request_event.session_id,
                branch_id=request_event.branch_id,
                correlation_id=request_event.correlation_id,
                parent_event_id=request_event.id,
            )
            self.store.append(denied)
            return denied

        prior_results = self.store.list_events(
            event_type=[
                EventType.TOOL_OUTPUT,
                EventType.TOOL_ERROR,
                EventType.TOOL_TIMEOUT,
                EventType.TOOL_DENIED,
            ],
            causation_id=request_event.id,
        )
        existing = next(
            (event for event in prior_results if event.payload.get("request_id") == request.id),
            None,
        )
        if existing is not None:
            return existing

        lifecycle: list[Event] = []
        started_parent = (
            approval_event.id
            if mode == "ask" and approved and approval_event is not None
            else request_event.id
        )
        started = Event.new(
            EventType.TOOL_STARTED,
            actor=Actor(kind=ActorKind.TOOL, id=request.tool),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=started_parent,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload=request.to_dict(),
        )
        lifecycle.append(started)
        self.store.append_many(tuple(lifecycle))

        result = self.broker.execute(request, approved=approved)
        result_event = result.to_event(
            request,
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            correlation_id=request_event.correlation_id,
            parent_event_id=started.id,
        )
        usage_event = Event.new(
            EventType.USAGE_TOOL,
            actor=Actor(kind=ActorKind.SYSTEM, id="tool-worker"),
            session_id=request_event.session_id,
            branch_id=request_event.branch_id,
            parent_event_id=result_event.id,
            causation_id=request_event.id,
            correlation_id=request_event.correlation_id,
            payload={
                "request_event_id": request_event.id,
                "provider_id": None,
                "model_id": None,
                "tool_id": request.tool,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "reasoning_tokens": None,
                "provider_cost": None,
                "latency_ms": max(0.0, result.elapsed_ms),
                "ttft_ms": None,
                "request_count": 1,
                "status": result.status.value,
            },
        )
        self.store.append_many((result_event, usage_event))
        if self.dispatcher is not None:
            self.dispatcher.dispatch(result_event)
        return result_event


Broker = ToolBroker

__all__ = [
    "Broker",
    "DockerShellSandbox",
    "HttpVerificationTool",
    "SafeCalculator",
    "SafeDeterministicTools",
    "ToolApprovalRequired",
    "ToolAuditRecord",
    "ToolBroker",
    "ToolExecution",
    "ToolPolicy",
    "ToolPolicyError",
    "ToolRequest",
    "ToolResult",
    "ToolStatus",
    "ToolWorker",
    "TruthDomain",
]
