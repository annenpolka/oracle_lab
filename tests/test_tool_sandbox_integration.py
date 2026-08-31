from __future__ import annotations

import shutil
import subprocess

import pytest

from oracle_lab.config import SandboxConfig
from oracle_lab.tooling import DockerShellSandbox, ToolStatus

IMAGE = "python:3.13-alpine"


@pytest.fixture(scope="module")
def sandbox() -> DockerShellSandbox:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker CLI is unavailable")
    try:
        daemon = subprocess.run(
            [docker, "info"],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("Docker daemon did not respond")
    if daemon.returncode != 0:
        pytest.skip("Docker daemon is unavailable")
    image = subprocess.run(
        [docker, "image", "inspect", IMAGE],
        capture_output=True,
        timeout=5,
        check=False,
    )
    if image.returncode != 0:
        pytest.skip("sandbox image is not cached; tests never pull implicitly")
    return DockerShellSandbox(
        SandboxConfig(
            image=IMAGE,
            timeout_ms=1_000,
            memory_mb=128,
            cpus=0.5,
            pids_limit=16,
            max_output_bytes=1_024,
        )
    )


def _run(sandbox: DockerShellSandbox, command: str, suffix: str, timeout_ms: int = 1_000):
    return sandbox.run(
        command,
        request_id=f"tlr_integration_{suffix}",
        source_event_id="evt_integration_source",
        timeout_ms=timeout_ms,
    )


def test_real_sandbox_contains_filesystem_and_credentials(
    sandbox: DockerShellSandbox,
) -> None:
    result = _run(
        sandbox,
        """python - <<'PY'
import os, pathlib
leaked = [key for key in os.environ if key.startswith(('AWS_', 'OPENAI_', 'AZURE_'))]
assert 'SSH_AUTH_SOCK' not in os.environ
assert not leaked, leaked
assert not pathlib.Path('/Users').exists()
try:
    pathlib.Path('/escape').write_text('forbidden')
except OSError:
    print('CONTAINED')
else:
    raise SystemExit('wrote to read-only root')
PY""",
        "filesystem",
    )
    assert result.status == ToolStatus.OK
    assert result.output.strip() == "CONTAINED"


def test_real_sandbox_contains_network(sandbox: DockerShellSandbox) -> None:
    result = _run(
        sandbox,
        """python - <<'PY'
import socket
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(('1.1.1.1', 443))
except OSError:
    print('CONTAINED')
else:
    raise SystemExit('network unexpectedly available')
PY""",
        "network",
    )
    assert result.status == ToolStatus.OK
    assert result.output.strip() == "CONTAINED"


def test_real_sandbox_contains_process_bomb(sandbox: DockerShellSandbox) -> None:
    result = _run(
        sandbox,
        """python - <<'PY'
import subprocess
children = []
try:
    for _ in range(64):
        children.append(subprocess.Popen(['sleep', '10']))
except OSError:
    print('CONTAINED')
else:
    raise SystemExit('pids limit did not engage')
finally:
    for child in children:
        child.terminate()
PY""",
        "pids",
    )
    assert result.status == ToolStatus.OK
    assert result.output.strip() == "CONTAINED"


def test_real_sandbox_contains_timeout_and_oversized_output(
    sandbox: DockerShellSandbox,
) -> None:
    timeout = _run(sandbox, "sleep 5", "timeout", timeout_ms=100)
    oversized = _run(
        sandbox,
        "python -c \"print('x' * 100000)\"",
        "output",
    )
    assert timeout.status == ToolStatus.TIMEOUT
    assert timeout.metadata["contained"] is True
    assert oversized.status == ToolStatus.OUTPUT_LIMIT
    assert len(oversized.output.encode("utf-8")) <= 1_024
