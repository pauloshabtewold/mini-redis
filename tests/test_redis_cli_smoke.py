import pathlib
import shutil
import subprocess
import sys
import time

import pytest

from tests.conftest import free_port, wait_until_listening

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
NO_REDIS_CLI_REASON = "redis-cli not found on PATH"


@pytest.mark.skipif(shutil.which("redis-cli") is None, reason=NO_REDIS_CLI_REASON)
def test_redis_cli_ping_answers_pong(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        # tmp_path, not the default ./dump.mrdb: this test's snapshot has no reason to
        # outlive it or to land in the repository's own working directory
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", str(port),
         "--snapshot-path", str(tmp_path / "dump.mrdb")],
        stdout=subprocess.DEVNULL,
    )
    try:
        assert wait_until_listening(port, time.monotonic() + 5), "server never started listening"
        result = subprocess.run(
            ["redis-cli", "-p", str(port), "PING"],
            capture_output=True, text=True, timeout=5,
        )
        assert result.stdout.strip() == "PONG", (result.returncode, result.stdout, result.stderr)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
