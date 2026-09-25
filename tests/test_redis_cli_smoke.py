import pathlib
import shutil
import subprocess
import sys
import time

import pytest

from tests.conftest import launch_server, stop_server

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
NO_REDIS_CLI_REASON = "redis-cli not found on PATH"


@pytest.mark.skipif(shutil.which("redis-cli") is None, reason=NO_REDIS_CLI_REASON)
def test_redis_cli_ping_answers_pong(tmp_path):
    # tmp_path, not the default ./dump.mrdb: this test's snapshot has no reason to
    # outlive it or to land in the repository's own working directory. launch_server
    # rather than a Popen of its own, so the port-ownership check lives in one place
    proc, port = launch_server(tmp_path / "dump.mrdb")
    try:
        result = subprocess.run(
            ["redis-cli", "-p", str(port), "PING"],
            capture_output=True, text=True, timeout=5,
        )
        assert result.stdout.strip() == "PONG", (result.returncode, result.stdout, result.stderr)
    finally:
        stop_server(proc)
