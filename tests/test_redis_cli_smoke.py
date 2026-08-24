import pathlib
import shutil
import socket
import subprocess
import sys
import time

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
NO_REDIS_CLI_REASON = "redis-cli not found on PATH"


def _free_port():
    # bound and released rather than fixed: the port is free when chosen and could in principle be taken before the server binds it, but a fixed port collides with a developer's own running instance, which is the more likely of the two
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_until_listening(port, deadline):
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return True
        except OSError:
            time.sleep(0.02)
    return False


@pytest.mark.skipif(shutil.which("redis-cli") is None, reason=NO_REDIS_CLI_REASON)
def test_redis_cli_ping_answers_pong():
    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(REPO_ROOT / "server.py"), "--port", str(port)])
    try:
        assert _wait_until_listening(port, time.monotonic() + 5), "server never started listening"
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
