"""Unit tests for HAProxyApi process takeover and reload signaling.

Regression tests for the orphaned-worker incident: during a graceful reload
the admin socket path keeps answering through its previous owner until the
new process rebinds it, so a readiness check that only asks "did the socket
answer?" passes for a spawn that dies before taking over. The manager then
advances ``self._proc`` to a corpse and the worker that spawn was meant to
stop is never signaled again — it keeps serving its stale config (with
health checks still running) indefinitely.
"""
import asyncio
import sys
from typing import Optional

import pytest

from ray.serve._private.haproxy import HAProxyApi, HAProxyConfig


class FakeProc:
    """Minimal stand-in for asyncio.subprocess.Process."""

    def __init__(
        self,
        pid: int,
        returncode: Optional[int] = None,
        stdout_path: str = "",
        stderr_path: str = "",
    ):
        self.pid = pid
        self.returncode = returncode
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path


@pytest.fixture
def api(tmp_path) -> HAProxyApi:
    cfg = HAProxyConfig(
        socket_path=str(tmp_path / "admin.sock"),
        server_state_base=str(tmp_path),
        server_state_file=str(tmp_path / "server-state"),
        enable_hap_optimization=False,
    )
    return HAProxyApi(cfg=cfg, config_file_path=str(tmp_path / "haproxy.cfg"))


def _make_stream_files(tmp_path, stderr_text: str = ""):
    stdout = tmp_path / "spawn.stdout.log"
    stderr = tmp_path / "spawn.stderr.log"
    stdout.write_text("")
    stderr.write_text(stderr_text)
    return str(stdout), str(stderr)


def _answer_show_info(api: HAProxyApi, response: str) -> None:
    async def fake_send(cmd: str) -> str:
        assert cmd == "show info"
        return response

    api._send_socket_command = fake_send


class TestWaitForHapAvailability:
    def test_rejects_answer_from_previous_socket_owner(self, api, tmp_path):
        """A spawn must not be declared ready just because the admin socket
        answered: during a reload the socket is still owned by the previous
        worker. Readiness requires the answering pid to be the spawn itself.
        """
        stdout, stderr = _make_stream_files(tmp_path, "fd transfer failed")
        # The socket answers, but from the OLD worker (pid 111).
        _answer_show_info(api, "Name: HAProxy\nPid: 111\nUptime: 1d\n")
        proc = FakeProc(pid=222, stdout_path=stdout, stderr_path=stderr)

        with pytest.raises(RuntimeError, match="did not take over") as exc_info:
            asyncio.run(api._wait_for_hap_availability(proc, timeout_s=1))

        # The spawn's stderr is surfaced at failure time.
        assert "fd transfer failed" in str(exc_info.value)

    def test_passes_when_answering_pid_matches(self, api, tmp_path):
        stdout, stderr = _make_stream_files(tmp_path)
        _answer_show_info(api, "Name: HAProxy\nPid: 222\nUptime: 0d\n")
        proc = FakeProc(pid=222, stdout_path=stdout, stderr_path=stderr)

        asyncio.run(api._wait_for_hap_availability(proc, timeout_s=1))

    def test_crashed_spawn_raises_with_stderr(self, api, tmp_path):
        stdout, stderr = _make_stream_files(tmp_path, "cannot bind socket")
        _answer_show_info(api, "Name: HAProxy\nPid: 111\n")
        proc = FakeProc(pid=222, returncode=1, stdout_path=stdout, stderr_path=stderr)

        with pytest.raises(RuntimeError, match="crashed during startup"):
            asyncio.run(api._wait_for_hap_availability(proc, timeout_s=1))


class TestGracefulReloadSignaling:
    def _patch_spawn(self, api, new_proc: FakeProc, captured: dict) -> None:
        async def fake_start(*extra_args, timeout_s=None):
            captured["args"] = list(extra_args)
            return new_proc

        async def fake_wait(proc, timeout_s=None):
            return None

        api._start_and_wait_for_haproxy = fake_start
        api._wait_for_hap_availability = fake_wait

    def test_resignals_live_old_procs(self, api, tmp_path):
        """Every reload re-targets still-alive displaced workers in -sf, so a
        worker whose original stop signal was lost is healed on the next
        reload instead of being stranded outside the chain forever.
        """
        stdout, stderr = _make_stream_files(tmp_path)
        current = FakeProc(pid=10)
        live_old = FakeProc(pid=20)
        exited_old = FakeProc(
            pid=30, returncode=0, stdout_path=stdout, stderr_path=stderr
        )
        api._proc = current
        api._old_procs = [live_old, exited_old]

        captured = {}
        new_proc = FakeProc(pid=40)
        self._patch_spawn(api, new_proc, captured)

        asyncio.run(api._graceful_reload())

        # Current worker first, then live displaced workers; exited ones not.
        assert captured["args"] == ["-sf", "10", "20"]
        assert api._proc is new_proc
        # The displaced current worker is tracked; the exited one is pruned.
        assert api._old_procs == [live_old, current]

    def test_sf_targets_only_current_when_no_old_procs(self, api):
        current = FakeProc(pid=10)
        api._proc = current
        api._old_procs = []

        captured = {}
        self._patch_spawn(api, FakeProc(pid=40), captured)

        asyncio.run(api._graceful_reload())

        assert captured["args"] == ["-sf", "10"]


class TestGetRunningPid:
    @pytest.mark.parametrize(
        "response,expected",
        [
            ("Name: HAProxy\nVersion: 2.8\nPid: 4242\nUptime: 0d\n", 4242),
            ("Name: HAProxy\nPid: not-a-pid\n", None),
            ("Name: HAProxy\nVersion: 2.8\n", None),
        ],
    )
    def test_parses_show_info(self, api, response, expected):
        _answer_show_info(api, response)
        assert asyncio.run(api._get_running_pid()) == expected

    def test_returns_none_when_socket_unavailable(self, api):
        async def fake_send(cmd: str) -> str:
            raise RuntimeError("socket does not exist")

        api._send_socket_command = fake_send
        assert asyncio.run(api._get_running_pid()) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
