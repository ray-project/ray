"""Unit tests for HAProxy master-worker process management (no HAProxy binary)."""
import asyncio
import signal
import sys
from typing import List, Optional
from unittest import mock

import pytest

from ray.serve._private.haproxy import (
    HAProxyApi,
    HAProxyConfig,
    MasterProcState,
    parse_show_proc,
)

_SHOW_PROC = """#<PID>          <type>          <reloads>       <uptime>        <version>
1162            master          5 [failed: 1]   0d00h02m07s     2.8.25
# workers
1271            worker          1               0d00h00m00s     2.8.25
# old workers
1233            worker          3               0d00h00m43s     2.8.25
1250            worker          2               0d00h00m20s     2.8.25
# programs
"""


@pytest.mark.parametrize(
    "output,expected",
    [
        (
            _SHOW_PROC,
            MasterProcState(
                master_pid=1162,
                failed_reloads=1,
                workers=[1271],
                old_workers=[1233, 1250],
            ),
        ),
        (
            "#<PID> <type> <reloads> <uptime> <version>\n"
            "10 master 0 0d00h00m01s 2.8.25\n# workers\n11 worker 0 0d 2.8.25\n",
            MasterProcState(master_pid=10, failed_reloads=None, workers=[11]),
        ),
        ("", MasterProcState()),
        ("Unknown command.\n", MasterProcState()),
    ],
)
def test_parse_show_proc(output, expected):
    assert parse_show_proc(output) == expected


class FakeMaster:
    """Stand-in for the asyncio.subprocess.Process of the HAProxy master."""

    def __init__(self, pid: int = 100, stderr_path: str = "/nonexistent"):
        self.pid = pid
        self.returncode: Optional[int] = None
        self._stderr_path = stderr_path
        self._stdout_path = stderr_path
        self.terminated = False
        self.exit_on_terminate = True

    def terminate(self):
        self.terminated = True
        if self.exit_on_terminate:
            self.returncode = 0

    def kill(self):
        self.returncode = -9

    async def wait(self):
        while self.returncode is None:
            await asyncio.sleep(0.01)
        return self.returncode


class FakeHAProxyProcesses:
    """Fake master CLI and admin socket. SIGUSR2 to the master triggers a
    reload that either succeeds (a new worker takes over) or fails."""

    def __init__(self, api: HAProxyApi, master: FakeMaster, first_worker: int = 200):
        self.api = api
        self.master = master
        self.workers: List[int] = [first_worker]
        self.old_workers: List[int] = []
        self.failed_reloads = 0
        self.next_worker = first_worker + 100
        self.reload_succeeds = True
        # Admin-socket answers still served by the previous worker after a
        # reload, before the new worker takes over the socket.
        self.stale_admin_answers = 0
        self.admin_pid = first_worker
        self.signals: List[tuple] = []

    async def send(self, command: str, socket_path: Optional[str] = None) -> str:
        if socket_path == self.api.cfg.master_socket_path:
            assert command == "show proc;quit"
            return (
                "#<PID> <type> <reloads> <uptime> <version>\n"
                f"{self.master.pid} master 0 [failed: {self.failed_reloads}] 0d 2.8\n"
                "# workers\n"
                + "".join(f"{pid} worker 0 0d 2.8\n" for pid in self.workers)
                + "# old workers\n"
                + "".join(f"{pid} worker 1 0d 2.8\n" for pid in self.old_workers)
            )
        if command == "show info":
            if self.stale_admin_answers:
                self.stale_admin_answers -= 1
                return f"Pid: {self.old_workers[-1]}\n"
            return f"Pid: {self.admin_pid}\n"
        if command == "show servers state":
            return "1\n"
        raise AssertionError(f"Unexpected command {command!r} on {socket_path}")

    def kill(self, pid: int, sig: int) -> None:
        self.signals.append((pid, sig))
        if pid == self.master.pid and sig == signal.SIGUSR2:
            if not self.reload_succeeds:
                self.failed_reloads += 1
                return
            self.old_workers.extend(self.workers)
            self.workers = [self.next_worker]
            self.admin_pid = self.next_worker
            self.next_worker += 100


@pytest.fixture
def api(tmp_path) -> HAProxyApi:
    api = HAProxyApi(
        cfg=HAProxyConfig(
            socket_path=str(tmp_path / "admin.sock"),
            server_state_file=str(tmp_path / "server-state"),
            master_worker_enabled=True,
            enable_hap_optimization=False,
        ),
        config_file_path=str(tmp_path / "haproxy.cfg"),
    )
    return api


def _running(api: HAProxyApi, fake: FakeHAProxyProcesses) -> None:
    api._proc = fake.master
    api._worker_pid = fake.workers[0]
    api._send_socket_command = fake.send


@pytest.mark.asyncio
async def test_start_launches_master_and_waits_for_worker(api, monkeypatch):
    master = FakeMaster()
    fake = FakeHAProxyProcesses(api, master)
    api._send_socket_command = fake.send
    spawned = []

    async def _exec(*args, **kwargs):
        spawned.append(args)
        return master

    monkeypatch.setattr("ray.serve._private.haproxy.get_haproxy_binary", lambda: "hap")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)

    proc = await api._start_and_wait_for_haproxy()

    assert proc is master
    args = spawned[0]
    assert args[:3] == ("hap", "-W", "-db")
    assert args[args.index("-f") + 1] == api.config_file_path
    assert args[args.index("-S") + 1] == api.cfg.master_socket_path
    assert "-sf" not in args
    assert api._worker_pid == 200
    assert api._old_worker_pids == []


@pytest.mark.asyncio
async def test_start_fails_when_master_exits(api, monkeypatch):
    master = FakeMaster()
    master.returncode = 1

    async def _exec(*args, **kwargs):
        # HAProxy writes its startup error to the redirected stderr file.
        kwargs["stderr"].write(b"[ALERT] config : parsing error\n")
        return master

    monkeypatch.setattr("ray.serve._private.haproxy.get_haproxy_binary", lambda: "hap")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)
    api._send_socket_command = mock.AsyncMock(side_effect=RuntimeError("no socket"))

    with pytest.raises(RuntimeError, match="parsing error"):
        await api._start_and_wait_for_haproxy()


@pytest.mark.asyncio
async def test_failed_start_stops_master_and_forked_workers(api, monkeypatch):
    master = FakeMaster()
    fake = FakeHAProxyProcesses(api, master)
    # The master forked worker 200, but it never answers the admin socket.
    fake.admin_pid = 999
    api._send_socket_command = fake.send
    killed = []

    async def _exec(*args, **kwargs):
        return master

    monkeypatch.setattr("ray.serve._private.haproxy.get_haproxy_binary", lambda: "hap")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)
    monkeypatch.setattr(
        "ray.serve._private.haproxy.os.kill", lambda pid, sig: killed.append((pid, sig))
    )
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: False)
    monkeypatch.setattr(api, "_our_haproxy_pids", lambda: {200})

    with pytest.raises(RuntimeError, match="No new HAProxy worker"):
        await api._start_and_wait_for_haproxy(timeout_s=1)

    assert master.terminated
    assert (200, signal.SIGKILL) in killed
    assert api._proc is None
    assert api._worker_pid is None


@pytest.mark.asyncio
async def test_unconfirmed_reload_still_tracks_displaced_worker(api, monkeypatch):
    fake = FakeHAProxyProcesses(api, FakeMaster())
    _running(api, fake)
    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", fake.kill)
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: True)

    async def _takeover_times_out(*args, **kwargs):
        raise RuntimeError("No new HAProxy worker took over the admin socket")

    monkeypatch.setattr(api, "_wait_for_worker_takeover", _takeover_times_out)

    with pytest.raises(RuntimeError, match="No new HAProxy worker"):
        await api._graceful_reload()

    # The master did replace worker 200 with 300; draining must wait for 200.
    assert api._worker_pid == 300
    assert api._old_worker_pids == [200]
    assert api.has_alive_old_procs()
    assert api._running_config_fingerprint is None


@pytest.mark.asyncio
async def test_reload_signals_master_instead_of_spawning(api, monkeypatch):
    fake = FakeHAProxyProcesses(api, FakeMaster())
    _running(api, fake)
    api._rendered_config_fingerprint = "new"
    api._running_config_fingerprint = "old"
    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", fake.kill)
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: pid in (200, 300))
    monkeypatch.setattr(
        asyncio,
        "create_subprocess_exec",
        mock.AsyncMock(side_effect=AssertionError("must not spawn")),
    )

    await api._graceful_reload()

    assert fake.signals[0] == (100, signal.SIGUSR2)
    assert api._proc is fake.master
    assert api._worker_pid == 300
    assert api._old_worker_pids == [200]
    assert api._running_config_fingerprint == "new"
    # The previous worker is re-signaled to soft-stop.
    assert (200, signal.SIGUSR1) in fake.signals
    assert api.has_alive_old_procs()


@pytest.mark.asyncio
async def test_reload_waits_for_admin_socket_takeover(api, monkeypatch):
    fake = FakeHAProxyProcesses(api, FakeMaster())
    _running(api, fake)
    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", fake.kill)
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: True)
    # The old worker still answers the admin socket for the first poll.
    original_kill = fake.kill

    def _kill(pid, sig):
        original_kill(pid, sig)
        if sig == signal.SIGUSR2:
            fake.stale_admin_answers = 1

    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", _kill)

    await api._graceful_reload()

    assert fake.stale_admin_answers == 0
    assert api._worker_pid == 300


@pytest.mark.asyncio
async def test_failed_reload_raises_and_keeps_current_worker(
    api, monkeypatch, tmp_path
):
    stderr = tmp_path / "stderr.log"
    stderr.write_text("[ALERT] unknown keyword 'bogus'\n")
    fake = FakeHAProxyProcesses(api, FakeMaster(stderr_path=str(stderr)))
    fake.reload_succeeds = False
    _running(api, fake)
    api._rendered_config_fingerprint = "broken"
    api._running_config_fingerprint = "good"
    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", fake.kill)

    with pytest.raises(RuntimeError, match="unknown keyword"):
        await api._graceful_reload()

    assert api._worker_pid == 200
    assert api._old_worker_pids == []
    # The next apply() retries the reload.
    assert api._running_config_fingerprint == "good"


@pytest.mark.asyncio
async def test_reload_times_out_without_new_worker(api, monkeypatch):
    fake = FakeHAProxyProcesses(api, FakeMaster())
    _running(api, fake)
    # Signal is lost: nothing changes and no failure is reported.
    monkeypatch.setattr("ray.serve._private.haproxy.os.kill", lambda pid, sig: None)

    with pytest.raises(RuntimeError, match="No new HAProxy worker"):
        await api._wait_for_worker_takeover(
            fake.master, previous_workers={200}, failed_before=0, timeout_s=1
        )


@pytest.mark.asyncio
async def test_reload_requires_master_cli(api):
    api._proc = FakeMaster()
    api._send_socket_command = mock.AsyncMock(side_effect=RuntimeError("gone"))

    with pytest.raises(RuntimeError, match="master CLI is unavailable"):
        await api._graceful_reload()


@pytest.mark.asyncio
async def test_dead_master_is_unhealthy_even_if_worker_answers(api):
    master = FakeMaster()
    fake = FakeHAProxyProcesses(api, master)
    _running(api, fake)

    assert await api.is_running()

    # The worker still answers the admin socket, but the master is gone.
    master.returncode = -9
    assert not await api.is_running()


@pytest.mark.asyncio
async def test_standalone_health_only_checks_admin_socket(api):
    api.cfg.master_worker_enabled = False
    api._proc = None
    api._send_socket_command = mock.AsyncMock(return_value="Pid: 1\n")

    assert await api.is_running()


def test_old_workers_are_pruned_once_exited(api, monkeypatch):
    api._old_worker_pids = [200, 300]
    alive = {200}
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: pid in alive)

    assert api.has_alive_old_procs()
    assert api._old_worker_pids == [200]

    alive.clear()
    assert not api.has_alive_old_procs()


def test_process_count_excludes_master(api, monkeypatch):
    api._proc = FakeMaster(pid=100)
    monkeypatch.setattr(
        "ray.serve._private.haproxy.os.listdir",
        lambda path: ["1", "100", "200", "300", "self"],
    )
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: pid in (100, 200))

    assert api.count_haproxy_processes() == 1

    api.cfg.master_worker_enabled = False
    assert api.count_haproxy_processes() == 2


@pytest.mark.asyncio
async def test_stop_terminates_master_and_kills_leftover_workers(api, monkeypatch):
    master = FakeMaster()
    api._proc = master
    api._worker_pid = 300
    api._old_worker_pids = [200]
    killed = []
    monkeypatch.setattr(
        "ray.serve._private.haproxy.os.kill", lambda pid, sig: killed.append((pid, sig))
    )
    # Worker 200 exited with the master; 300 is still around.
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: pid == 300)

    await api.stop()

    assert master.terminated
    assert killed == [(300, signal.SIGKILL)]
    assert api._proc is None
    assert api._worker_pid is None
    assert api._old_worker_pids == []


@pytest.mark.asyncio
async def test_stop_kills_master_that_ignores_sigterm(api, monkeypatch):
    master = FakeMaster()
    master.exit_on_terminate = False
    api._proc = master
    monkeypatch.setattr(api, "_is_our_haproxy", lambda pid: False)
    monkeypatch.setattr(
        "ray.serve._private.haproxy.asyncio.wait_for",
        mock.AsyncMock(side_effect=asyncio.TimeoutError),
    )

    await api.stop()

    assert master.terminated
    assert master.returncode == -9


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
