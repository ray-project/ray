import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

import ray
import ray._private.ray_constants as ray_constants
import ray._private.services as services
import ray.util.client.server.server as ray_client_server
from ray._common.test_utils import wait_for_condition


@pytest.fixture
def ray_gdb_start():
    # Setup environment and start ray
    _environ = os.environ.copy()
    for process_name in ["RAYLET", "PLASMA_STORE"]:
        os.environ["RAY_{}_GDB".format(process_name)] = "1"
        os.environ["RAY_{}_TMUX".format(process_name)] = "1"

    yield None

    # Restore original environment and stop ray
    os.environ.clear()
    os.environ.update(_environ)
    ray.shutdown()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="This test requires Linux.",
)
def test_raylet_gdb(ray_gdb_start):
    # ray_gdb_start yields the expected process name
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        return 42

    assert ray.get(f.remote()) == 42

    # Check process name in `ps aux | grep gdb`
    pgrep_command = subprocess.Popen(
        ["pgrep", "-f", "gdb.*raylet/raylet"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert pgrep_command.communicate()[0]


@pytest.mark.skipif(sys.platform == "win32", reason="memray not supported in win32")
@pytest.mark.skipif(sys.platform == "darwin", reason="memray not supported in Darwin")
def test_memory_profiler_command_builder(monkeypatch, tmp_path):
    session_dir = tmp_path
    # When there's no env var, command should be just a regular python command.
    command = services._build_python_executable_command_memory_profileable(
        ray_constants.PROCESS_TYPE_DASHBOARD, session_dir
    )
    assert command == [sys.executable, "-u"]

    with monkeypatch.context() as m:
        m.setenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV, "dashboard")
        m.setenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV, "-q")
        command = services._build_python_executable_command_memory_profileable(
            ray_constants.PROCESS_TYPE_DASHBOARD, session_dir
        )

        assert command == [
            sys.executable,
            "-u",
            "-m",
            "memray",
            "run",
            "-o",
            str(
                Path(tmp_path)
                / "profile"
                / f"{Path(tmp_path).name}_memory_dashboard.bin"
            ),  # noqa
            "-q",
        ]

        # Test with explicit -o path
        m.delenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV)
        m.delenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV)
        m.setenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV, "dashboard")
        m.setenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV, "-o,/custom/path.bin,-q")
        command = services._build_python_executable_command_memory_profileable(
            ray_constants.PROCESS_TYPE_DASHBOARD, session_dir
        )
        assert command == [
            sys.executable,
            "-u",
            "-m",
            "memray",
            "run",
            "-o",
            "/custom/path.bin",
            "-q",
        ]

        # Test with explicit --output path
        m.delenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV)
        m.delenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV)
        m.setenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV, "dashboard")
        m.setenv(
            services.RAY_MEMRAY_PROFILE_OPTIONS_ENV, "--output,/custom/path.bin,-q"
        )
        command = services._build_python_executable_command_memory_profileable(
            ray_constants.PROCESS_TYPE_DASHBOARD, session_dir
        )
        assert command == [
            sys.executable,
            "-u",
            "-m",
            "memray",
            "run",
            "--output",
            "/custom/path.bin",
            "-q",
        ]

        m.delenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV)
        m.delenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV)
        m.setenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV, "dashboard,dashboard_agent")
        m.setenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV, "-q,--live,--live-port,1234")
        command = services._build_python_executable_command_memory_profileable(
            ray_constants.PROCESS_TYPE_DASHBOARD_AGENT, session_dir
        )
        assert command == [
            sys.executable,
            "-u",
            "-m",
            "memray",
            "run",
            "-q",
            "--live",
            "--live-port",
            "1234",
        ]


@pytest.mark.skipif(sys.platform == "win32", reason="memray not supported in win32")
@pytest.mark.skipif(sys.platform == "darwin", reason="memray not supported in Darwin")
def test_memory_profile_dashboard_and_agent(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        m.setenv(services.RAY_MEMRAY_PROFILE_COMPONENT_ENV, "dashboard,dashboard_agent")
        m.setenv(services.RAY_MEMRAY_PROFILE_OPTIONS_ENV, "-q")
        addr = ray.init()

        def verify():
            session_dir = Path(addr["session_dir"])
            profile_dir = session_dir / "profile"
            assert profile_dir.exists()
            files = []
            for f in profile_dir.iterdir():
                files.append(f.name)
            assert len(files) == 2
            assert f"{session_dir.name}_memory_dashboard.bin" in files
            assert f"{session_dir.name}_memory_dashboard_agent.bin" in files
            return True

        wait_for_condition(verify)


def test_start_ray_client_server_redis_password_env_updates(monkeypatch):
    captured = {}
    expected_process_info = object()

    def fake_start_ray_process(command, process_type, **kwargs):
        captured["command"] = command
        captured["process_type"] = process_type
        captured["kwargs"] = kwargs
        return expected_process_info

    with monkeypatch.context() as m:
        m.setattr(services, "start_ray_process", fake_start_ray_process)
        m.delenv(ray_constants.RAY_REDIS_PASSWORD_ENV, raising=False)

        process_info = services.start_ray_client_server(
            address="127.0.0.1:6379",
            ray_client_server_ip="127.0.0.1",
            ray_client_server_port=10001,
            redis_username="redis-user",
            redis_password="secret123",
            fate_share=False,
            runtime_env_agent_address="127.0.0.1:12345",
            node_id="node-1",
        )

        assert process_info is expected_process_info
        assert captured["process_type"] == ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER
        assert "--redis-username=redis-user" in captured["command"]
        assert not any(
            arg.startswith("--redis-password=") for arg in captured["command"]
        )
        assert captured["kwargs"]["env_updates"] == {
            ray_constants.RAY_REDIS_PASSWORD_ENV: "secret123"
        }
        assert captured["kwargs"]["fate_share"] is False
        assert captured["kwargs"]["avoid_preexec_fn"] is False
        assert ray_constants.RAY_REDIS_PASSWORD_ENV not in os.environ


def test_start_ray_client_specific_server_avoids_preexec_fn(monkeypatch):
    captured = {}
    expected_process_info = object()

    def fake_start_ray_process(command, process_type, **kwargs):
        captured["command"] = command
        captured["process_type"] = process_type
        captured["kwargs"] = kwargs
        return expected_process_info

    with monkeypatch.context() as m:
        m.setattr(services.sys, "platform", "linux")
        m.setattr(services, "start_ray_process", fake_start_ray_process)

        process_info = services.start_ray_client_server(
            address="127.0.0.1:6379",
            ray_client_server_ip="127.0.0.1",
            ray_client_server_port=10001,
            fate_share=True,
            server_type="specific-server",
            serialized_runtime_env_context="{}",
        )

        assert process_info is expected_process_info
        assert captured["process_type"] == ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER
        assert "--mode=specific-server" in captured["command"]
        assert captured["kwargs"]["fate_share"] is False
        assert captured["kwargs"]["pipe_stdin"] is True
        assert captured["kwargs"]["avoid_preexec_fn"] is True


def test_ray_client_specific_server_parent_pipe_monitor_starts_thread(monkeypatch):
    started = []

    class FakeThread:
        def __init__(self, target=None, name=None, daemon=None):
            started.append(
                {
                    "target": target,
                    "name": name,
                    "daemon": daemon,
                }
            )

        def start(self):
            started[-1]["started"] = True

    with monkeypatch.context() as m:
        m.setattr(ray_client_server.threading, "Thread", FakeThread)
        ray_client_server._start_parent_pipe_monitor()

    assert len(started) == 1
    assert started[0]["name"] == "ray-client-parent-pipe-monitor"
    assert started[0]["daemon"] is True
    assert started[0]["started"] is True
    assert callable(started[0]["target"])


def test_ray_client_specific_server_parent_pipe_monitor_exits_on_eof(monkeypatch):
    killed = []
    read_calls = {"n": 0}

    def fake_read(fd, n):
        read_calls["n"] += 1
        if read_calls["n"] == 1:
            return b""
        raise AssertionError("read should stop after EOF")

    class ImmediateThread:
        def __init__(self, target=None, name=None, daemon=None):
            self._target = target

        def start(self):
            self._target()

    with monkeypatch.context() as m:
        m.setattr(ray_client_server.os, "read", fake_read)
        m.setattr(ray_client_server.os, "getpid", lambda: 4242)
        m.setattr(
            ray_client_server.os,
            "kill",
            lambda pid, sig: killed.append((pid, sig)),
        )
        m.setattr(ray_client_server.sys.stdin, "fileno", lambda: 0)
        m.setattr(ray_client_server.threading, "Thread", ImmediateThread)
        ray_client_server._start_parent_pipe_monitor()

    assert killed == [(4242, signal.SIGTERM)]


def test_start_ray_process_avoid_preexec_close_fds_when_supported(monkeypatch):
    captured = {}
    expected_process = object()

    def fake_console_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return expected_process

    with monkeypatch.context() as m:
        m.setattr(services.sys, "platform", "linux")
        m.setattr(services.os, "POSIX_SPAWN_CLOSEFROM", object(), raising=False)
        m.setattr(services, "ConsolePopen", fake_console_popen)

        process_info = services.start_ray_process(
            [sys.executable],
            ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
            fate_share=False,
            avoid_preexec_fn=True,
        )

        assert process_info.process is expected_process
        assert captured["kwargs"]["preexec_fn"] is None
        assert captured["kwargs"]["close_fds"] is True


def test_start_ray_process_avoid_preexec_blocks_sigint_for_child(monkeypatch):
    captured = {}
    expected_process = object()
    calls = []
    previous_mask = {signal.SIGTERM}

    def fake_console_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return expected_process

    def fake_pthread_sigmask(how, signals):
        calls.append((how, set(signals) if signals is not None else signals))
        return previous_mask

    with monkeypatch.context() as m:
        m.setattr(services.sys, "platform", "linux")
        m.setattr(services.os, "POSIX_SPAWN_CLOSEFROM", object(), raising=False)
        m.setattr(services.signal, "pthread_sigmask", fake_pthread_sigmask)
        m.setattr(services, "ConsolePopen", fake_console_popen)

        process_info = services.start_ray_process(
            [sys.executable],
            ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
            fate_share=False,
            avoid_preexec_fn=True,
        )

    assert process_info.process is expected_process
    assert captured["kwargs"]["preexec_fn"] is None
    assert calls == [
        (signal.SIG_BLOCK, {signal.SIGINT}),
        (signal.SIG_SETMASK, previous_mask),
    ]


def test_start_ray_process_avoid_preexec_leaves_fds_open_without_closefrom(
    monkeypatch,
):
    captured = {}
    expected_process = object()

    def fake_console_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return expected_process

    with monkeypatch.context() as m:
        m.setattr(services.sys, "platform", "linux")
        m.delattr(services.os, "POSIX_SPAWN_CLOSEFROM", raising=False)
        m.setattr(services, "ConsolePopen", fake_console_popen)

        process_info = services.start_ray_process(
            [sys.executable],
            ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
            fate_share=False,
            avoid_preexec_fn=True,
        )

        assert process_info.process is expected_process
        assert captured["kwargs"]["preexec_fn"] is None
        assert captured["kwargs"]["close_fds"] is False


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-sv", __file__]))
