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
        assert captured["kwargs"]["use_posix_spawn"] is False
        assert ray_constants.RAY_REDIS_PASSWORD_ENV not in os.environ


def test_start_ray_client_specific_server_uses_fork_safe_spawn(monkeypatch):
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
        assert "--monitor-parent-pipe" in captured["command"]
        assert captured["kwargs"]["fate_share"] is False
        assert captured["kwargs"]["pipe_stdin"] is True
        assert captured["kwargs"]["use_posix_spawn"] is True


def test_setup_worker_parent_pipe_monitor_starts_subprocess(monkeypatch):
    from ray._private.workers import setup_worker

    captured = {}
    expected_process = object()
    fake_stdin = object()

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return expected_process

    with monkeypatch.context() as m:
        m.setattr(setup_worker.sys, "stdin", fake_stdin)
        m.setattr(setup_worker.os, "getpid", lambda: 12345)
        m.setattr(setup_worker.subprocess, "Popen", fake_popen)
        process = setup_worker._start_parent_pipe_monitor(True)

    assert process is expected_process
    assert captured["command"] == [
        setup_worker.sys.executable,
        "-c",
        setup_worker._PARENT_PIPE_MONITOR_SCRIPT,
        "12345",
    ]
    assert captured["kwargs"]["stdin"] is fake_stdin
    assert captured["kwargs"]["stdout"] == setup_worker.subprocess.DEVNULL
    assert captured["kwargs"]["stderr"] == setup_worker.subprocess.DEVNULL
    assert setup_worker._start_parent_pipe_monitor(False) is None


def test_ray_client_specific_server_blocks_sigint(monkeypatch):
    calls = []
    sig_block = object()

    def fake_pthread_sigmask(how, signals):
        calls.append((how, signals))

    with monkeypatch.context() as m:
        m.setattr(
            ray_client_server.signal,
            "pthread_sigmask",
            fake_pthread_sigmask,
            raising=False,
        )
        m.setattr(ray_client_server.signal, "SIG_BLOCK", sig_block, raising=False)

        ray_client_server._block_sigint_for_specific_server()

    assert calls == [(sig_block, {signal.SIGINT})]


def test_ray_client_specific_server_sigint_block_noops_without_posix_signal_support(
    monkeypatch,
):
    calls = []

    def fake_pthread_sigmask(how, signals):
        calls.append((how, signals))

    with monkeypatch.context() as m:
        m.setattr(
            ray_client_server.signal,
            "pthread_sigmask",
            fake_pthread_sigmask,
            raising=False,
        )
        m.delattr(ray_client_server.signal, "SIG_BLOCK", raising=False)

        ray_client_server._block_sigint_for_specific_server()

    assert calls == []


def test_start_ray_process_posix_spawn_close_fds_when_supported(monkeypatch):
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
            use_posix_spawn=True,
        )

        assert process_info.process is expected_process
        assert captured["kwargs"]["preexec_fn"] is None
        assert captured["kwargs"]["close_fds"] is True


def test_start_ray_process_posix_spawn_blocks_sigint_for_child(monkeypatch):
    captured = {}
    expected_process = object()
    calls = []
    previous_mask = {signal.SIGTERM}
    sig_block = object()
    sig_setmask = object()

    def fake_console_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return expected_process

    def fake_pthread_sigmask(how, signals):
        calls.append((how, signals))
        return previous_mask

    with monkeypatch.context() as m:
        m.setattr(services.sys, "platform", "linux")
        m.setattr(services.os, "POSIX_SPAWN_CLOSEFROM", object(), raising=False)
        m.setattr(
            services.signal,
            "pthread_sigmask",
            fake_pthread_sigmask,
            raising=False,
        )
        m.setattr(services.signal, "SIG_BLOCK", sig_block, raising=False)
        m.setattr(services.signal, "SIG_SETMASK", sig_setmask, raising=False)
        m.setattr(services, "ConsolePopen", fake_console_popen)

        process_info = services.start_ray_process(
            [sys.executable],
            ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
            fate_share=False,
            use_posix_spawn=True,
        )

    assert process_info.process is expected_process
    assert captured["kwargs"]["preexec_fn"] is None
    assert calls == [
        (sig_block, {signal.SIGINT}),
        (sig_setmask, previous_mask),
    ]


def test_start_ray_process_posix_spawn_leaves_fds_open_for_older_runtime(
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
            use_posix_spawn=True,
        )

        assert process_info.process is expected_process
        assert captured["kwargs"]["preexec_fn"] is None
        assert captured["kwargs"]["close_fds"] is False


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-sv", __file__]))
