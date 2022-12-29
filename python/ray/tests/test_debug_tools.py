import os
import subprocess
import sys

import pytest
from pathlib import Path

import ray

import ray._private.services as services
import ray._private.ray_constants as ray_constants

from ray._private.test_utils import wait_for_condition


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
            "-o",
            str(
                Path(tmp_path)
                / "profile"
                / f"{Path(tmp_path).name}_memory_dashboard_agent.bin"
            ),  # noqa
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


if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
