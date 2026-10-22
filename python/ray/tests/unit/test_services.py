import shlex
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ray._private import services
from ray._private.resource_isolation_config import ResourceIsolationConfig


@pytest.mark.skipif(sys.platform == "win32", reason="Tests POSIX command parsing")
@pytest.mark.parametrize(
    "directory_name",
    ["logs with spaces", "logs'quotes", 'logs"quotes', r"logs\backslash", "logs$HOME"],
)
def test_raylet_commands_preserve_logs_dir(tmp_path, monkeypatch, directory_name):
    logs_dir = str(tmp_path / directory_name)
    # Enable all worker languages without requiring their executables.
    monkeypatch.setattr(services.shutil, "which", lambda _: "/usr/bin/java")
    monkeypatch.setattr(services, "get_ray_jars_dir", lambda: str(tmp_path))
    cpp_worker = tmp_path / "cpp_worker"
    cpp_worker.touch()
    monkeypatch.setattr(services, "DEFAULT_WORKER_EXECUTABLE", str(cpp_worker))
    monkeypatch.setattr(
        services.ray._private.utils, "get_dashboard_dependency_error", lambda: None
    )
    start_process = Mock()
    monkeypatch.setattr(services, "start_ray_process", start_process)

    services.start_raylet(
        redis_address=None,
        gcs_address="127.0.0.1:6379",
        node_id="node-id",
        node_ip_address="127.0.0.1",
        node_manager_port=0,
        raylet_name=str(tmp_path / "raylet"),
        plasma_store_name=str(tmp_path / "plasma"),
        cluster_id="cluster-id",
        worker_path="default_worker.py",
        setup_worker_path="setup_worker.py",
        temp_dir=str(tmp_path),
        session_dir=str(tmp_path / "session"),
        resource_dir=str(tmp_path / "session" / "runtime_resources"),
        log_dir=logs_dir,
        resource_and_label_spec=SimpleNamespace(
            to_resource_dict=lambda: {"CPU": 1}, labels={}, num_cpus=1
        ),
        plasma_directory=str(tmp_path),
        fallback_directory=str(tmp_path),
        object_store_memory=100_000_000,
        session_name="session",
        is_head_node=True,
        resource_isolation_config=ResourceIsolationConfig(),
    )

    raylet_args = dict(
        arg.split("=", 1) for arg in start_process.call_args.args[0][1:] if "=" in arg
    )
    for command, logs_option in (
        ("python_worker_command", "--logs-dir"),
        ("java_worker_command", "-Dray.logging.dir"),
        ("cpp_worker_command", "--ray_logs_dir"),
        ("dashboard_agent_command", "--log-dir"),
        ("runtime_env_agent_command", "--log-dir"),
    ):
        # The raylet's POSIX ParseCommandLine uses the same quoting rules.
        args = shlex.split(raylet_args[f"--{command}"])
        assert f"{logs_option}={logs_dir}" in args
        if "worker" in command:
            assert "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER" in args
    assert "--cluster-id=cluster-id" in shlex.split(
        raylet_args["--python_worker_command"]
    )
    assert shlex.split(raylet_args["--java_worker_command"])[-1] == (
        "io.ray.runtime.runner.worker.DefaultWorker"
    )


def test_serialize_command_windows(monkeypatch):
    monkeypatch.setattr(services, "sys", SimpleNamespace(platform="win32"))
    command = [r"C:\Program Files\python.exe", r"--logs-dir=C:\logs'quoted\logs", ""]
    assert services._serialize_command(command) == (
        r'''"C:\Program Files\python.exe" --logs-dir=C:\logs'quoted\logs ""'''
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
