import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from click.testing import CliRunner

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import get_current_unused_port


def _get_non_loopback_ip() -> str:
    import socket

    import psutil

    for addrs in psutil.net_if_addrs().values():
        for addr in addrs:
            if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                return addr.address
    return "127.0.0.1"


def _kill_process_group(proc: subprocess.Popen) -> None:
    """Best-effort kill of a process and its children."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            # The process may have already died.
            pass


@contextmanager
def _setup_mock_network_utils(curr_ip: str, head_ip: str):
    import socket

    # Mock socket.getaddrinfo to return a valid IP
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [("", "", "", "", (curr_ip, 6379))]

        # Mock psutil.net_if_addrs to return localhost IP
        with patch("psutil.net_if_addrs") as mock_net_if_addrs:
            mock_net_if_addrs.return_value = {
                "lo": [
                    type(
                        "addr",
                        (),
                        {"family": socket.AF_INET, "address": head_ip},
                    )()
                ]
            }
            yield


@pytest.fixture
def cleanup_ray():
    """Shutdown all ray instances."""
    yield
    subprocess.run(["ray", "stop", "--force"], capture_output=True)
    ray.shutdown()


def test_symmetric_run_basic_interface(cleanup_ray):
    """Test basic symmetric_run interface with minimal arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    # Mock subprocess.run to avoid actually starting Ray
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            # Test basic symmetric_run call using CliRunner
            result = runner.invoke(symmetric_run, args)
            assert result.exit_code == 0

            # Verify that subprocess.run was called for ray start
            assert mock_run.called
            calls = mock_run.call_args_list

            # Should have called ray start with --head
            ray_start_calls = [
                call for call in calls if "ray" in str(call) and "start" in str(call)
            ]
            assert len(ray_start_calls) > 0

            # Should have called ray stop
            ray_stop_calls = [
                call for call in calls if "ray" in str(call) and "stop" in str(call)
            ]
            assert len(ray_stop_calls) > 0


def test_symmetric_run_worker_node_behavior(cleanup_ray):
    """Test symmetric_run behavior when not on the head node."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0

        with _setup_mock_network_utils("192.168.1.100", "192.168.1.101"):
            # Pretend head is ready so worker proceeds.
            with patch("ray.scripts.symmetric_run.check_head_node_ready") as ready:
                ready.return_value = True

                args = ["--address", "192.168.1.100:6379", "--", "echo", "test"]
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

            # Verify that subprocess.run was called
            assert mock_run.called
            calls = mock_run.call_args_list

            # Should have called ray start with --address (worker mode)
            ray_start_calls = [
                call for call in calls if "ray" in str(call) and "start" in str(call)
            ]
            assert len(ray_start_calls) > 0

            # Check that it's in worker mode (--address instead of --head)
            start_call = ray_start_calls[0]
            start_args = start_call[0][0]
            assert "--address" in start_args
            assert "192.168.1.100:6379" in start_args
            assert "--head" not in start_args
            assert "--block" in start_args  # Worker nodes should block


def test_symmetric_run_arg_validation(cleanup_ray):
    """Test that symmetric_run validates arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):
        # Mock subprocess.run to avoid actually starting Ray
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            result = runner.invoke(symmetric_run, args)
            assert result.exit_code == 0

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "echo", "test"]
            result = runner.invoke(symmetric_run, args)
            assert result.exit_code == 1
            assert "No separator" in result.output

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "--head", "--", "echo", "test"]
            result = runner.invoke(symmetric_run, args)
            assert result.exit_code == 1
            assert "Cannot use --head option" in result.output

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            # Test args with "=" are passed to ray start
            args = ["--address", "127.0.0.1:6379", "--num-cpus=4", "--", "echo", "test"]
            result = runner.invoke(symmetric_run, args)
            assert result.exit_code == 0

            ray_start_calls = [
                call
                for call in mock_run.call_args_list
                if "ray" in str(call) and "start" in str(call)
            ]
            assert len(ray_start_calls) > 0
            assert "--num-cpus=4" in ray_start_calls[0][0][0]


class PortAllocator:
    """Allocate unique ports for a test, avoiding collisions."""

    def __init__(self):
        self._allocated: set[int] = set()

    def allocate(self) -> int:
        for _ in range(100):
            port = get_current_unused_port()
            if port not in self._allocated:
                self._allocated.add(port)
                return port
        raise RuntimeError("Could not allocate unique port after 100 attempts")

    def build_ray_port_args(self) -> list[str]:
        """Build Ray port arguments for a single node (needs unique ports per node)."""
        worker_ports = ",".join(str(self.allocate()) for _ in range(5))
        return [
            f"--node-manager-port={self.allocate()}",
            f"--object-manager-port={self.allocate()}",
            f"--dashboard-port={self.allocate()}",
            f"--worker-port-list={worker_ports}",
            "--disable-usage-stats",
            "--num-cpus=0",
        ]


def test_symmetric_run_three_node_cluster_simulated(cleanup_ray):
    """Simulate multi-node symmetric_run on a single machine."""

    # Clean slate (important when iterating locally).
    subprocess.run(["ray", "stop", "--force"], capture_output=True)

    ports = PortAllocator()

    # We use the REAL IP for the head, so actual Ray processes can bind to it.
    # For workers, we mock psutil to HIDE this IP so they think they are remote.
    head_ip = _get_non_loopback_ip()
    gcs_port = ports.allocate()
    address = f"{head_ip}:{gcs_port}"

    base_env = os.environ.copy()
    base_env["RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER"] = "1"
    base_env["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

    test_dir = os.path.dirname(__file__)
    wrapper_script = os.path.join(test_dir, "symmetric_run_wrapper.py")
    entrypoint_script = os.path.join(test_dir, "symmetric_run_test_entrypoint.py")

    # Symmetric commands on all nodes
    def build_cmd(port_args: list[str]) -> list[str]:
        return [
            sys.executable,
            wrapper_script,
            "--address",
            address,
            "--min-nodes",
            "3",
            *port_args,
            "--",
            sys.executable,
            entrypoint_script,
        ]

    head_cmd = build_cmd(ports.build_ray_port_args())
    worker_cmds = [build_cmd(ports.build_ray_port_args()) for _ in range(2)]

    head_env = {**base_env, "RAY_ADDRESS": address}
    # So that workers don't think they have the head node's IP.
    worker_env = {**base_env, "MOCK_HIDE_IP": head_ip}

    worker_procs = []
    head_proc = None
    try:
        head_proc = subprocess.Popen(
            head_cmd,
            env=head_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

        # Wait for head to start listening before starting workers.
        def _check_head_ready() -> bool:
            import socket

            host, port_str = address.split(":")
            try:
                with socket.create_connection((host, int(port_str)), timeout=1):
                    return True
            except (socket.timeout, ConnectionRefusedError, OSError):
                return False

        wait_for_condition(_check_head_ready, timeout=280, retry_interval_ms=250)

        for cmd in worker_cmds:
            worker_procs.append(
                subprocess.Popen(
                    cmd,
                    env=worker_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True,
                )
            )
            time.sleep(2)

        head_stdout_b, head_stderr_b = head_proc.communicate(timeout=150)
        head_stdout = head_stdout_b.decode("utf-8", "replace")
        head_stderr = head_stderr_b.decode("utf-8", "replace")
        head_rc = head_proc.returncode

    finally:
        for p in worker_procs:
            _kill_process_group(p)
        if head_proc:
            _kill_process_group(head_proc)

    assert head_rc == 0, f"Head failed: rc={head_rc}\nstderr:\n{head_stderr[-2000:]}"
    assert "On head node" in head_stdout
    assert "ENTRYPOINT_SUCCESS" in head_stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
