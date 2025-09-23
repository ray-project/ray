import sys
import threading
from contextlib import contextmanager
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import ray
import ray.scripts.scripts as scripts


@contextmanager
def _setup_mock_network_utils(curr_ip, head_ip):
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


def _run_head_and_workers(
    symmetric_run_cmd, args: List[str], head_ip: str, worker_ips: List[str]
):
    """Run symmetric_run concurrently on head and worker nodes."""
    expected_workers = len(worker_ips)
    head_ready = threading.Event()
    workers_ready = threading.Event()
    worker_counter = {"count": 0}
    counter_lock = threading.Lock()
    address_arg = args[args.index("--address") + 1]
    port = int(address_arg.split(":")[1])

    def mark_worker_ready():
        with counter_lock:
            worker_counter["count"] += 1
            if worker_counter["count"] == expected_workers:
                workers_ready.set()

    def fake_check_head_node_ready(address, timeout=None):
        # Wait until head reports readiness.
        if not head_ready.wait(timeout):
            return False
        mark_worker_ready()
        return True

    def fake_check_cluster_ready(nnodes, timeout=None):
        head_ready.set()
        if expected_workers == 0:
            return True
        ready = workers_ready.wait(timeout)
        return ready

    head_result = {}
    worker_results = [{} for _ in worker_ips]

    def run_head():
        runner = CliRunner()
        with _setup_mock_network_utils(head_ip, head_ip):
            head_result["result"] = runner.invoke(symmetric_run_cmd, args)

    def run_worker(idx, worker_ip):
        runner = CliRunner()
        with _setup_mock_network_utils(head_ip, worker_ip):
            worker_results[idx]["result"] = runner.invoke(symmetric_run_cmd, args)

    threads = []
    try:
        with patch(
            "ray.scripts.symmetric_run.check_ray_already_started", return_value=False
        ), patch(
            "ray.scripts.symmetric_run.check_cluster_ready",
            side_effect=fake_check_cluster_ready,
        ), patch(
            "ray.scripts.symmetric_run.check_head_node_ready",
            side_effect=fake_check_head_node_ready,
        ):
            head_thread = threading.Thread(target=run_head, name="head-thread")
            threads.append(head_thread)
            for idx, worker_ip in enumerate(worker_ips):
                t = threading.Thread(
                    target=run_worker,
                    args=(idx, worker_ip),
                    name=f"worker-thread-{idx}",
                )
                threads.append(t)

            for t in threads:
                t.start()

            for t in threads:
                t.join(timeout=120)  # TODO: reduce timeout
    finally:
        head_ready.set()
        workers_ready.set()

    for t in threads:
        assert not t.is_alive(), f"Thread {t.name} did not finish in time"

    return head_result.get("result"), [r.get("result") for r in worker_results]


@pytest.fixture
def cleanup_ray():
    """Shutdown all ray instances"""
    yield
    runner = CliRunner()
    runner.invoke(scripts.stop, ["--force"])
    ray.shutdown()


def test_symmetric_run_basic_interface(monkeypatch, cleanup_ray):
    """Test basic symmetric_run interface with minimal arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    # Mock subprocess.run to avoid actually starting Ray
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
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


def test_symmetric_run_worker_node_behavior(monkeypatch, cleanup_ray):
    """Test symmetric_run behavior when not on the head node."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0

        with _setup_mock_network_utils("192.168.1.100", "192.168.1.101"):
            # Mock socket connection check to simulate head node ready
            with patch("socket.socket") as mock_socket:
                mock_socket_instance = MagicMock()
                mock_socket_instance.connect_ex.return_value = 0
                mock_socket.return_value.__enter__.return_value = mock_socket_instance

                # Test worker node behavior
                args = ["--address", "192.168.1.100:6379", "--", "echo", "test"]
                with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
                    with patch(
                        "ray.scripts.symmetric_run.check_head_node_ready"
                    ) as mock_check_head_node_ready:
                        mock_check_head_node_ready.return_value = True
                        result = runner.invoke(symmetric_run, args)
                        assert result.exit_code == 0

                # Verify that subprocess.run was called
                assert mock_run.called
                calls = mock_run.call_args_list

                # Should have called ray start with --address (worker mode)
                ray_start_calls = [
                    call
                    for call in calls
                    if "ray" in str(call) and "start" in str(call)
                ]
                assert len(ray_start_calls) > 0

                # Check that it's in worker mode (--address instead of --head)
                start_call = ray_start_calls[0]
                start_args = start_call[0][0]
                assert "--address" in start_args
                assert "192.168.1.100:6379" in start_args
                assert "--head" not in start_args
                assert "--block" in start_args  # Worker nodes should block


def test_symmetric_run_arg_validation(monkeypatch, cleanup_ray):
    """Test that symmetric_run validates arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    # Mock subprocess.run to avoid actually starting Ray
    with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
                # Test basic symmetric_run call using CliRunner
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "echo", "test"]
            with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "No separator" in result.output

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "--head", "--", "echo", "test"]
            with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "Cannot use --head option in symmetric_run." in result.output

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            # Test args with "=" are passed to ray start
            args = ["--address", "127.0.0.1:6379", "--num-cpus=4", "--", "echo", "test"]
            with patch("sys.argv", ["ray.scripts.symmetric_run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

                ray_start_calls = [
                    call
                    for call in mock_run.call_args_list
                    if "ray" in str(call) and "start" in str(call)
                ]
                assert len(ray_start_calls) > 0
                assert "--num-cpus=4" in ray_start_calls[0][0][0]


def test_symmetric_run_multi_node(monkeypatch, cleanup_ray):
    """
    Test symmetric_run with a simulated 3-node (1 head + 2 workers) cluster.
    """
    from ray.scripts.symmetric_run import symmetric_run

    head_ip = "127.0.0.1"
    address = f"{head_ip}:6379"
    worker_ips = ["10.0.0.2", "10.0.0.3"]

    common_args = ["--address", address, "--min-nodes", "3", "--", "echo", "ok"]

    head_result, worker_results = _run_head_and_workers(
        symmetric_run, common_args, head_ip, worker_ips
    )

    assert head_result is not None
    assert all(result is not None for result in worker_results)
    assert head_result.exception is None
    assert all(result.exception is None for result in worker_results)
    assert head_result.exit_code == 0
    assert all(result.exit_code == 0 for result in worker_results)
    assert "On head node. Starting Ray cluster head..." in head_result.output
    assert "Running command on head node: ['echo', 'ok']" in head_result.output
    for result in worker_results:
        assert "On worker node. Connecting to Ray cluster" in result.output
        assert address in result.output


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
