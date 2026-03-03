import sys
from contextlib import contextmanager
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

            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
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
                with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
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

            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                # Test basic symmetric_run call using CliRunner
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "No separator" in result.output

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "--head", "--", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "Cannot use --head option in symmetric_run." in result.output

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            # Test args with "=" are passed to ray start
            args = ["--address", "127.0.0.1:6379", "--num-cpus=4", "--", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

                ray_start_calls = [
                    call
                    for call in mock_run.call_args_list
                    if "ray" in str(call) and "start" in str(call)
                ]
                assert len(ray_start_calls) > 0
                assert "--num-cpus=4" in ray_start_calls[0][0][0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
