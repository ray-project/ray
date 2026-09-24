"""Tests for the posix_spawn fix for specific-server spawning (#63202).

The proxier is a multi-threaded gRPC server. Spawning specific-server
children via fork() races with gRPC's poller threads. This fix skips
preexec_fn for specific-server so CPython uses posix_spawn() instead.
"""

import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest


def test_use_posix_spawn_and_fate_share_raises():
    """use_posix_spawn=True + fate_share=True must raise ValueError."""
    from ray._private.services import start_ray_process

    with pytest.raises(ValueError, match="cannot be combined"):
        start_ray_process(
            command=[sys.executable, "-c", "pass"],
            process_type="test",
            fate_share=True,
            use_posix_spawn=True,
        )


@pytest.mark.skipif(
    sys.platform != "linux", reason="posix_spawn optimization is Linux-only"
)
def test_posix_spawn_skips_preexec_fn():
    """When use_posix_spawn=True, preexec_fn must be None."""
    from ray._private.services import ConsolePopen

    with patch.object(
        ConsolePopen, "__init__", return_value=None
    ) as mock_init:
        mock_init.return_value = None
        try:
            from ray._private.services import start_ray_process

            start_ray_process(
                command=[sys.executable, "-c", "pass"],
                process_type="test",
                fate_share=False,
                use_posix_spawn=True,
                stdout_file=subprocess.DEVNULL,
                stderr_file=subprocess.DEVNULL,
            )
        except (OSError, TypeError, AttributeError):
            # ConsolePopen mock doesn't create a real process — that's fine,
            # we only need to inspect how it was called.
            pass

        mock_init.assert_called_once()
        _, kwargs = mock_init.call_args
        assert kwargs.get("preexec_fn") is None, (
            "preexec_fn must be None when use_posix_spawn=True"
        )


@pytest.mark.skipif(
    sys.platform != "linux", reason="posix_spawn optimization is Linux-only"
)
def test_normal_spawn_uses_preexec_fn():
    """Without use_posix_spawn, preexec_fn must be set (on Linux)."""
    from ray._private.services import ConsolePopen

    with patch.object(
        ConsolePopen, "__init__", return_value=None
    ) as mock_init:
        mock_init.return_value = None
        try:
            from ray._private.services import start_ray_process

            start_ray_process(
                command=[sys.executable, "-c", "pass"],
                process_type="test",
                fate_share=False,
                use_posix_spawn=False,
                stdout_file=subprocess.DEVNULL,
                stderr_file=subprocess.DEVNULL,
            )
        except (OSError, TypeError, AttributeError):
            pass

        mock_init.assert_called_once()
        _, kwargs = mock_init.call_args
        assert kwargs.get("preexec_fn") is not None, (
            "preexec_fn must be set when use_posix_spawn=False on Linux"
        )


def test_client_server_specific_server_disables_fate_share():
    """start_ray_client_server must pass fate_share=False for specific-server."""
    with patch("ray._private.services.start_ray_process") as mock_start:
        mock_start.return_value = MagicMock()
        from ray._private.services import start_ray_client_server

        start_ray_client_server(
            address="127.0.0.1:6379",
            ray_client_server_ip="127.0.0.1",
            ray_client_server_port=10001,
            server_type="specific-server",
            fate_share=True,
        )

        mock_start.assert_called_once()
        _, kwargs = mock_start.call_args
        assert kwargs["fate_share"] is False, (
            "fate_share must be False for specific-server"
        )
        assert kwargs["use_posix_spawn"] is True, (
            "use_posix_spawn must be True for specific-server"
        )


def test_client_server_proxy_keeps_fate_share():
    """start_ray_client_server must preserve fate_share for proxy server."""
    with patch("ray._private.services.start_ray_process") as mock_start:
        mock_start.return_value = MagicMock()
        from ray._private.services import start_ray_client_server

        start_ray_client_server(
            address="127.0.0.1:6379",
            ray_client_server_ip="127.0.0.1",
            ray_client_server_port=10001,
            server_type="proxy",
            fate_share=True,
            runtime_env_agent_address="127.0.0.1:8000",
        )

        mock_start.assert_called_once()
        _, kwargs = mock_start.call_args
        assert kwargs["fate_share"] is True, (
            "fate_share must be preserved for proxy server"
        )
        assert kwargs["use_posix_spawn"] is False, (
            "use_posix_spawn must be False for proxy server"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
