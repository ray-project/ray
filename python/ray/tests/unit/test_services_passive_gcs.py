# Unit tests for ray._private.services.start_api_server against a passive GCS.
import sys
from unittest.mock import MagicMock

import pytest

import ray._private.services as services
import ray._private.utils
from ray.experimental import internal_kv

DASHBOARD_URL = "127.0.0.1:8265"


@pytest.fixture
def dashboard_process(monkeypatch):
    """Stub out the dashboard subprocess and the GCS client services.py builds."""
    process_info = MagicMock()
    process_info.process.poll.return_value = None
    process_info.command = None

    def fake_start_ray_process(command, *args, **kwargs):
        process_info.command = command
        return process_info

    monkeypatch.setattr(services, "start_ray_process", fake_start_ray_process)
    monkeypatch.setattr(services, "GcsClient", MagicMock())
    monkeypatch.setattr(
        ray._private.utils, "get_dashboard_dependency_error", lambda: None
    )
    yield process_info
    # _initialize_internal_kv() leaks the stub client into a module global.
    internal_kv._internal_kv_reset()


def _start_api_server(
    *,
    gcs_is_passive,
    raise_on_failure=True,
    logdir="/tmp/ray/logs",
    tracing_startup_hook=None,
):
    return services.start_api_server(
        include_dashboard=True,
        raise_on_failure=raise_on_failure,
        host="127.0.0.1",
        gcs_address="127.0.0.1:6379",
        cluster_id_hex="0" * 56,
        node_ip_address="127.0.0.1",
        temp_dir="/tmp/ray",
        logdir=logdir,
        session_dir="/tmp/ray/session",
        gcs_is_passive=gcs_is_passive,
        tracing_startup_hook=tracing_startup_hook,
    )


def test_start_api_server_does_not_wait_for_a_passive_gcs(dashboard_process):
    dashboard_url, process_info = _start_api_server(gcs_is_passive=True)

    # No address to report, but the dashboard is running and must stay tracked.
    assert dashboard_url is None
    assert process_info is dashboard_process
    internal_kv.internal_kv_get_gcs_client().internal_kv_get.assert_not_called()


def test_start_api_server_reports_the_address_when_active(dashboard_process):
    gcs_client = services.GcsClient.return_value
    gcs_client.internal_kv_get.return_value = DASHBOARD_URL.encode()

    dashboard_url, process_info = _start_api_server(gcs_is_passive=False)

    assert dashboard_url == DASHBOARD_URL
    assert process_info is dashboard_process


@pytest.mark.parametrize(
    "hook, expected", [("my.module:hook", "my.module:hook"), (None, "")]
)
def test_start_api_server_passes_the_tracing_startup_hook(
    dashboard_process, hook, expected
):
    _start_api_server(gcs_is_passive=True, tracing_startup_hook=hook)

    assert f"--tracing-startup-hook={expected}" in dashboard_process.command


def test_start_api_server_still_fails_on_a_dead_dashboard(dashboard_process):
    gcs_client = services.GcsClient.return_value
    gcs_client.internal_kv_get.return_value = None
    dashboard_process.process.poll.return_value = 1

    # logdir="" skips the log tailing and raises the generic failure instead.
    with pytest.raises(Exception, match="Failed to start a dashboard"):
        _start_api_server(gcs_is_passive=False, logdir="")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
