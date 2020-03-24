import pytest
import requests

from unittest.mock import patch

from ray.dashboard.metrics_exporter.client import MetricsExportClient
from ray.dashboard.metrics_exporter.exporter import Exporter
from ray.dashboard.metrics_exporter.schema import AuthResponse

MOCK_DASHBOARD_ID = "1234"
MOCK_DASHBOARD_ADDRESS = "127.0.0.1:9081"
MOCK_ACCESS_TOKEN = "1234"


def _setup_client_and_exporter(controller):
    exporter = Exporter(MOCK_DASHBOARD_ID, MOCK_DASHBOARD_ADDRESS, controller)
    client = MetricsExportClient(MOCK_DASHBOARD_ADDRESS, controller,
                                 MOCK_DASHBOARD_ID, exporter)
    return exporter, client


@patch("ray.dashboard.dashboard.DashboardController")
def test_verify_exporter_cannot_run_without_access_token(mock_controller):
    exporter, client = _setup_client_and_exporter(mock_controller)
    # Should raise an assertion error because there's no access token set.
    with pytest.raises(AssertionError):
        exporter.run()


@patch("ray.dashboard.dashboard.DashboardController")
@patch(
    "ray.dashboard.metrics_exporter.api.authentication_request",
    side_effect=requests.exceptions.HTTPError)
def test_client_invalid_request_status_returned(auth_request, mock_controller):
    """
    If authentication request fails with an invalid status code,
       `start_exporting_metrics` should fail.
    """
    exporter, client = _setup_client_and_exporter(mock_controller)

    # authenticate should throw an exception because API request fails.
    with pytest.raises(requests.exceptions.HTTPError):
        client._authenticate()

    # This should fail because authentication throws an exception.
    result, error = client.start_exporting_metrics()
    assert result is False


@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_authentication(auth_request, mock_controller):
    auth_request.return_value = AuthResponse(
        dashboard_url=MOCK_DASHBOARD_ADDRESS, access_token=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    assert client.enabled is False
    client._authenticate()
    assert client.dashboard_url == MOCK_DASHBOARD_ADDRESS
    assert client.enabled is True


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_without_authentication(
        auth_request, mock_controller, start):
    """
    `start_exporting_metrics` should trigger authentication if users
        are not authenticated.
    """
    auth_request.return_value = AuthResponse(
        dashboard_url=MOCK_DASHBOARD_ADDRESS, access_token=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    # start_exporting_metrics should succeed.
    result, error = client.start_exporting_metrics()
    assert result is True
    assert error is None
    assert client.enabled is True


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_with_authentication(auth_request,
                                                     mock_controller, start):
    """
    If users are already authenticated, `start_exporting_metrics`
       should not authenticate users.
    """
    auth_request.return_value = AuthResponse(
        dashboard_url=MOCK_DASHBOARD_ADDRESS, access_token=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)
    # Already authenticated.
    client._authenticate()
    assert client.enabled is True

    result, error = client.start_exporting_metrics()
    # Auth request should be called only once because
    # it was already authenticated.
    auth_request.call_count == 1
    assert result is True
    assert error is None


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_succeed(auth_request, mock_controller, start):
    auth_request.return_value = AuthResponse(
        dashboard_url=MOCK_DASHBOARD_ADDRESS, access_token=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    result, error = client.start_exporting_metrics()
    assert result is True
    assert error is None
    assert client.is_exporting_started is True
    start.call_count == 1

    with pytest.raises(AssertionError):
        client.start_exporting_metrics()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
