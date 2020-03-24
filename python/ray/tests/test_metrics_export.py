import pytest
import requests
import unittest

from unittest.mock import patch

from ray.dashboard.dashboard import DashboardController
from ray.dashboard.metrics_exporter.client import MetricsExportClient
from ray.dashboard.metrics_exporter.exporter import Exporter
from ray.dashboard.metrics_exporter.schema import AuthResponse


class TestMetricsExport(unittest.TestCase):

    MOCK_DASHBOARD_ID = "1234"
    MOCK_DASHBOARD_ADDRESS = "127.0.0.1:9081"
    MOCK_ACCESS_TOKEN = "1234"

    @pytest.mark.xfail(raises=AssertionError)
    @patch("ray.dashboard.dashboard.DashboardController")
    def test_verify_exporter_cannot_run_without_access_token(self, mock_controller):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        # Should raise an assertion error because there's no access token set.
        exporter.run()

    @pytest.mark.xfail(raises=requests.exceptions.HTTPError)
    @patch("ray.dashboard.dashboard.DashboardController")
    @patch("ray.dashboard.metrics_exporter.api.authentication_request", side_effect=requests.exceptions.HTTPError)
    def test_client_invalid_request_status_returned(self, auth_request, mock_controller):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        client = MetricsExportClient(self.MOCK_DASHBOARD_ADDRESS,
            mock_controller, self.MOCK_DASHBOARD_ID, exporter)
        
        # This should fail because authentication throws an exception.
        result, error = client.start_exporting_metrics()
        assert result == False

    @pytest.mark.xfail(raises=requests.exceptions.HTTPError)
    @patch("ray.dashboard.dashboard.DashboardController")
    @patch("ray.dashboard.metrics_exporter.api.authentication_request")
    def test_authentication(self, auth_request, mock_controller):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        auth_request.return_value = AuthResponse(
            dashboard_url=self.MOCK_DASHBOARD_ADDRESS,
            access_token=self.MOCK_ACCESS_TOKEN)
        client = MetricsExportClient(self.MOCK_DASHBOARD_ADDRESS,
            mock_controller, self.MOCK_DASHBOARD_ID, exporter)

        assert client.enabled == False
        client._authenticate()
        assert client.dashboard_url == self.MOCK_DASHBOARD_ADDRESS
        assert client.enabled == True

    @patch.object(Exporter, "start")
    @patch("ray.dashboard.dashboard.DashboardController")
    @patch("ray.dashboard.metrics_exporter.api.authentication_request")
    def test_start_exporting_metrics_without_authentication(self, auth_request, mock_controller, start):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        auth_request.return_value = AuthResponse(
            dashboard_url=self.MOCK_DASHBOARD_ADDRESS,
            access_token=self.MOCK_ACCESS_TOKEN)
        client = MetricsExportClient(self.MOCK_DASHBOARD_ADDRESS,
            mock_controller, self.MOCK_DASHBOARD_ID, exporter)
        
        # start_exporting_metrics should succeed.
        result, error = client.start_exporting_metrics()
        assert result == True
        assert error == None

    @patch.object(Exporter, "start")
    @patch("ray.dashboard.dashboard.DashboardController")
    @patch("ray.dashboard.metrics_exporter.api.authentication_request")
    def test_start_exporting_metrics_with_authentication(self, auth_request, mock_controller, start):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        auth_request.return_value = AuthResponse(
            dashboard_url=self.MOCK_DASHBOARD_ADDRESS,
            access_token=self.MOCK_ACCESS_TOKEN)
        client = MetricsExportClient(self.MOCK_DASHBOARD_ADDRESS,
            mock_controller, self.MOCK_DASHBOARD_ID, exporter)
        # Already authenticated.
        client._authenticate()
        assert client.enabled == True

        result, error = client.start_exporting_metrics()
        # Auth request should be called only once because 
        # it was already authenticated.
        auth_request.call_count == 1
        assert result == True
        assert error == None

    @patch.object(Exporter, "start")
    @patch("ray.dashboard.dashboard.DashboardController")
    @patch("ray.dashboard.metrics_exporter.api.authentication_request")
    def test_start_exporting_metrics_succeed(self, auth_request, mock_controller, start):
        exporter = Exporter(self.MOCK_DASHBOARD_ID,
                            self.MOCK_DASHBOARD_ADDRESS,
                            mock_controller)
        auth_request.return_value = AuthResponse(
            dashboard_url=self.MOCK_DASHBOARD_ADDRESS,
            access_token=self.MOCK_ACCESS_TOKEN)
        client = MetricsExportClient(self.MOCK_DASHBOARD_ADDRESS,
            mock_controller, self.MOCK_DASHBOARD_ID, exporter)

        result, error = client.start_exporting_metrics()
        assert result == True
        assert error == None
        start.call_count == 1


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
