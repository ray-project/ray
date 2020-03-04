import requests

from ray.dashboard.hosted_dashboard.exporter import Exporter


class DashboardClient:
    """Managing the authentication to external services.

    Args:
        host: Host address of service that are used to authenticate.
        port: Port of the host that are used to authenticate.

    Attributes:
        exporter(Exporter): Exporter thread that keeps exporting
            metrics to the external services.
    """

    def __init__(self, host, port, dashboard_controller):
        self.auth_url = "http://{}:{}/auth".format(host, port)
        self.timeout = 5.0

        self.auth_info = self._connect()
        self.exporter = Exporter(
            self.auth_info.get("ingestor_url"),
            self.auth_info.get("access_token"), dashboard_controller)
        self.hosted_dashboard_url = self.auth_info["dashboard_url"]

    def _authorize(self):
        resp = requests.get(self.auth_url, timeout=self.timeout)
        status = resp.status_code
        json_response = resp.json()
        return status, json_response

    def _connect(self):
        status, json_response = self._authorize()
        if status != 200:
            raise ConnectionError(
                "Failed to authorize to hosted dashbaord server.")

        return json_response

    def start_exporting_metrics(self):
        """Run an exporter thread to export metrics"""
        self.exporter.start()
