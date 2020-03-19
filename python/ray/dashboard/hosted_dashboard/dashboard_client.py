import json
import requests

from ray.dashboard.hosted_dashboard.exporter import Exporter


class DashboardClient:
    """Manages the communication to external services
       to export metrics.

    Args:
        host(str): Host address of service that are used to authenticate.
        port(str): Port of the host that are used to authenticate.
        dashboard_controller(BaseDashboardController): Dashboard controller to
            run dashboard business logic.
        dashboard_id(str): Unique dashboard ID.
    """

    def __init__(self, host, port, dashboard_controller, dashboard_id):
        self.auth_url = "http://{}:{}/auth".format(host, port)
        self.ingestor_url = "http://{}:{}/ingest".format(host, port)
        self.hosted_dashboard_url = "http://{}:{}/".format(host, port)
        self.timeout = 5.0
        self.dashboard_id = dashboard_id
        self.dashboard_controller = dashboard_controller
        self.is_authenticated = False
        self.exporter = None
        self.auth_info = None

    def _authenticate(self):
        resp = requests.post(
            self.auth_url,
            timeout=self.timeout,
            data=json.dumps({
                "cluster_id": self.dashboard_id
            }))
        status = resp.status_code
        self.auth_info = resp.json()
        if status != 200:
            raise ConnectionError(
                "Failed to authenticate to hosted dashbaord server.")
        self.is_authenticated = True

    def start_exporting_metrics(self):
        if not self.is_authenticated:
            self._authenticate()

        # Exporter is a Python thread that keeps exporting metrics with
        # access token obtained obtained by authentication process.
        self.exporter = Exporter(self.dashboard_id, self.ingestor_url,
                                 self.auth_info.get("access_token"),
                                 self.dashboard_controller)
        self.hosted_dashboard_url = self.auth_info["dashboard_url"]
        self.exporter.start()
