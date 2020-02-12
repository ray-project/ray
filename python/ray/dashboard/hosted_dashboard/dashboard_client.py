import requests

from ray.dashboard.hosted_dashboard.exporter import Exporter

class DashboardClient:
    """Managing the communication to hosted dashboard.

    Args:
        dashboard_controller (BaseDashboardController): Dashboard
            controller that is used to export metrics.
        hosted_dashboard_addr (str): The address users host their
            dashboard.

    Attributes:
        ingestor_url(str): Address that metrics will be exported.
        exporter(Exporter): Exporter thread that keeps exporting
            metrics to the external services.
    """

    def __init__(self, host, port, dashboard_controller):
        self.auth_url = "http://{}:{}/auth".format(host, port)
        self.timeout = 5.0

        self.auth_info = self._connect()
        self.exporter = Exporter(
                            self.auth_info.get("ingestor_url"), 
                            self.auth_info.get("access_token"), 
                            dashboard_controller)
    
    def _authorize(self):
        resp = requests.get(self.auth_url, timeout=self.timeout)
        status = resp.status_code
        json_response = resp.json()
        return status, json_response["ingestor_url"], json_response["access_token"]

    def _connect(self):
        status, ingestor_url, access_token = self._authorize()
        if status != 200:
            raise ConnectionError("Failed to authorize to hosted dashbaord server.")

        auth_info = {
            "ingestor_url": ingestor_url,
            "access_token": access_token
        }
        return auth_info

    def start_exporting_metrics(self):
        """Run an exporter thread to export metrics"""
        self.exporter.start()
