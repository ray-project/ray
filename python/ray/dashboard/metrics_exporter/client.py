import json
import logging
import requests

from ray.dashboard.metrics_exporter.exporter import Exporter

logger = logging.getLogger(__name__)

class MetricsExportClient:
    """Manages the communication to external services to export metrics.

    Args:
        address: Address to export metrics
        dashboard_controller(BaseDashboardController): Dashboard controller to
            run dashboard business logic.
        dashboard_id(str): Unique dashboard ID.
    """

    def __init__(self, address, dashboard_controller, dashboard_id):
        host, port = address.strip().split(":")
        self.auth_url = "http://{}:{}/auth".format(host, port)
        self.ingestor_url = "http://{}:{}/ingest".format(host, port)
        self._dashboard_url = None
        self.timeout = 5.0
        self.dashboard_id = dashboard_id
        self.dashboard_controller = dashboard_controller
        self.exporter = None
        self.auth_info = None
        self.is_authenticated = False
        self.is_exporting_started = False

    def _authenticate(self):
        """
        Return:
            Whether or not the authentication succeed.
        """
        try:
            resp = requests.post(
                self.auth_url,
                timeout=self.timeout,
                data=json.dumps({
                    "cluster_id": self.dashboard_id
                }))
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error occured while connecting to "
                         "a metrics auth server.: {}".format(e))
            return False

        if resp.status_code != 200:
            logger.error("Failed to authenticate to metrics importing "
                         "server. Status code: {}".format(resp.status_code))
            return False
        
        self.auth_info = resp.json()
        self.is_authenticated = True
        self._dashboard_url = self.auth_info["dashboard_url"]
        if not self._dashboard_url.startswith("http://"):
            self._dashboard_url = "http://" + self._dashboard_url
        return True

    @property
    def enabled(self):
        return self.is_authenticated

    @property
    def dashboard_url(self):
        return self._dashboard_url

    def enable(self):
        assert not self.is_authenticated

        succeed = self._authenticate()
        if not succeed:
            logger.error("Failed to authenticate to a metrics auth server.")
            return False
        return True

    def start_exporting_metrics(self):
        """Create a thread to export metrics. 

        Once this function succeeds, it should not be called again.

        Return:
            Whether or not it suceedes to run exporter.
        """
        assert self.is_authenticated
        assert not self.is_exporting_started

        # Exporter is a Python thread that keeps exporting metrics with
        # access token obtained by an authentication process.
        self.exporter = Exporter(self.dashboard_id, self.ingestor_url,
                                 self.auth_info.get("access_token"),
                                 self.dashboard_controller)
        self.exporter.start()
        self.is_exporting_started = True
        return True
