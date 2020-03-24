import logging
import traceback
import re
import requests

from ray.dashboard.metrics_exporter import api
from ray.dashboard.metrics_exporter.exporter import Exporter

logger = logging.getLogger(__name__)


class MetricsExportClient:
    """Group of functionalities used by Dashboard to do external communication.

    start_export_metrics should not be called more than once as it can create 
    multiple threads that export the same metrics.

    Args:
        address: Address to export metrics
        dashboard_controller(BaseDashboardController): Dashboard controller to
            run dashboard business logic.
        dashboard_id(str): Unique dashboard ID.
    """

    def __init__(self,
                 address,
                 dashboard_controller,
                 dashboard_id,
                 exporter: Exporter):
        self.dashboard_id = dashboard_id
        self.auth_url = "{}/auth".format(address)
        self.dashboard_controller = dashboard_controller
        self.exporter = exporter

        # Data obtained from requests.
        self._dashboard_url = None
        self.auth_info = None

        # Client states
        self.is_authenticated = False
        self.is_exporting_started = False

    def _authenticate(self):
        """
        Return:
            Whether or not the authentication succeed.
        """
        self.auth_info = api.authentication_request(
            self.auth_url, self.dashboard_id)
        self._dashboard_url = self.auth_info.dashboard_url
        self.is_authenticated = True

    @property
    def enabled(self):
        return self.is_authenticated

    @property
    def dashboard_url(self):
        # This function should be used only after authentication succeed.
        assert self._dashboard_url is not None, (
                "dashboard url should be obtained by "
                "`start_exporting_metrics` method first.")
        return self._dashboard_url

    def start_exporting_metrics(self):
        """Create a thread to export metrics.

        Once this function succeeds, it should not be called again.

        Return:
            Whether or not it suceedes to run exporter.
        """
        assert not self.is_exporting_started
        if not self.is_authenticated:
            try:
                self._authenticate()
            except Exception as e:
                error = ("Authentication failed with an error: {}\n"
                         "Traceback: {}".format(e, traceback.format_exc()))
                logger.error(error)
                return False, error

        # Exporter is a Python thread that keeps exporting metrics with
        # access token obtained by an authentication process.
        self.exporter.access_token = self.auth_info.access_token
        self.exporter.start()
        self.is_exporting_started = True
        return True, None
