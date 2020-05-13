import logging
import threading
import traceback
import time

from ray.dashboard.metrics_exporter import api
from ray.dashboard.metrics_exporter.actions import ActionHandler

logger = logging.getLogger(__name__)


class MetricsExportClient:
    """Group of functionalities used by Dashboard to do external communication.

    start_export_metrics should not be called more than once as it can create
    multiple threads that export the same metrics.

    Args:
        address(str): Address to export metrics.
            This should include a web protocol.
        dashboard_controller(BaseDashboardController): Dashboard controller to
            run dashboard business logic.
        dashboard_id(str): Unique dashboard ID.
        exporter(Exporter): Thread to export metrics.
    """

    def __init__(self, address, dashboard_controller, dashboard_id, exporter):
        self.dashboard_id = dashboard_id
        self.address = address
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
        self.auth_info = api.authentication_request(self.auth_url,
                                                    self.dashboard_id)
        self._dashboard_url = "{address}/dashboard/{access_token}".format(
            address=self.address,
            access_token=self.auth_info.access_token_dashboard)
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

        self.exporter.access_token = self.auth_info.access_token_ingest
        self.exporter.start()
        self.is_exporting_started = True
        return True, None


class Exporter(threading.Thread):
    """Python thread that exports metrics periodically.

    Args:
        dashboard_id(str): Unique Dashboard ID.
        address(str): Address to export metrics.
        dashboard_controller(BaseDashboardController): dashboard
            controller for dashboard business logic.
        update_frequency(float): Frequency to export metrics.
    """

    def __init__(self,
                 dashboard_id,
                 address,
                 dashboard_controller,
                 update_frequency=1.0):
        assert update_frequency >= 1.0

        self.dashboard_id = dashboard_id
        self.dashboard_controller = dashboard_controller
        self.action_handler = ActionHandler(dashboard_controller)
        self.export_address = "{}/ingest".format(address)
        self.update_frequency = update_frequency
        self._access_token = None

        super().__init__()

    @property
    def access_token(self):
        return self._access_token

    @access_token.setter
    def access_token(self, access_token):
        self._access_token = access_token

    def export(self, ray_config, node_info, raylet_info, tune_info,
               tune_availability):
        ingest_response = api.ingest_request(
            self.export_address, self.access_token, ray_config, node_info,
            raylet_info, tune_info, tune_availability)
        actions = ingest_response.actions
        self.action_handler.handle_actions(actions)

    def run(self):
        assert self.access_token is not None, (
            "Set access token before running an exporter thread.")
        while True:
            try:
                time.sleep(self.update_frequency)
                self.export(self.dashboard_controller.get_ray_config(),
                            self.dashboard_controller.get_node_info(),
                            self.dashboard_controller.get_raylet_info(),
                            self.dashboard_controller.tune_info(),
                            self.dashboard_controller.tune_availability())
            except Exception as e:
                logger.error("Exception occured while exporting metrics: {}.\n"
                             "Traceback: {}".format(e, traceback.format_exc()))
                continue
