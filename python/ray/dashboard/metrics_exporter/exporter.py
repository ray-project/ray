import logging
import threading
import traceback
import time

from ray.dashboard.metrics_exporter import api

logger = logging.getLogger(__file__)


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
        api.ingest_request(self.export_address, self.dashboard_id,
                           self.access_token, ray_config, node_info,
                           raylet_info, tune_info, tune_availability)
        # TODO(sang): Add piggybacking response handler.

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
