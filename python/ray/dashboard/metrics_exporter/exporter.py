import json
import logging
import requests
import threading
import time

logger = logging.getLogger(__file__)


class Exporter(threading.Thread):
    """Python thread that exports metrics periodically.

    Args:
        dashboard_id(str): Unique Dashboard ID.
        export_address(str): Address to export metrics.
        access_token(str): Access token that is appeneded to
            authorization header.
        dashboard_controller(BaseDashboardController): dashboard
            controller for dashboard business logic.
        update_frequency(float): Frequency to export metrics.
    """

    def __init__(self,
                 dashboard_id,
                 export_address,
                 access_token,
                 dashboard_controller,
                 update_frequency=1.0):
        assert update_frequency >= 1.0
        self.dashboard_id = dashboard_id
        self.dashboard_controller = dashboard_controller
        self.export_address = export_address
        self.update_frequency = update_frequency
        self.access_token = access_token

        super().__init__()

    def export(self, ray_config, node_info, raylet_info, tune_info,
               tune_availability):
        try:
            response = requests.post(
                self.export_address,
                data=json.dumps({
                    "cluster_id": self.dashboard_id,
                    "access_token": self.access_token,
                    "ray_config": ray_config,
                    "node_info": node_info,
                    "raylet_info": raylet_info,
                    "tune_info": tune_info,
                    "tune_availability": tune_availability
                }))
        except requests.exceptions.HTTPError as e:
            logger.error("Failed to export metrics due to "
                        "an error: {}".format(e))

        if response.status_code != 200:
            logger.error("Failed to export metrics\n"
                         "URL: {}.\n"
                         "Status code: {}"
                         .format(self.export_address, response.status_code))

    def run(self):
        while True:
            try:
                time.sleep(self.update_frequency)
                self.export(self.dashboard_controller.get_ray_config(),
                            self.dashboard_controller.get_node_info(),
                            self.dashboard_controller.get_raylet_info(),
                            self.dashboard_controller.tune_info(),
                            self.dashboard_controller.tune_availability())
            except Exception as e:
                logger.error(
                    "Exception occured while exporting metrics: {}".format(e))
                continue
