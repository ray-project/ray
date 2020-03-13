import json
import logging
import requests
import threading
import time

import grpc

from ray.core.generated import dashboard_pb2
from ray.core.generated import dashboard_pb2_grpc

logger = logging.getLogger(__file__)

class Exporter(threading.Thread):
    """Thread that keeps running and export metrics"""

    def __init__(self,
                 cluster_id,
                 export_address,
                 access_token,
                 dashboard_controller,
                 update_frequency=1.0):
        self.cluster_id = cluster_id
        self.dashboard_controller = dashboard_controller
        self.export_address = export_address
        if not self.export_address.startswith('http://'):
            self.export_address = 'http://' + self.export_address
        self.update_frequency = update_frequency
        self.access_token = access_token

        super().__init__()

    def export(self, node_info, raylet_info):
        response = requests.post(self.export_address, data=json.dumps({
            "cluster_id": self.cluster_id,
            "access_token": self.access_token,
            "node_info": node_info,
            "raylet_info": raylet_info
        }))

    def run(self):
        while True:
            try:
                time.sleep(self.update_frequency)
                self.export(self.dashboard_controller.get_node_info(), self.dashboard_controller.get_raylet_info())
            except Exception as e:
                logger.error("Exception occured while exporting metrics: {}".format(e))
                continue
