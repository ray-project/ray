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
                 export_address,
                 access_token,
                 dashboard_controller,
                 update_frequency=1.0):
        self.dashboard_controller = dashboard_controller
        self.export_address = export_address
        self.update_frequency = update_frequency
        # SANG-TODO delete it.
        # self.channel = grpc.insecure_channel(self.export_address)
        # self.stub = dashboard_pb2_grpc.DashboardServiceStub(self.channel)

        super().__init__()

    def export(self, node_info, raylet_info):
        response = requests.post(self.export_address, data=json.dumps({
            "node_info": node_info,
            "raylet_info": raylet_info
        }))

    # def export_node_info(self, data: dict):
    #     request = dashboard_pb2.NodeInfoEventRequest(
    #         json_data=json.dumps(data).encode("utf-8"))
    #     self.stub.NodeInfoEvent.future(request)

    # def export_raylet_info(self, data: dict):
    #     request = dashboard_pb2.RayletInfoEventRequest(
    #         json_data=json.dumps(data).encode("utf-8"))
    #     self.stub.RayletInfoEvent.future(request)

    def run(self):
        while True:
            try:
                time.sleep(self.update_frequency)
                self.export(self.dashboard_controller.get_node_info(), self.dashboard_controller.get_raylet_info())
                # self.export_node_info(self.dashboard_controller.get_node_info())
                # self.export_raylet_info(
                #     self.dashboard_controller.get_raylet_info())
            except Exception as e:
                logger.error("Exception occured while exporting metrics: {}".format(e))
                continue
