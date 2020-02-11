import json
import threading
import time

import grpc

from ray.core.generated import dashboard_pb2
from ray.core.generated import dashboard_pb2_grpc


class Exporter(threading.Thread):
    """Thread that keeps running and export metrics"""

    def __init__(self,
                 export_address,
                 dashboard_controller,
                 update_frequency=1.0):
        self.dashboard_controller = dashboard_controller
        self.export_address = export_address
        self.update_frequency = update_frequency
        self.channel = grpc.insecure_channel(self.export_address)
        self.stub = dashboard_pb2_grpc.DashboardServiceStub(self.channel)

        super().__init__()

    def export_node_info(self, data: dict):
        request = dashboard_pb2.NodeInfoEventRequest(
            json_data=json.dumps(data).encode("utf-8"))
        self.stub.NodeInfoEvent.future(request)

    def export_raylet_info(self, data: dict):
        request = dashboard_pb2.RayletInfoEventRequest(
            json_data=json.dumps(data).encode("utf-8"))
        self.stub.RayletInfoEvent.future(request)

    def export_log_file(self, data: dict):
        raise NotImplementedError("Not implemented yet.")

    def export_error_info(self, data: dict):
        raise NotImplementedError("Not implemented yet.")

    def export_profiling_status(self, data: dict):
        raise NotImplementedError("Not implemented yet.")

    def export_profiling_info(self, data: dict):
        raise NotImplementedError("Not implemented yet.")

    def run(self):
        # TODO(sang): Add error handling.
        while True:
            time.sleep(self.update_frequency)
            self.export_node_info(self.dashboard_controller.get_node_info())
            self.export_raylet_info(self.dashboard_controller.get_raylet_info())
