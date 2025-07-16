import logging

import ray
from .node_killer_base import NodeKillerBase
from ray.core.generated import node_manager_pb2


@ray.remote(num_cpus=0)
class RayletKiller(NodeKillerBase):
    def _kill_resource(self, node_id, node_to_kill_ip, node_to_kill_port):
        if node_to_kill_port is not None:
            try:
                self._kill_raylet(node_to_kill_ip, node_to_kill_port, graceful=False)
            except Exception:
                pass
            logging.info(
                f"Killed node {node_id} at address: "
                f"{node_to_kill_ip}, port: {node_to_kill_port}"
            )
            self.killed.add(node_id)

    def _kill_raylet(self, ip, port, graceful=False):
        import grpc
        from grpc._channel import _InactiveRpcError

        from ray.core.generated import node_manager_pb2_grpc

        raylet_address = f"{ip}:{port}"
        channel = grpc.insecure_channel(raylet_address)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        try:
            stub.ShutdownRaylet(
                node_manager_pb2.ShutdownRayletRequest(graceful=graceful)
            )
        except _InactiveRpcError:
            assert not graceful
