from typing import Callable, List, Tuple

import grpc


import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import GRPC_OPTIONS


INITIAL_TIMEOUT_SEC = 5

def connect_to_coordinator(conn_str: str, 
    secure: bool = False, 
    metadata: List[Tuple[str, str]] = None) -> int:
    """
    Connects to RayClient Coordinator Server and returns the port to
    connect to an actual RayClient Server.
    """ 
    if metadata == None:
        metadata = []
    channel = None
    if secure:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            conn_str, credentials, options=GRPC_OPTIONS)
    else:
        channel = grpc.insecure_channel(
            conn_str, options=GRPC_OPTIONS)
    
    grpc.channel_ready_future(channel).result(timeout=INITIAL_TIMEOUT_SEC)
    server_stub = ray_client_pb2_grpc.ServerCoordinatorStub(channel)
    resp = server_stub.ChooseServer(ray_client_pb2.ServerCoordinatorRequest(), metadata=metadata)
    return resp.port


def get_disconnect_handler(coordinator_conn_str: str) -> Callable[[int], None]:
    def disconnect(port: int):
        channel = grpc.insecure_channel(coordinator_conn_str)
        grpc.channel_ready_future(channel).result(timeout=INITIAL_TIMEOUT_SEC)
        coordinator_stub = ray_client_pb2_grpc.ServerCoordinatorStub(channel)
        coordinator_stub.RemoveStub(ray_client_pb2.RemoveServerRequest(port=port))
    return disconnect