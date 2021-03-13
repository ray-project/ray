import ray
import logging
import grpc
import sys

from typing import TYPE_CHECKING, Callable
from threading import Lock

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client import CURRENT_PROTOCOL_VERSION
from ray._private.client_mode_hook import disable_client_hook

if TYPE_CHECKING:
    from ray.util.client.server.server import RayletServicer

logger = logging.getLogger(__name__)


class DataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, basic_service: "RayletServicer",
                 ray_connect_handler: Callable):
        self.basic_service = basic_service
        self._clients_lock = Lock()
        self._num_clients = 0  # guarded by self._clients_lock
        self.ray_connect_handler = ray_connect_handler

    def Datapath(self, request_iterator, context):
        metadata = {k: v for k, v in context.invocation_metadata()}
        client_id = metadata["client_id"]
        if client_id == "":
            logger.error("Client connecting with no client_id")
            return
        logger.info(f"New data connection from client {client_id}")
        try:
            with self._clients_lock:
                with disable_client_hook():
                    if self._num_clients == 0 and not ray.is_initialized():
                        self.ray_connect_handler()
                self._num_clients += 1
            for req in request_iterator:
                resp = None
                req_type = req.WhichOneof("type")
                if req_type == "get":
                    get_resp = self.basic_service._get_object(
                        req.get, client_id)
                    resp = ray_client_pb2.DataResponse(get=get_resp)
                elif req_type == "put":
                    put_resp = self.basic_service._put_object(
                        req.put, client_id)
                    resp = ray_client_pb2.DataResponse(put=put_resp)
                elif req_type == "release":
                    released = []
                    for rel_id in req.release.ids:
                        rel = self.basic_service.release(client_id, rel_id)
                        released.append(rel)
                    resp = ray_client_pb2.DataResponse(
                        release=ray_client_pb2.ReleaseResponse(ok=released))
                elif req_type == "connection_info":
                    resp = ray_client_pb2.DataResponse(
                        connection_info=self._build_connection_response())
                else:
                    raise Exception(f"Unreachable code: Request type "
                                    f"{req_type} not handled in Datapath")
                resp.req_id = req.req_id
                yield resp
        except grpc.RpcError as e:
            logger.debug(f"Closing data channel: {e}")
        finally:
            logger.info(f"Lost data connection from client {client_id}")
            self.basic_service.release_all(client_id)

            with self._clients_lock:
                self._num_clients -= 1

            with disable_client_hook():
                if self._num_clients == 0:
                    ray.shutdown()

    def _build_connection_response(self):
        with self._clients_lock:
            cur_num_clients = self._num_clients
        return ray_client_pb2.ConnectionInfoResponse(
            num_clients=cur_num_clients,
            python_version="{}.{}.{}".format(
                sys.version_info[0], sys.version_info[1], sys.version_info[2]),
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            protocol_version=CURRENT_PROTOCOL_VERSION)
