import ray
import logging
import grpc
import sys

from typing import TYPE_CHECKING
from threading import Lock

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

if TYPE_CHECKING:
    from ray.util.client.server.server import RayletServicer

logger = logging.getLogger(__name__)


class DataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, basic_service: "RayletServicer"):
        self.basic_service = basic_service
        self._clients_lock = Lock()
        self._num_clients = 0  # guarded by self._clients_lock

    def Datapath(self, request_iterator, context):
        metadata = {k: v for k, v in context.invocation_metadata()}
        client_id = metadata["client_id"]
        if client_id == "":
            logger.error("Client connecting with no client_id")
            return
        logger.info(f"New data connection from client {client_id}")
        try:
            with self._clients_lock:
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
                    with self._clients_lock:
                        cur_num_clients = self._num_clients
                    info = ray_client_pb2.ConnectionInfoResponse(
                        num_clients=cur_num_clients,
                        python_version="{}.{}.{}".format(
                            sys.version_info[0], sys.version_info[1],
                            sys.version_info[2]),
                        ray_version=ray.__version__,
                        ray_commit=ray.__commit__)
                    resp = ray_client_pb2.DataResponse(connection_info=info)
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
