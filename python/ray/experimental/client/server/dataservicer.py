import logging
import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc


logger = logging.getLogger(__name__)


class DataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, basic_service: "RayletServicer"):
        self.basic_service = basic_service

    def Datapath(self, request_iterator, context):
        metadata = {k: v for k, v in context.invocation_metadata()}
        client_id = metadata["client_id"]
        if client_id == "":
            logging.error("Client connecting with no client_id")
            return
        logging.info(f"New data connection from client {client_id}")
        try:
            for req in request_iterator:
                resp = None
                req_type = req.WhichOneof("type")
                if req_type == "get":
                    get_resp = self.basic_service._get_object(req.get, client_id)
                    resp = ray_client_pb2.DataResponse(get=get_resp)
                elif req_type == "put":
                    put_resp = self.basic_service._put_object(req.put, client_id)
                    resp = ray_client_pb2.DataResponse(put=put_resp)
                elif req_type == "release":
                    released = True
                    for rel_id in req.release.ids:
                        rel = self.basic_service.release(client_id, rel_id)
                        released = rel and released
                    resp = ray_client_pb2.DataResponse(
                        release=ray_client_pb2.ReleaseResponse(ok=released))
                else:
                    raise Exception("Uncovered request type")
                resp.req_id = req.req_id
                yield resp
        except grpc.RpcError as e:
            logging.debug("Closing channel: {e}")
        finally:
            logging.info(f"Lost data connection from client {client_id}")
            self.basic_service.release_all(client_id)
