from types import Set

from ray.experimental.client.server.server import RayletServicer
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc


class DataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, basic_service: RayletServicer):
        self.basic_service = basic_service

    def Datapath(self, request_iterator, context):
        held: Set[bytes] = set(bytes)
        client_id = request_iterator.metadata["client_id"]
        for req in request_iterator:
            resp = None
            req_type = req.WhichOneof("type")
            if req_type == "get":
                pass
            elif req_type == "put":
                pass
            elif req_type == "release":
                pass
            else:
                raise Exception("Uncovered request type")
            yield resp

        for i in held:
            self.basic_service.release(client_id, i)
