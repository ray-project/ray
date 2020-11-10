import logging
from concurrent import futures
import grpc
from ray import cloudpickle
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
from ray.experimental.client.core_ray_api import set_client_api_as_ray


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self):
        self.object_refs = {}
        self.function_refs = {}

    def GetObject(self, request, context=None):
        objectref = self.object_refs[request.id]
        print("get: %s" % objectref)
        item = ray.get(objectref)
        if item is None:
            return ray_client_pb2.GetResponse(valid=False)
        return ray_client_pb2.GetResponse(valid=True, data=item)

    def PutObject(self, request, context=None):
        objectref = ray.put(request.data)
        self.object_refs[objectref.binary()] = objectref
        print("put: %s" % objectref)
        return ray_client_pb2.PutResponse(id=objectref.binary())

    def Schedule(self, task, context=None):
        print("Got Schedule: ", task)
        funcref = self.object_refs[task.payload_id]
        func = cloudpickle.loads(ray.get(funcref))
        print("Got function back: ", func, type(func))
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Unimplemented")
        return ray_client_pb2.ClientTaskTicket()


def serve(connection_str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer()
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    server.add_insecure_port(connection_str)
    server.start()
    return server


if __name__ == "__main__":
    logging.basicConfig()
    # TODO(barakmich): Perhaps wrap ray init
    ray.init()
    set_client_api_as_ray()
    server = serve("0.0.0.0:50051")
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)
