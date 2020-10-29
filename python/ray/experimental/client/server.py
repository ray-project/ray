
import logging
from concurrent import futures
import grpc
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self):
        pass

    def GetObject(self, request, context=None):
        data = self.objects.get(request.id)
        if data is None:
            return ray_client_pb2.GetResponse(valid=False)
        return ray_client_pb2.GetResponse(valid=True, data=data)

    def PutObject(self, request, context=None):
        id = self.objects.put(request.data)
        return ray_client_pb2.PutResponse(id=id)

    def Schedule(self, task, context=None):
        return_val = self.executor.execute(task, context)
        return ray_client_pb2.TaskTicket(return_id=return_val)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer()
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    server.add_insecure_port('0.0.0.0:50051')
    server.start()
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    logging.basicConfig()
    serve()
