from concurrent import futures
from ray.util.client.common import CLIENT_SERVER_MAX_THREADS, GRPC_OPTIONS
import grpc

from typing import Any

import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc


class MiddlemanDataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self):
        self.stub = None

    def set_channel(self, channel):
        self.stub = None

    def Datapath(self, request_iterator, context):
        yield from self.stub.Datapath(request_iterator, context)


class MiddlemanLogServicer(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self):
        self.stub = None

    def set_channel(self, channel):
        self.stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)

    def Logstream(self, request_iterator, context):
        yield from self.stub.Logstream(request_iterator, context)


class MiddlemanRayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self):
        self.stub = None

    def set_channel(self, channel):
        self.stub = ray_client_pb2_grpc.RayletDriverStub(channel)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stub, name)


class MiddlemanServer:
    def __init__(self,
                 listen_addr="localhost:10011",
                 real_addr="localhost:50051"):
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
            options=GRPC_OPTIONS)
        self.task_servicer = MiddlemanRayletServicer()
        self.data_servicer = MiddlemanDataServicer()
        self.logs_servicer = MiddlemanLogServicer()
        ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
            self.task_servicer, self.server)
        ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(
            self.data_servicer, self.server)
        ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(
            self.logs_servicer, self.server)
        self.real_addr = real_addr
        self.server.add_insecure_port(listen_addr)
        self.channel = None
        self.reset_channel()

    def reset_channel(self):
        if self.channel:
            self.channel.close()
        self.channel = grpc.insecure_channel(
            self.real_addr, options=GRPC_OPTIONS)
        grpc.channel_ready_future(self.channel)
        self.task_servicer.set_channel(self.channel)
        self.data_servicer.set_channel(self.channel)
        self.logs_servicer.set_channel(self.channel)

    def start(self):
        self.server.start()

    def stop(self, grace: int):
        self.server.stop(grace)
