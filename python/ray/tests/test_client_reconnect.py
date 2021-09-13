from concurrent import futures
import contextlib
import logging
import threading
import sys
from ray.util.client.common import CLIENT_SERVER_MAX_THREADS, GRPC_OPTIONS
import grpc

import time
import random
from typing import Optional

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

import ray.util.client.server.server as ray_client_server
import ray


class MiddlemanDataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    def __init__(self, on_response=None):
        self.stub = None
        self.on_response = on_response

    def set_channel(self, channel):
        self.stub = ray_client_pb2_grpc.RayletDataStreamerStub(channel)

    def Datapath(self, request_iterator, context):
        for response in self.stub.Datapath(
                request_iterator, metadata=context.invocation_metadata()):
            if self.on_response:
                self.on_response(response)
            yield response


class MiddlemanLogServicer(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self, on_response=None):
        self.stub = None
        self.on_response = on_response

    def set_channel(self, channel):
        self.stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)

    def Logstream(self, request_iterator, context):
        for response in self.stub.Logstream(
                request_iterator, metadata=context.invocation_metadata()):
            if self.on_response:
                self.on_response(response)
            yield response


class MiddlemanRayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, on_request=None, on_response=None):
        self.stub = None
        self.on_request = on_request
        self.on_response = on_response

    def set_channel(self, channel):
        self.stub = ray_client_pb2_grpc.RayletDriverStub(channel)

    def _call_inner_function(
            self, request, context,
            method: str) -> Optional[ray_client_pb2_grpc.RayletDriverStub]:
        if self.on_request:
            self.on_request(request)
        response = getattr(self.stub, method)(
            request, metadata=context.invocation_metadata())
        if self.on_response:
            self.on_response(response)
        return response

    def Init(self, request, context=None) -> ray_client_pb2.InitResponse:
        return self._call_inner_function(request, context, "Init")

    def KVPut(self, request, context=None) -> ray_client_pb2.KVPutResponse:
        return self._call_inner_function(request, context, "KVPut")

    def KVGet(self, request, context=None) -> ray_client_pb2.KVGetResponse:
        return self._call_inner_function(request, context, "KVGet")

    def KVDel(self, request, context=None) -> ray_client_pb2.KVDelResponse:
        return self._call_inner_function(request, context, "KVDel")

    def KVList(self, request, context=None) -> ray_client_pb2.KVListResponse:
        return self._call_inner_function(request, context, "KVList")

    def KVExists(self, request,
                 context=None) -> ray_client_pb2.KVExistsResponse:
        return self._call_inner_function(request, context, "KVExists")

    def ListNamedActors(self, request, context=None
                        ) -> ray_client_pb2.ClientListNamedActorsResponse:
        return self._call_inner_function(request, context, "ListNamedActors")

    def ClusterInfo(self, request,
                    context=None) -> ray_client_pb2.ClusterInfoResponse:
        return self._call_inner_function(request, context, "ClusterInfo")

    def Terminate(self, req, context=None):
        return self._call_inner_function(req, context, "Terminate")

    def GetObject(self, request, context=None):
        return self._call_inner_function(request, context, "GetObject")

    def PutObject(self, request: ray_client_pb2.PutRequest,
                  context=None) -> ray_client_pb2.PutResponse:
        return self._call_inner_function(request, context, "PutObject")

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        return self._call_inner_function(request, context, "WaitObject")

    def Schedule(self, task, context=None) -> ray_client_pb2.ClientTaskTicket:
        return self._call_inner_function(task, context, "Schedule")


class MiddlemanServer:
    def __init__(self,
                 listen_addr="localhost:10011",
                 real_addr="localhost:50051",
                 on_log_response=None,
                 on_data_response=None,
                 on_task_request=None,
                 on_task_response=None):
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
            options=GRPC_OPTIONS)
        self.task_servicer = MiddlemanRayletServicer(
            on_response=on_task_response, on_request=on_task_request)
        self.data_servicer = MiddlemanDataServicer(
            on_response=on_data_response)
        self.logs_servicer = MiddlemanLogServicer(on_response=on_log_response)
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

    def stop(self, grace: int) -> None:
        self.server.stop(grace)


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@contextlib.contextmanager
def start_middleman_server(on_log_response=None,
                           on_data_response=None,
                           on_task_request=None,
                           on_task_response=None):
    ray._inside_client_test = True
    server = ray_client_server.serve("localhost:50051")
    middleman = None
    try:
        middleman = MiddlemanServer(
            on_log_response=None,
            on_data_response=None,
            on_task_request=None,
            on_task_response=None)
        middleman.start()
        ray.init("ray://localhost:10011", logging_level=logging.INFO)
        yield middleman, server
    finally:
        ray._inside_client_test = False
        ray.util.disconnect()
        server.stop(0)
        if middleman:
            middleman.stop(0)


def test_disconnect_during_get():
    @ray.remote
    def slow_result():
        time.sleep(20)
        return 12345

    def disconnect(middleman):
        time.sleep(3)
        middleman.reset_channel()

    with start_middleman_server() as (middleman, _):
        disconnect_thread = threading.Thread(
            target=disconnect, args=(middleman, ))
        disconnect_thread.start()
        result = ray.get(slow_result.remote())
        assert result == 12345
        disconnect_thread.join()


def test_valid_actor_state():
    @ray.remote
    class IncrActor:
        def __init__(self):
            self.val = 0

        def incr(self):
            self.val += 1
            return self.val

    i = 0

    def fail_every_seven(_):
        nonlocal i
        i += 1
        if i % 7 == 0:
            raise RuntimeError

    with start_middleman_server(
            on_data_response=fail_every_seven,
            on_task_request=fail_every_seven,
            on_task_response=fail_every_seven):
        actor = IncrActor.remote()
        for _ in range(100):
            ref = actor.incr.remote()
        assert ray.get(ref) == 100


def test_valid_actor_state_2():
    # Do a full on disconnect (cancel channel) every 11 requests. Failure
    # happens:
    #   - before request sent: request never reaches server
    #   - before response received: response never reaches server
    #   - during gets
    @ray.remote
    class IncrActor:
        def __init__(self):
            self.val = 0

        def incr(self):
            self.val += 1
            return self.val

    i = 0

    with start_middleman_server() as (middleman, _):

        def fail_every_eleven(_):
            nonlocal i
            i += 1
            if i % 11 == 0:
                middleman.reset_channel()

        middleman.data_servicer.on_response = fail_every_eleven
        middleman.task_servicer.on_request = fail_every_eleven
        middleman.task_servicer.on_response = fail_every_eleven

        actor = IncrActor.remote()
        for _ in range(100):
            ref = actor.incr.remote()
        assert ray.get(ref) == 100


def test_noisy_puts():
    """
    Randomly kills the data channel with 10% when receiving response
    (requests made it to server, responses dropped) and checks that final
    result is still consistent
    """
    random.seed(12345)
    with start_middleman_server() as (middleman, _):

        def fail_randomly(response: ray_client_pb2.DataResponse):
            if random.random() < 0.1:
                raise RuntimeError

        middleman.data_servicer.on_response = fail_randomly

        refs = [ray.put(i * 123) for i in range(500)]
        results = ray.get(refs)
        for i, result in enumerate(results):
            assert result == i * 123


def test_noisy_kv_puts():
    """
    Same as above, but interfere's with KV puts, and checks that the
    result
    """
