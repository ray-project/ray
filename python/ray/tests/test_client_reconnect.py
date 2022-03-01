from concurrent import futures
import asyncio
import contextlib
import os
import threading
import sys
import grpc
import numpy as np

import time
import random
import pytest
from typing import Any, Callable, Optional
from unittest.mock import patch

import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import CLIENT_SERVER_MAX_THREADS, GRPC_OPTIONS
import ray.util.client.server.server as ray_client_server
from ray._private.client_mode_hook import disable_client_hook

# At a high level, these tests rely on an extra RPC server sitting
# between the client and the real Ray server to inject errors, drop responses
# and drop requests, i.e. at a high level:
#   Ray Client <-> Middleman Server <-> Proxy Server

# Type for middleman hooks used to inject errors
Hook = Callable[[Any], None]


class MiddlemanDataServicer(ray_client_pb2_grpc.RayletDataStreamerServicer):
    """
    Forwards all requests to the real data servicer. Useful for injecting
    errors between a client and server pair.
    """

    def __init__(
        self, on_response: Optional[Hook] = None, on_request: Optional[Hook] = None
    ):
        """
        Args:
            on_response: Optional hook to inject errors before sending back a
                response
        """
        self.stub = None
        self.on_response = on_response
        self.on_request = on_request

    def set_channel(self, channel: grpc.Channel) -> None:
        self.stub = ray_client_pb2_grpc.RayletDataStreamerStub(channel)

    def _requests(self, request_iterator):
        for req in request_iterator:
            if self.on_request:
                self.on_request(req)
            yield req

    def Datapath(self, request_iterator, context):
        try:
            for response in self.stub.Datapath(
                self._requests(request_iterator), metadata=context.invocation_metadata()
            ):
                if self.on_response:
                    self.on_response(response)
                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(e.details())


class MiddlemanLogServicer(ray_client_pb2_grpc.RayletLogStreamerServicer):
    """
    Forwards all requests to the real log servicer. Useful for injecting
    errors between a client and server pair.
    """

    def __init__(self, on_response: Optional[Hook] = None):
        """
        Args:
            on_response: Optional hook to inject errors before sending back a
                response
        """
        self.stub = None
        self.on_response = on_response

    def set_channel(self, channel: grpc.Channel) -> None:
        self.stub = ray_client_pb2_grpc.RayletLogStreamerStub(channel)

    def Logstream(self, request_iterator, context):
        try:
            for response in self.stub.Logstream(
                request_iterator, metadata=context.invocation_metadata()
            ):
                if self.on_response:
                    self.on_response(response)
                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(e.details())


class MiddlemanRayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    """
    Forwards all requests to the raylet driver servicer. Useful for injecting
    errors between a client and server pair.
    """

    def __init__(
        self, on_request: Optional[Hook] = None, on_response: Optional[Hook] = None
    ):
        """
        Args:
            on_request: Optional hook to inject errors before forwarding a
                request
            on_response: Optional hook to inject errors before sending back a
                response
        """
        self.stub = None
        self.on_request = on_request
        self.on_response = on_response

    def set_channel(self, channel: grpc.Channel) -> None:
        self.stub = ray_client_pb2_grpc.RayletDriverStub(channel)

    def _call_inner_function(
        self, request: Any, context, method: str
    ) -> Optional[ray_client_pb2_grpc.RayletDriverStub]:
        if self.on_request:
            self.on_request(request)
        try:
            response = getattr(self.stub, method)(
                request, metadata=context.invocation_metadata()
            )
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(e.details())
            raise
        if self.on_response and method != "GetObject":
            # GetObject streams response, handle on_response separately
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

    def KVExists(self, request, context=None) -> ray_client_pb2.KVExistsResponse:
        return self._call_inner_function(request, context, "KVExists")

    def ListNamedActors(
        self, request, context=None
    ) -> ray_client_pb2.ClientListNamedActorsResponse:
        return self._call_inner_function(request, context, "ListNamedActors")

    def ClusterInfo(self, request, context=None) -> ray_client_pb2.ClusterInfoResponse:
        return self._call_inner_function(request, context, "ClusterInfo")

    def Terminate(self, req, context=None):
        return self._call_inner_function(req, context, "Terminate")

    def GetObject(self, request, context=None):
        for response in self._call_inner_function(request, context, "GetObject"):
            if self.on_response:
                self.on_response(response)
            yield response

    def PutObject(
        self, request: ray_client_pb2.PutRequest, context=None
    ) -> ray_client_pb2.PutResponse:
        return self._call_inner_function(request, context, "PutObject")

    def WaitObject(
        self, request: ray_client_pb2.WaitRequest, context=None
    ) -> ray_client_pb2.WaitResponse:
        return self._call_inner_function(request, context, "WaitObject")

    def Schedule(
        self, task: ray_client_pb2.ClientTask, context=None
    ) -> ray_client_pb2.ClientTaskTicket:
        return self._call_inner_function(task, context, "Schedule")


class MiddlemanServer:
    """
    Helper class that wraps the RPC server that middlemans the connection
    between the client and the real ray server. Useful for injecting
    errors between a client and server pair.
    """

    def __init__(
        self,
        listen_addr: str,
        real_addr,
        on_log_response: Optional[Hook] = None,
        on_data_request: Optional[Hook] = None,
        on_data_response: Optional[Hook] = None,
        on_task_request: Optional[Hook] = None,
        on_task_response: Optional[Hook] = None,
    ):
        """
        Args:
            listen_addr: The address the middleman server will listen on
            real_addr: The address of the real ray server
            on_log_response: Optional hook to inject errors before sending back
                a log response
            on_data_response: Optional hook to inject errors before sending
                back a data response
            on_task_request: Optional hook to inject errors before forwarding
                a raylet driver request
            on_task_response: Optional hook to inject errors before sending
                back a raylet driver response
        """
        self.listen_addr = listen_addr
        self.real_addr = real_addr
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
            options=GRPC_OPTIONS,
        )
        self.task_servicer = MiddlemanRayletServicer(
            on_response=on_task_response, on_request=on_task_request
        )
        self.data_servicer = MiddlemanDataServicer(
            on_response=on_data_response, on_request=on_data_request
        )
        self.logs_servicer = MiddlemanLogServicer(on_response=on_log_response)
        ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
            self.task_servicer, self.server
        )
        ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(
            self.data_servicer, self.server
        )
        ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(
            self.logs_servicer, self.server
        )
        self.server.add_insecure_port(self.listen_addr)
        self.channel = None
        self.reset_channel()

    def reset_channel(self) -> None:
        """
        Manually close and reopen the channel to the real ray server. This
        simulates a disconnection between the client and the server.
        """
        if self.channel:
            self.channel.close()
        self.channel = grpc.insecure_channel(self.real_addr, options=GRPC_OPTIONS)
        grpc.channel_ready_future(self.channel)
        self.task_servicer.set_channel(self.channel)
        self.data_servicer.set_channel(self.channel)
        self.logs_servicer.set_channel(self.channel)

    def start(self) -> None:
        self.server.start()

    def stop(self, grace: int) -> None:
        self.server.stop(grace)


@contextlib.contextmanager
def start_middleman_server(
    on_log_response=None,
    on_data_request=None,
    on_data_response=None,
    on_task_request=None,
    on_task_response=None,
):
    """
    Helper context that starts a middleman server listening on port 10011,
    and a ray client server on port 50051.
    """
    ray._inside_client_test = True
    server = ray_client_server.serve("localhost:50051")
    middleman = None
    try:
        middleman = MiddlemanServer(
            listen_addr="localhost:10011",
            real_addr="localhost:50051",
            on_log_response=on_log_response,
            on_data_request=on_data_request,
            on_data_response=on_data_response,
            on_task_request=on_task_request,
            on_task_response=on_task_response,
        )
        middleman.start()
        ray.init("ray://localhost:10011")
        yield middleman, server
    finally:
        ray._inside_client_test = False
        ray.util.disconnect()
        if middleman:
            middleman.stop(0)
        # Delete server to allow the client server to be GC'ed, which shuts
        # down Ray. Then wait for Ray to shut down in the local process.
        # Otherwise, the Ray cluster may stay alive until the next call to
        # start_middleman_server(), become the backing Ray cluster to the
        # client server, and shut down in the middle of the test case after
        # GC finally catches up, leading to test failures.
        server.stop(0)
        del server
        start = time.monotonic()
        with disable_client_hook():
            while ray.is_initialized():
                time.sleep(1)
                if time.monotonic() - start > 30:
                    raise RuntimeError("Failed to terminate Ray")


def test_disconnect_during_get():
    """
    Disconnect the proxy and the client in the middle of a long running get
    """

    @ray.remote
    def slow_result():
        time.sleep(20)
        return 12345

    def disconnect(middleman):
        time.sleep(3)
        middleman.reset_channel()

    with start_middleman_server() as (middleman, _):
        disconnect_thread = threading.Thread(target=disconnect, args=(middleman,))
        disconnect_thread.start()
        result = ray.get(slow_result.remote())
        assert result == 12345
        disconnect_thread.join()


def test_disconnects_during_large_get():
    """
    Disconnect repeatedly during a large (multi-chunk) get.
    """
    i = 0
    started = False

    def fail_every_three(_):
        # Inject an error every third time this method is called
        nonlocal i, started
        if not started:
            return
        i += 1
        if i % 3 == 0:
            raise RuntimeError

    @ray.remote
    def large_result():
        # 1024x1024x128 float64 matrix (1024 MiB). With 64MiB chunk size,
        # it will take at least 16 chunks to transfer this object. Since
        # the failure is injected every 3 chunks, this transfer can only
        # work if the chunked get request retries at the last received chunk
        # (instead of starting from the beginning each retry)
        return np.random.random((1024, 1024, 128))

    with start_middleman_server(on_task_response=fail_every_three):
        started = True
        result = ray.get(large_result.remote())
        assert result.shape == (1024, 1024, 128)


def test_disconnects_during_large_async_get():
    """
    Disconnect repeatedly during a large (multi-chunk) async get.
    """
    i = 0
    started = False

    def fail_every_three(_):
        # Inject an error every third time this method is called
        nonlocal i, started
        if not started:
            return
        i += 1
        if i % 3 == 0:
            raise RuntimeError

    @ray.remote
    def large_result():
        # 1024x1024x128 float64 matrix (1024 MiB). With 64MiB chunk size,
        # it will take at least 16 chunks to transfer this object. Since
        # the failure is injected every 3 chunks, this transfer can only
        # work if the chunked get request retries at the last received chunk
        # (instead of starting from the beginning each retry)
        return np.random.random((1024, 1024, 128))

    with start_middleman_server(on_data_response=fail_every_three):
        started = True

        async def get_large_result():
            return await large_result.remote()

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(get_large_result())
        assert result.shape == (1024, 1024, 128)


def test_disconnect_during_large_put():
    """
    Disconnect during a large (multi-chunk) put.
    """
    i = 0
    started = False

    def fail_halfway(_):
        # Inject an error halfway through the object transfer
        nonlocal i, started
        if not started:
            return
        i += 1
        if i == 8:
            raise RuntimeError

    with start_middleman_server(on_data_request=fail_halfway):
        started = True
        objref = ray.put(np.random.random((1024, 1024, 128)))
        assert i > 8  # Check that the failure was injected
        result = ray.get(objref)
        assert result.shape == (1024, 1024, 128)


def test_valid_actor_state():
    """
    Repeatedly inject errors in the middle of mutating actor calls. Check
    at the end that the final state of the actor is consistent with what
    we would expect had the disconnects not occurred.
    """

    @ray.remote
    class IncrActor:
        def __init__(self):
            self.val = 0

        def incr(self):
            self.val += 1
            return self.val

    i = 0
    # This is to prevent erroring in the initial connection logic.
    started = False

    def fail_every_seven(_):
        # Inject an error every seventh time this method is called
        nonlocal i, started
        i += 1
        if i % 7 == 0 and started:
            raise RuntimeError

    with start_middleman_server(
        on_data_response=fail_every_seven,
        on_task_request=fail_every_seven,
        on_task_response=fail_every_seven,
    ):
        started = True
        actor = IncrActor.remote()
        for _ in range(100):
            ref = actor.incr.remote()
        assert ray.get(ref) == 100


def test_valid_actor_state_2():
    """
    Do a full disconnect (cancel channel) every 11 requests. Failure
    happens:
      - before request sent: request never reaches server
      - before response received: response never reaches server
      - while get's are being processed
    """

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
    Randomly kills the data channel with 10% chance when receiving response
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


def test_client_reconnect_grace_period():
    """
    Tests that the client gives up attempting to reconnect the channel
    after the grace period expires.
    """
    # Lower grace period to 5 seconds to save time
    with patch.dict(
        os.environ, {"RAY_CLIENT_RECONNECT_GRACE_PERIOD": "5"}
    ), start_middleman_server() as (middleman, _):
        assert ray.get(ray.put(42)) == 42
        # Close channel
        middleman.channel.close()
        start_time = time.time()
        with pytest.raises(ConnectionError):
            ray.get(ray.put(42))
        # Connection error should have been raised within a reasonable
        # amount of time. Set to significantly higher than 5 seconds
        # to account for reconnect backoff timing
        assert time.time() - start_time < 20


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
