"""This file implements a threaded stream controller to abstract a data stream
back to the ray clientserver.
"""
import logging
import queue
import threading
import grpc

from typing import Any, Callable, Dict, Optional, Union

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

logger = logging.getLogger(__name__)

# The maximum field value for request_id -- which is also the maximum
# number of simultaneous in-flight requests.
INT32_MAX = (2**31) - 1

ResponseCallable = Callable[[Union[ray_client_pb2.DataResponse, Exception]],
                            None]


class DataClient:
    def __init__(self, channel: "grpc._channel.Channel", client_id: str,
                 metadata: list):
        """Initializes a thread-safe datapath over a Ray Client gRPC channel.

        Args:
            channel: connected gRPC channel
            client_id: the generated ID representing this client
            metadata: metadata to pass to gRPC requests
        """
        self.channel = channel
        self._client_id = client_id
        self._metadata = metadata
        self.data_thread = self._start_datathread()

        # Serialize access to all mutable internal states: self.request_queue,
        # self.ready_data, self.asyncio_waiting_data,
        # self._in_shutdown, self._req_id and calling self._next_id().
        self.lock = threading.Lock()

        # Waiting for response or shutdown.
        self.cv = threading.Condition(lock=self.lock)

        self.request_queue = queue.Queue()
        self.ready_data: Dict[int, Any] = {}
        # NOTE: Dictionary insertion is guaranteed to complete before lookup
        # and/or removal because of synchronization via the request_queue.
        self.asyncio_waiting_data: Dict[int, ResponseCallable] = {}
        self._in_shutdown = False
        self._req_id = 0

        self.data_thread.start()

    # Must hold self.lock when calling this function.
    def _next_id(self) -> int:
        assert self.lock.locked()
        self._req_id += 1
        if self._req_id > INT32_MAX:
            self._req_id = 1
        # Responses that aren't tracked (like opportunistic releases)
        # have req_id=0, so make sure we never mint such an id.
        assert self._req_id != 0
        return self._req_id

    def _start_datathread(self) -> threading.Thread:
        return threading.Thread(
            target=self._data_main,
            name="ray_client_streaming_rpc",
            args=(),
            daemon=True)

    def _data_main(self) -> None:
        stub = ray_client_pb2_grpc.RayletDataStreamerStub(self.channel)
        resp_stream = stub.Datapath(
            iter(self.request_queue.get, None),
            metadata=self._metadata,
            wait_for_ready=True)
        try:
            for response in resp_stream:
                if response.req_id == 0:
                    # This is not being waited for.
                    logger.debug(f"Got unawaited response {response}")
                    continue
                if response.req_id in self.asyncio_waiting_data:
                    try:
                        # NOTE: calling self.asyncio_waiting_data.pop() results
                        # in the destructor of ClientObjectRef running, which
                        # calls ReleaseObject(). So self.asyncio_waiting_data
                        # is accessed without holding self.lock. Holding the
                        # lock shouldn't be necessary either.
                        callback = self.asyncio_waiting_data.pop(
                            response.req_id)
                        callback(response)
                    except Exception:
                        logger.exception("Callback error:")
                else:
                    with self.lock:
                        self.ready_data[response.req_id] = response
                        self.cv.notify_all()
        except grpc.RpcError as e:
            with self.lock:
                self._in_shutdown = True
                self._last_exception = e
                self.cv.notify_all()

                callbacks = self.asyncio_waiting_data.values()
                self.asyncio_waiting_data = {}

            # Abort async requests with the error.
            err = ConnectionError("Failed during this or a previous request. "
                                  f"Exception that broke the connection: {e}")
            for callback in callbacks:
                callback(err)
            # Since self._in_shutdown is set to True, no new item
            # will be added to self.asyncio_waiting_data

            if e.code() == grpc.StatusCode.CANCELLED:
                # Gracefully shutting down
                logger.info("Cancelling data channel")
            elif e.code() in (grpc.StatusCode.UNAVAILABLE,
                              grpc.StatusCode.RESOURCE_EXHAUSTED):
                # TODO(barakmich): The server may have
                # dropped. In theory, we can retry, as per
                # https://grpc.github.io/grpc/core/md_doc_statuscodes.html but
                # in practice we may need to think about the correct semantics
                # here.
                logger.info("Server disconnected from data channel")
            else:
                logger.exception(
                    "Got Error from data channel -- shutting down:")

    def close(self) -> None:
        thread = None
        with self.lock:
            self._in_shutdown = True
            # Notify blocking operations to fail.
            self.cv.notify_all()
            # Add sentinel to terminate streaming RPC.
            if self.request_queue is not None:
                self.request_queue.put(None)
            if self.data_thread is not None:
                thread = self.data_thread
        # Wait until streaming RPCs are done.
        if thread is not None:
            thread.join()

    def _blocking_send(self, req: ray_client_pb2.DataRequest
                       ) -> ray_client_pb2.DataResponse:
        with self.lock:
            self._check_shutdown()
            req_id = self._next_id()
            req.req_id = req_id
            self.request_queue.put(req)

            self.cv.wait_for(
                lambda: req_id in self.ready_data or self._in_shutdown)
            self._check_shutdown()

            data = self.ready_data[req_id]
            del self.ready_data[req_id]

        return data

    def _async_send(self,
                    req: ray_client_pb2.DataRequest,
                    callback: Optional[ResponseCallable] = None) -> None:
        with self.lock:
            self._check_shutdown()
            req_id = self._next_id()
            req.req_id = req_id
            if callback:
                self.asyncio_waiting_data[req_id] = callback
            self.request_queue.put(req)

    # Must hold self.lock when calling this function.
    def _check_shutdown(self):
        assert self.lock.locked()
        if not self._in_shutdown:
            return

        self.lock.release()

        # Do not try disconnect() or throw exceptions in self.data_thread.
        # Otherwise deadlock can occur.
        if threading.current_thread().ident == self.data_thread.ident:
            return

        from ray.util import disconnect
        disconnect()

        self.lock.acquire()

        last_exception = getattr(self, "_last_exception", None)
        if last_exception is not None:
            msg = ("Request can't be sent because the Ray client has already "
                   "been disconnected due to an error. Last exception: "
                   f"{last_exception}")
        else:
            msg = ("Request can't be sent because the Ray client has already "
                   f"been disconnected.")

        raise ConnectionError(msg)

    def Init(self, request: ray_client_pb2.InitRequest,
             context=None) -> ray_client_pb2.InitResponse:
        datareq = ray_client_pb2.DataRequest(init=request, )
        resp = self._blocking_send(datareq)
        return resp.init

    def PrepRuntimeEnv(self,
                       request: ray_client_pb2.PrepRuntimeEnvRequest,
                       context=None) -> ray_client_pb2.PrepRuntimeEnvResponse:
        datareq = ray_client_pb2.DataRequest(prep_runtime_env=request, )
        resp = self._blocking_send(datareq)
        return resp.prep_runtime_env

    def ConnectionInfo(self,
                       context=None) -> ray_client_pb2.ConnectionInfoResponse:
        datareq = ray_client_pb2.DataRequest(
            connection_info=ray_client_pb2.ConnectionInfoRequest())
        resp = self._blocking_send(datareq)
        return resp.connection_info

    def GetObject(self, request: ray_client_pb2.GetRequest,
                  context=None) -> ray_client_pb2.GetResponse:
        datareq = ray_client_pb2.DataRequest(get=request, )
        resp = self._blocking_send(datareq)
        return resp.get

    def RegisterGetCallback(self, request: ray_client_pb2.GetRequest,
                            callback: ResponseCallable) -> None:
        if len(request.ids) != 1:
            raise ValueError(
                "RegisterGetCallback() must have exactly 1 Object ID. "
                f"Actual: {request}")
        datareq = ray_client_pb2.DataRequest(get=request, )
        self._async_send(datareq, callback)

    # TODO: convert PutObject to async
    def PutObject(self, request: ray_client_pb2.PutRequest,
                  context=None) -> ray_client_pb2.PutResponse:
        datareq = ray_client_pb2.DataRequest(put=request, )
        resp = self._blocking_send(datareq)
        return resp.put

    def ReleaseObject(self,
                      request: ray_client_pb2.ReleaseRequest,
                      context=None) -> None:
        datareq = ray_client_pb2.DataRequest(release=request, )
        self._async_send(datareq)
