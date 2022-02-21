"""This file implements a threaded stream controller to abstract a data stream
back to the ray clientserver.
"""
import logging
import queue
import threading
import grpc

from collections import OrderedDict
from typing import Any, Callable, Dict, TYPE_CHECKING, Optional, Union

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import INT32_MAX

if TYPE_CHECKING:
    from ray.util.client.worker import Worker

logger = logging.getLogger(__name__)

ResponseCallable = Callable[[Union[ray_client_pb2.DataResponse, Exception]], None]

# Send an acknowledge on every 32nd response received
ACKNOWLEDGE_BATCH_SIZE = 32


class DataClient:
    def __init__(self, client_worker: "Worker", client_id: str, metadata: list):
        """Initializes a thread-safe datapath over a Ray Client gRPC channel.

        Args:
            client_worker: The Ray Client worker that manages this client
            client_id: the generated ID representing this client
            metadata: metadata to pass to gRPC requests
        """
        self.client_worker = client_worker
        self._client_id = client_id
        self._metadata = metadata
        self.data_thread = self._start_datathread()

        # Track outstanding requests to resend in case of disconnection
        self.outstanding_requests: Dict[int, Any] = OrderedDict()

        # Serialize access to all mutable internal states: self.request_queue,
        # self.ready_data, self.asyncio_waiting_data,
        # self._in_shutdown, self._req_id, self.outstanding_requests and
        # calling self._next_id()
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
        self._last_exception = None
        self._acknowledge_counter = 0

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
            daemon=True,
        )

    def _data_main(self) -> None:
        reconnecting = False
        try:
            while not self.client_worker._in_shutdown:
                stub = ray_client_pb2_grpc.RayletDataStreamerStub(
                    self.client_worker.channel
                )
                metadata = self._metadata + [("reconnecting", str(reconnecting))]
                resp_stream = stub.Datapath(
                    iter(self.request_queue.get, None),
                    metadata=metadata,
                    wait_for_ready=True,
                )
                try:
                    for response in resp_stream:
                        self._process_response(response)
                    return
                except grpc.RpcError as e:
                    reconnecting = self._can_reconnect(e)
                    if not reconnecting:
                        self._last_exception = e
                        return
                    self._reconnect_channel()
        except Exception as e:
            self._last_exception = e
        finally:
            logger.debug("Shutting down data channel.")
            self._shutdown()

    def _process_response(self, response: Any) -> None:
        """
        Process responses from the data servicer.
        """
        if response.req_id == 0:
            # This is not being waited for.
            logger.debug(f"Got unawaited response {response}")
            return
        if response.req_id in self.asyncio_waiting_data:
            try:
                # NOTE: calling self.asyncio_waiting_data.pop() results
                # in the destructor of ClientObjectRef running, which
                # calls ReleaseObject(). So self.asyncio_waiting_data
                # is accessed without holding self.lock. Holding the
                # lock shouldn't be necessary either.
                callback = self.asyncio_waiting_data.pop(response.req_id)
                if callback:
                    callback(response)
            except Exception:
                logger.exception("Callback error:")
            with self.lock:
                # Update outstanding requests
                if response.req_id in self.outstanding_requests:
                    del self.outstanding_requests[response.req_id]
                    # Acknowledge response
                    self._acknowledge(response.req_id)
        else:
            with self.lock:
                self.ready_data[response.req_id] = response
                self.cv.notify_all()

    def _can_reconnect(self, e: grpc.RpcError) -> bool:
        """
        Processes RPC errors that occur while reading from data stream.
        Returns True if the error can be recovered from, False otherwise.
        """
        if not self.client_worker._can_reconnect(e):
            logger.error("Unrecoverable error in data channel.")
            logger.debug(e)
            return False
        logger.debug("Recoverable error in data channel.")
        logger.debug(e)
        return True

    def _shutdown(self) -> None:
        """
        Shutdown the data channel
        """
        with self.lock:
            self._in_shutdown = True
            self.cv.notify_all()

            callbacks = self.asyncio_waiting_data.values()
            self.asyncio_waiting_data = {}

        if self._last_exception:
            # Abort async requests with the error.
            err = ConnectionError(
                "Failed during this or a previous request. Exception that "
                f"broke the connection: {self._last_exception}"
            )
        else:
            err = ConnectionError(
                "Request cannot be fulfilled because the data client has "
                "disconnected."
            )
        for callback in callbacks:
            if callback:
                callback(err)
        # Since self._in_shutdown is set to True, no new item
        # will be added to self.asyncio_waiting_data

    def _acknowledge(self, req_id: int) -> None:
        """
        Puts an acknowledge request on the request queue periodically.
        Lock should be held before calling this. Used when an async or
        blocking response is received.
        """
        if not self.client_worker._reconnect_enabled:
            # Skip ACKs if reconnect isn't enabled
            return
        assert self.lock.locked()
        self._acknowledge_counter += 1
        if self._acknowledge_counter % ACKNOWLEDGE_BATCH_SIZE == 0:
            self.request_queue.put(
                ray_client_pb2.DataRequest(
                    acknowledge=ray_client_pb2.AcknowledgeRequest(req_id=req_id)
                )
            )

    def _reconnect_channel(self) -> None:
        """
        Attempts to reconnect the gRPC channel and resend outstanding
        requests. First, the server is pinged to see if the current channel
        still works. If the ping fails, then the current channel is closed
        and replaced with a new one.

        Once a working channel is available, a new request queue is made
        and filled with any outstanding requests to be resent to the server.
        """
        try:
            # Ping the server to see if the current channel is reuseable, for
            # example if gRPC reconnected the channel on its own or if the
            # RPC error was transient and the channel is still open
            ping_succeeded = self.client_worker.ping_server(timeout=5)
        except grpc.RpcError:
            ping_succeeded = False

        if not ping_succeeded:
            # Ping failed, try refreshing the data channel
            logger.warning(
                "Encountered connection issues in the data channel. "
                "Attempting to reconnect."
            )
            try:
                self.client_worker._connect_channel(reconnecting=True)
            except ConnectionError:
                logger.warning("Failed to reconnect the data channel")
                raise
            logger.debug("Reconnection succeeded!")

        # Recreate the request queue, and resend outstanding requests
        with self.lock:
            self.request_queue = queue.Queue()
            for request in self.outstanding_requests.values():
                # Resend outstanding requests
                self.request_queue.put(request)

    def close(self) -> None:
        thread = None
        with self.lock:
            self._in_shutdown = True
            # Notify blocking operations to fail.
            self.cv.notify_all()
            # Add sentinel to terminate streaming RPC.
            if self.request_queue is not None:
                # Intentional shutdown, tell server it can clean up the
                # connection immediately and ignore the reconnect grace period.
                cleanup_request = ray_client_pb2.DataRequest(
                    connection_cleanup=ray_client_pb2.ConnectionCleanupRequest()
                )
                self.request_queue.put(cleanup_request)
                self.request_queue.put(None)
            if self.data_thread is not None:
                thread = self.data_thread
        # Wait until streaming RPCs are done.
        if thread is not None:
            thread.join()

    def _blocking_send(
        self, req: ray_client_pb2.DataRequest
    ) -> ray_client_pb2.DataResponse:
        with self.lock:
            self._check_shutdown()
            req_id = self._next_id()
            req.req_id = req_id
            self.request_queue.put(req)
            self.outstanding_requests[req_id] = req

            self.cv.wait_for(lambda: req_id in self.ready_data or self._in_shutdown)
            self._check_shutdown()

            data = self.ready_data[req_id]
            del self.ready_data[req_id]
            del self.outstanding_requests[req_id]
            self._acknowledge(req_id)

        return data

    def _async_send(
        self,
        req: ray_client_pb2.DataRequest,
        callback: Optional[ResponseCallable] = None,
    ) -> None:
        with self.lock:
            self._check_shutdown()
            req_id = self._next_id()
            req.req_id = req_id
            self.asyncio_waiting_data[req_id] = callback
            self.outstanding_requests[req_id] = req
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

        if self._last_exception is not None:
            msg = (
                "Request can't be sent because the Ray client has already "
                "been disconnected due to an error. Last exception: "
                f"{self._last_exception}"
            )
        else:
            msg = (
                "Request can't be sent because the Ray client has already "
                "been disconnected."
            )

        raise ConnectionError(msg)

    def Init(
        self, request: ray_client_pb2.InitRequest, context=None
    ) -> ray_client_pb2.InitResponse:
        datareq = ray_client_pb2.DataRequest(
            init=request,
        )
        resp = self._blocking_send(datareq)
        return resp.init

    def PrepRuntimeEnv(
        self, request: ray_client_pb2.PrepRuntimeEnvRequest, context=None
    ) -> ray_client_pb2.PrepRuntimeEnvResponse:
        datareq = ray_client_pb2.DataRequest(
            prep_runtime_env=request,
        )
        resp = self._blocking_send(datareq)
        return resp.prep_runtime_env

    def ConnectionInfo(self, context=None) -> ray_client_pb2.ConnectionInfoResponse:
        datareq = ray_client_pb2.DataRequest(
            connection_info=ray_client_pb2.ConnectionInfoRequest()
        )
        resp = self._blocking_send(datareq)
        return resp.connection_info

    def GetObject(
        self, request: ray_client_pb2.GetRequest, context=None
    ) -> ray_client_pb2.GetResponse:
        datareq = ray_client_pb2.DataRequest(
            get=request,
        )
        resp = self._blocking_send(datareq)
        return resp.get

    def RegisterGetCallback(
        self, request: ray_client_pb2.GetRequest, callback: ResponseCallable
    ) -> None:
        if len(request.ids) != 1:
            raise ValueError(
                "RegisterGetCallback() must have exactly 1 Object ID. "
                f"Actual: {request}"
            )
        datareq = ray_client_pb2.DataRequest(
            get=request,
        )
        self._async_send(datareq, callback)

    # TODO: convert PutObject to async
    def PutObject(
        self, request: ray_client_pb2.PutRequest, context=None
    ) -> ray_client_pb2.PutResponse:
        datareq = ray_client_pb2.DataRequest(
            put=request,
        )
        resp = self._blocking_send(datareq)
        return resp.put

    def ReleaseObject(
        self, request: ray_client_pb2.ReleaseRequest, context=None
    ) -> None:
        datareq = ray_client_pb2.DataRequest(
            release=request,
        )
        self._async_send(datareq)

    def Schedule(self, request: ray_client_pb2.ClientTask, callback: ResponseCallable):
        datareq = ray_client_pb2.DataRequest(task=request)
        self._async_send(datareq, callback)

    def Terminate(
        self, request: ray_client_pb2.TerminateRequest
    ) -> ray_client_pb2.TerminateResponse:
        req = ray_client_pb2.DataRequest(
            terminate=request,
        )
        resp = self._blocking_send(req)
        return resp.terminate

    def ListNamedActors(
        self, request: ray_client_pb2.ClientListNamedActorsRequest
    ) -> ray_client_pb2.ClientListNamedActorsResponse:
        req = ray_client_pb2.DataRequest(
            list_named_actors=request,
        )
        resp = self._blocking_send(req)
        return resp.list_named_actors
