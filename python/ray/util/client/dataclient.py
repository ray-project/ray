"""This file implements a threaded stream controller to abstract a data stream
back to the ray clientserver.
"""
import logging
import queue
import threading
import grpc

from typing import Any, Callable, Dict, Optional

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.common import INT32_MAX

logger = logging.getLogger(__name__)

ResponseCallable = Callable[[ray_client_pb2.DataResponse], None]


class DataClient:
    def __init__(self, client_worker, client_id: str, metadata: list):
        """Initializes a thread-safe datapath over a Ray Client gRPC channel.

        Args:
            client_worker: The Ray Client worker that manages this client
            client_id: the generated ID representing this client
            metadata: metadata to pass to gRPC requests
        """
        self.client_worker = client_worker
        self.request_queue = queue.Queue()
        self.data_thread = self._start_datathread()
        self.ready_data: Dict[int, Any] = {}
        self.cv = threading.Condition()
        self.lock = threading.RLock()

        # Track outstanding requests to resend in case of disconnection
        self.outstanding_requests_lock = threading.RLock()
        self.outstanding_requests: Dict[int, Any] = {}

        # NOTE: Dictionary insertion is guaranteed to complete before lookup
        # and/or removal because of synchronization via the request_queue.
        self.asyncio_waiting_data: Dict[int, ResponseCallable] = {}
        self._req_id = 0
        self._client_id = client_id
        self._metadata = metadata
        self._in_shutdown = False
        self.data_thread.start()

    def _next_id(self) -> int:
        with self.lock:
            self._req_id += 1
            if self._req_id > INT32_MAX:
                self._req_id = 1
            # Responses that aren't tracked (like opportunistic releases)
            # have req_id=0, so make sure we never mint such an id.
            assert self._req_id != 0
            return self._req_id

    def _start_datathread(self) -> threading.Thread:
        return threading.Thread(target=self._data_main, args=(), daemon=True)

    def _data_main(self) -> None:
        reconnecting = False
        try:
            while not self.client_worker._in_shutdown:
                stub = ray_client_pb2_grpc.RayletDataStreamerStub(
                    self.client_worker.channel)
                metadata = self._metadata + \
                    [("reconnecting", str(reconnecting))]
                resp_stream = stub.Datapath(
                    iter(self.request_queue.get, None),
                    metadata=metadata,
                    wait_for_ready=True)
                try:
                    for response in resp_stream:
                        self._process_response(response)
                    return
                except grpc.RpcError as e:
                    reconnecting = self._process_rpc_error(e)
                    if not reconnecting:
                        break
        finally:
            logger.info("Shutting down data channel")
            self._shutdown()

    def _process_response(self, response: Any) -> None:
        """
        Process responses from the data servicer.
        """
        if response.req_id == 0:
            # This is not being waited for.
            logger.debug(f"Got unawaited response {response}")
            return
        # Acknowledge to server that response has been received -- server
        # can clear cache entry
        self._acknowledge(response.req_id)
        if response.req_id in self.asyncio_waiting_data:
            # Async response. Check for callback and update outstanding
            # requests.
            callback = self.asyncio_waiting_data.pop(response.req_id)
            try:
                if callback:
                    callback(response)
            except Exception:
                logger.exception("Callback error:")
            if response.req_id in self.outstanding_requests:
                del self.outstanding_requests[response.req_id]
        else:
            # Blocking response. Populate ready data and wake up waiting
            # threads.
            with self.cv:
                self.ready_data[response.req_id] = response
                self.cv.notify_all()

    def _acknowledge(self, req_id: int):
        self.request_queue.put(
            ray_client_pb2.DataRequest(
                acknowledge=ray_client_pb2.AcknowledgeRequest(req_id=req_id)))

    def _process_rpc_error(self, e: grpc.RpcError) -> bool:
        """
        Processes RPC errors that occur while reading from data stream.
        Returns True if the error can be recovered from, False otherwise.
        """
        if not self.client_worker._can_reconnect(e):
            logger.info("Unrecoverable error in data channel.")
            return False

        # Ping the server to see if the current channel is reuseable
        try:
            ping_succeeded = self.client_worker.ping_server(timeout=5)
        except grpc.RpcError:
            ping_succeeded = False

        if not ping_succeeded:
            # Ping failed, try refreshing the data channel
            logger.warning(
                "Encountered connection issues in the data channel. "
                "Attempting to reconnect.")
            logger.debug(e)
            try:
                self.client_worker._connect_channel(reconnecting=True)
            except ConnectionError:
                logger.warning("Failed to reconnect the data channel")
                return False
            logger.info("Reconnection succeeded!")

        # Recreate the request queue, and resend outstanding requests
        self.request_queue = queue.Queue()
        with self.outstanding_requests_lock:
            for request in self.outstanding_requests.values():
                # Resend outstanding requests
                self.request_queue.put(request)
        return True

    def _shutdown(self):
        with self.cv:
            self._in_shutdown = True
            self.cv.notify_all()

    def close(self) -> None:
        if self.request_queue is not None:
            # Intentional shutdown, tell server it can clean up the connection
            # immediately and ignore the reconnect grace period.
            cleanup_request = ray_client_pb2.DataRequest(
                connection_cleanup=ray_client_pb2.ConnectionCleanupRequest())
            self.request_queue.put(cleanup_request)
            self.request_queue.put(None)
        if self.data_thread is not None:
            self.data_thread.join()

    def _blocking_send(self, req: ray_client_pb2.DataRequest
                       ) -> ray_client_pb2.DataResponse:
        if self._in_shutdown:
            from ray.util import disconnect
            disconnect()
            raise ConnectionError(
                "Request can't be sent because the data channel is "
                "terminated. This is likely because the data channel "
                "disconnected at some point before this request was "
                "prepared. Ray Client has been disconnected.")
        req_id = self._next_id()
        req.req_id = req_id
        self.request_queue.put(req)
        with self.outstanding_requests_lock:
            self.outstanding_requests[req_id] = req
        data = None
        with self.cv:
            self.cv.wait_for(
                lambda: req_id in self.ready_data or self._in_shutdown)
            if self._in_shutdown:
                from ray.util import disconnect
                disconnect()
                raise ConnectionError(
                    "Sending request failed because the data channel "
                    "terminated. This is usually due to an error "
                    f"in handling the most recent request: {req}. Ray Client "
                    "has been disconnected.")
            data = self.ready_data[req_id]
            with self.outstanding_requests_lock:
                del self.outstanding_requests[req_id]
            del self.ready_data[req_id]
        return data

    def _async_send(self,
                    req: ray_client_pb2.DataRequest,
                    callback: Optional[ResponseCallable] = None) -> None:
        if self._in_shutdown:
            from ray.util import disconnect
            disconnect()
            raise ConnectionError(
                "Request can't be sent because the data channel is "
                "terminated. This is likely because the data channel "
                "disconnected at some point before this request was "
                "prepared. Ray Client has been disconnected.")
        req_id = self._next_id()
        req.req_id = req_id
        self.asyncio_waiting_data[req_id] = callback
        with self.outstanding_requests_lock:
            self.outstanding_requests[req_id] = req
        self.request_queue.put(req)

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

    def RegisterGetCallback(self,
                            request: ray_client_pb2.GetRequest,
                            callback: ResponseCallable,
                            context=None) -> None:
        datareq = ray_client_pb2.DataRequest(get=request, )
        self._async_send(datareq, callback)

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
