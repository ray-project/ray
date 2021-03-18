"""This file implements a threaded stream controller to abstract a data stream
back to the ray clientserver.
"""
import logging
import queue
import threading
import grpc

from typing import Any
from typing import Dict

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

logger = logging.getLogger(__name__)

# The maximum field value for request_id -- which is also the maximum
# number of simultaneous in-flight requests.
INT32_MAX = (2**31) - 1


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
        self.request_queue = queue.Queue()
        self.data_thread = self._start_datathread()
        self.ready_data: Dict[int, Any] = {}
        self.cv = threading.Condition()
        self.lock = threading.RLock()
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
        stub = ray_client_pb2_grpc.RayletDataStreamerStub(self.channel)
        resp_stream = stub.Datapath(
            iter(self.request_queue.get, None),
            metadata=[("client_id", self._client_id)] + self._metadata,
            wait_for_ready=True)
        try:
            for response in resp_stream:
                if response.req_id == 0:
                    # This is not being waited for.
                    logger.debug(f"Got unawaited response {response}")
                    continue
                with self.cv:
                    self.ready_data[response.req_id] = response
                    self.cv.notify_all()
        except grpc.RpcError as e:
            with self.cv:
                self._in_shutdown = True
                self.cv.notify_all()
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
                logger.error(
                    f"Got Error from data channel -- shutting down: {e}")
                raise e

    def close(self) -> None:
        if self.request_queue is not None:
            self.request_queue.put(None)
        if self.data_thread is not None:
            self.data_thread.join()

    def _blocking_send(self, req: ray_client_pb2.DataRequest
                       ) -> ray_client_pb2.DataResponse:
        req_id = self._next_id()
        req.req_id = req_id
        self.request_queue.put(req)
        data = None
        with self.cv:
            self.cv.wait_for(
                lambda: req_id in self.ready_data or self._in_shutdown)
            if self._in_shutdown:
                raise ConnectionError(
                    "Cannot send request due to data channel "
                    f"shutting down. Request: {req}")
            data = self.ready_data[req_id]
            del self.ready_data[req_id]
        return data

    def _async_send(self, req: ray_client_pb2.DataRequest) -> None:
        req_id = self._next_id()
        req.req_id = req_id
        self.request_queue.put(req)

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
