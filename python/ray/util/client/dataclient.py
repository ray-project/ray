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
        self.request_queue = queue.Queue()
        self.data_thread = self._start_datathread()
        self.ready_data: Dict[int, Any] = {}

        # Waiting for response or shutdown.
        self.cv = threading.Condition()
        # Serialize access to self.request_queue, self.asyncio_waiting_data,
        # self._req_id, and self.data_thread
        self.lock = threading.RLock()

        # NOTE: Dictionary insertion is guaranteed to complete before lookup
        # and/or removal because of synchronization via the request_queue.
        self.asyncio_waiting_data: Dict[int, ResponseCallable] = {}
        self._req_id = 0
        self._client_id = client_id
        self._metadata = metadata
        self._in_shutdown = False
        self.data_thread.start()

    def _next_id(self) -> int:
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
            metadata=self._metadata,
            wait_for_ready=True)
        try:
            for response in resp_stream:
                if response.req_id == 0:
                    # This is not being waited for.
                    logger.debug(f"Got unawaited response {response}")
                    continue
                self.lock.acquire()
                if response.req_id in self.asyncio_waiting_data:
                    callback = self.asyncio_waiting_data.pop(response.req_id)
                    self.lock.release()
                    try:
                        callback(response)
                    except Exception:
                        logger.exception("Callback error:")
                else:
                    self.lock.release()
                    with self.cv:
                        self.ready_data[response.req_id] = response
                        self.cv.notify_all()
        except grpc.RpcError as e:
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

            # This data thread needs to call disconnect() below, and it
            # cannot join itself. It is exiting anyway so setting it to None.
            with self.lock:
                still_connected = self.data_thread is not None
                self.data_thread = None
            if still_connected:
                from ray.util import disconnect
                disconnect()

            with self.cv:
                with self.lock:
                    self._in_shutdown = True
                    # Abort async requests with the error.
                    err = ConnectionError(
                        "Request failed because of cancellation, "
                        "disconnection or failure of this or a previous "
                        f"request. Exception: {e}")
                    for callback in self.asyncio_waiting_data.values():
                        callback(err)
                    self.asyncio_waiting_data = {}
                    # Since self._in_shutdown is set to True, no new item
                    # will be added to self.asyncio_waiting_data

                self.cv.notify_all()

    def close(self) -> None:
        thread = None
        with self.lock:
            if self.request_queue is not None:
                self.request_queue.put(None)
            if self.data_thread is not None:
                thread = self.data_thread
                self.data_thread = None
        if thread is not None:
            thread.join()

    def _blocking_send(self, req: ray_client_pb2.DataRequest
                       ) -> ray_client_pb2.DataResponse:
        with self.lock:
            self._check_shutdown()
            req_id = self._next_id()
            req.req_id = req_id
            self.request_queue.put(req)
        with self.cv:
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

    def _check_shutdown(self):
        if not self._in_shutdown:
            return

        raise ConnectionError(
            "Request can't be sent because the data channel is "
            "terminated. This is likely because the data channel "
            "disconnected at some point before this request was "
            "prepared. Ray Client has been disconnected.")

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

    # TODO: convert to async
    def PutObject(self, request: ray_client_pb2.PutRequest,
                  context=None) -> ray_client_pb2.PutResponse:
        datareq = ray_client_pb2.DataRequest(put=request, )
        resp = self._blocking_send(datareq)
        return resp.put

    def WaitObject(self, request: ray_client_pb2.WaitRequest
                   ) -> ray_client_pb2.WaitResponse:
        req = ray_client_pb2.DataRequest(wait=request, )
        resp = self._blocking_send(req)
        return resp.wait

    def ReleaseObject(self,
                      request: ray_client_pb2.ReleaseRequest,
                      context=None) -> None:
        datareq = ray_client_pb2.DataRequest(release=request, )
        self._async_send(datareq)

    def Schedule(self, request: ray_client_pb2.ClientTask,
                 callback: ResponseCallable):
        datareq = ray_client_pb2.DataRequest(task=request)
        self._async_send(datareq, callback)

    def Terminate(self, request: ray_client_pb2.TerminateRequest
                  ) -> ray_client_pb2.TerminateResponse:
        req = ray_client_pb2.DataRequest(terminate=request, )
        resp = self._blocking_send(req)
        return resp.terminate

    def ListNamedActors(self,
                        request: ray_client_pb2.ClientListNamedActorsRequest
                        ) -> ray_client_pb2.ClientListNamedActorsResponse:
        req = ray_client_pb2.DataRequest(list_named_actors=request, )
        resp = self._blocking_send(req)
        return resp.list_named_actors

    def ClusterInfo(self, request: ray_client_pb2.ClusterInfoRequest
                    ) -> ray_client_pb2.ClusterInfoResponse:
        req = ray_client_pb2.DataRequest(cluster_info=request, )
        resp = self._blocking_send(req)
        return resp.cluster_info

    def KVGet(self, request: ray_client_pb2.KVGetRequest
              ) -> ray_client_pb2.KVGetResponse:
        req = ray_client_pb2.DataRequest(kv_get=request, )
        resp = self._blocking_send(req)
        return resp.kv_get

    def KVExists(self, request: ray_client_pb2.KVExistsRequest
                 ) -> ray_client_pb2.KVExistsResponse:
        req = ray_client_pb2.DataRequest(kv_exists=request, )
        resp = self._blocking_send(req)
        return resp.kv_exists

    def KVPut(self, request: ray_client_pb2.KVPutRequest
              ) -> ray_client_pb2.KVPutResponse:
        req = ray_client_pb2.DataRequest(kv_put=request, )
        resp = self._blocking_send(req)
        return resp.kv_put

    def KVDel(self, request: ray_client_pb2.KVDelRequest
              ) -> ray_client_pb2.KVDelResponse:
        req = ray_client_pb2.DataRequest(kv_del=request, )
        resp = self._blocking_send(req)
        return resp.kv_del

    def KVList(self, request: ray_client_pb2.KVListRequest
               ) -> ray_client_pb2.KVListResponse:
        req = ray_client_pb2.DataRequest(kv_list=request, )
        resp = self._blocking_send(req)
        return resp.kv_list
