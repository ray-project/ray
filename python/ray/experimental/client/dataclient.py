"""
This file implements a threaded stream controller to abstract a data stream
back to the ray clientserver.
"""
import queue
import threading

from typing import Any
from typing import Dict

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc


class DataClient:
    def __init__(self, channel, client_id):
        """Initializes a thread-safe datapath over a Ray Client gRPC channel.

        Args:
            channel: connected gRPC channel
        """
        self.channel = channel
        self.request_queue = queue.Queue()
        self.data_thread = self._start_datathread()
        self.ready_data: Dict[int, Any] = {}
        self.cv = threading.Condition()
        self._req_id = 0
        self._max_id = 100000
        self._client_id = client_id
        self.data_thread.start()

    def _next_id(self) -> int:
        self._req_id += 1
        if self._req_id > self._max_id:
            self._req_id = 1
        return self._req_id

    def _start_datathread(self) -> threading.Thread:
        return threading.Thread(target=self._data_main, args=(), daemon=True)

    def _data_main(self) -> None:
        stub = ray_client_pb2_grpc.RayletDataStreamerStub(self.channel)
        resp_stream = stub.Datapath(
            iter(self.request_queue.get, None),
            metadata=(
                ("client_id", self._client_id),
            )
        )
        for response in resp_stream:
            with self.cv:
                self.ready_data[response.req_id] = response
                self.cv.notify_all()

    def close(self, close_channel: bool = False) -> None:
        if self.request_queue is not None:
            self.request_queue.put(None)
            self.request_queue = None
        if self.data_thread is not None:
            self.data_thread.join()
            self.data_thread = None
        if close_channel:
            self.channel.close()

    def _blocking_send(
        self, req: ray_client_pb2.DataRequest
    ) -> ray_client_pb2.DataResponse:
        req_id = self._next_id()
        req.req_id = req_id
        self.request_queue.put(req)
        data = None
        with self.cv:
            self.cv.wait_for(lambda: req_id in self.ready_data)
            data = self.ready_data[req_id]
            del self.ready_data[req_id]
        if data is None:
            raise Exception("Couldn't get data")
        return data

    def GetObject(self, request: ray_client_pb2.GetRequest, context=None) -> ray_client_pb2.GetResponse:
        datareq = ray_client_pb2.DataRequest(
            get=request,
        )
        resp = self._blocking_send(datareq)
        return resp.get

    def PutObject(self, request: ray_client_pb2.PutRequest, context=None) -> ray_client_pb2.PutResponse:
        datareq = ray_client_pb2.DataRequest(
            put=request,
        )
        resp = self._blocking_send(datareq)
        return resp.put

    def ReleaseObject(self, request: ray_client_pb2.ReleaseRequest, context=None) -> ray_client_pb2.ReleaseRequest:
        datareq = ray_client_pb2.DataRequest(
            release=request,
        )
        resp = self._blocking_send(datareq)
        return resp.release
