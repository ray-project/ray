"""This file implements a threaded stream controller to return logs back from
the ray clientserver.
"""
import logging
import queue
import sys
import threading
import time
from typing import TYPE_CHECKING

import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.util.client.worker import Worker

logger = logging.getLogger(__name__)
# TODO(barakmich): Running a logger in a logger causes loopback.
# The client logger need its own root -- possibly this one.
# For the moment, let's just not propogate beyond this point.
logger.propagate = False


class LogstreamClient:
    def __init__(self, client_worker: "Worker", metadata: list):
        """Initializes a thread-safe log stream over a Ray Client gRPC channel.

        Args:
            client_worker: The Ray Client worker that manages this client
            metadata: metadata to pass to gRPC requests
        """
        self.client_worker = client_worker
        self._metadata = metadata
        self.request_queue = queue.Queue()
        self.log_thread = self._start_logthread()
        self.log_thread.start()
        self.last_req = None

    def _start_logthread(self) -> threading.Thread:
        return threading.Thread(target=self._log_main, args=(), daemon=True)

    def _log_main(self) -> None:
        reconnecting = False
        while not self.client_worker._in_shutdown:
            if reconnecting:
                # Refresh queue and retry last request
                self.request_queue = queue.Queue()
                if self.last_req:
                    self.request_queue.put(self.last_req)
            stub = ray_client_pb2_grpc.RayletLogStreamerStub(self.client_worker.channel)
            try:
                log_stream = stub.Logstream(
                    iter(self.request_queue.get, None), metadata=self._metadata
                )
            except ValueError:
                # Trying to use the stub on a cancelled channel will raise
                # ValueError. This should only happen when the data client
                # is attempting to reset the connection -- sleep and try
                # again.
                time.sleep(0.5)
                continue
            try:
                for record in log_stream:
                    if record.level < 0:
                        self.stdstream(level=record.level, msg=record.msg)
                    self.log(level=record.level, msg=record.msg)
                return
            except grpc.RpcError as e:
                reconnecting = self._process_rpc_error(e)
                if not reconnecting:
                    return

    def _process_rpc_error(self, e: grpc.RpcError) -> bool:
        """
        Processes RPC errors that occur while reading from data stream.
        Returns True if the error can be recovered from, False otherwise.
        """
        if self.client_worker._can_reconnect(e):
            if log_once("lost_reconnect_logs"):
                logger.warning(
                    "Log channel is reconnecting. Logs produced while "
                    "the connection was down can be found on the head "
                    "node of the cluster in "
                    "`ray_client_server_[port].out`"
                )
            logger.debug("Log channel dropped, retrying.")
            time.sleep(0.5)
            return True
        logger.debug("Shutting down log channel.")
        if not self.client_worker._in_shutdown:
            logger.exception("Unexpected exception:")
        return False

    def log(self, level: int, msg: str):
        """Log the message from the log stream.
        By default, calls logger.log but this can be overridden.

        Args:
            level: The loglevel of the received log message
            msg: The content of the message
        """
        logger.log(level=level, msg=msg)

    def stdstream(self, level: int, msg: str):
        """Log the stdout/stderr entry from the log stream.
        By default, calls print but this can be overridden.

        Args:
            level: The loglevel of the received log message
            msg: The content of the message
        """
        print_file = sys.stderr if level == -2 else sys.stdout
        print(msg, file=print_file, end="")

    def set_logstream_level(self, level: int):
        logger.setLevel(level)
        req = ray_client_pb2.LogSettingsRequest()
        req.enabled = True
        req.loglevel = level
        self.request_queue.put(req)
        self.last_req = req

    def close(self) -> None:
        self.request_queue.put(None)
        if self.log_thread is not None:
            self.log_thread.join()

    def disable_logs(self) -> None:
        req = ray_client_pb2.LogSettingsRequest()
        req.enabled = False
        self.request_queue.put(req)
        self.last_req = req
