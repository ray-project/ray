"""This file implements a threaded stream controller to return logs back from
the ray clientserver.
"""
import sys
import logging
import queue
import threading
import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

logger = logging.getLogger(__name__)
# TODO(barakmich): Running a logger in a logger causes loopback.
# The client logger need its own root -- possibly this one.
# For the moment, let's just not propogate beyond this point.
logger.propagate = False


class LogstreamClient:
    def __init__(self, channel: "grpc._channel.Channel", metadata: list):
        """Initializes a thread-safe log stream over a Ray Client gRPC channel.

        Args:
            channel: connected gRPC channel
            metadata: metadata to pass to gRPC requests
        """
        self.channel = channel
        self._metadata = metadata
        self.request_queue = queue.Queue()
        self.log_thread = self._start_logthread()
        self.log_thread.start()

    def _start_logthread(self) -> threading.Thread:
        return threading.Thread(target=self._log_main, args=(), daemon=True)

    def _log_main(self) -> None:
        stub = ray_client_pb2_grpc.RayletLogStreamerStub(self.channel)
        log_stream = stub.Logstream(
            iter(self.request_queue.get, None), metadata=self._metadata)
        try:
            for record in log_stream:
                if record.level < 0:
                    self.stdstream(level=record.level, msg=record.msg)
                self.log(level=record.level, msg=record.msg)
        except grpc.RpcError as e:
            if grpc.StatusCode.CANCELLED != e.code():
                # Not just shutting down normally
                logger.error(
                    f"Got Error from logger channel -- shutting down: {e}")
                raise e

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

    def close(self) -> None:
        self.request_queue.put(None)
        if self.log_thread is not None:
            self.log_thread.join()

    def disable_logs(self) -> None:
        req = ray_client_pb2.LogSettingsRequest()
        req.enabled = False
        self.request_queue.put(req)
