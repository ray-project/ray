"""This file implements a threaded stream controller to return logs back from
the ray clientserver.
"""
import sys
import logging
import queue
import random
import threading
import time
import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

logger = logging.getLogger(__name__)
# TODO(barakmich): Running a logger in a logger causes loopback.
# The client logger need its own root -- possibly this one.
# For the moment, let's just not propogate beyond this point.
logger.propagate = False

# How often to make manual KeepAlive calls
LOGSCLIENT_KEEPALIVE_INTERVAL = 30


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
        self.stub = ray_client_pb2_grpc.RayletLogStreamerStub(self.channel)
        self.log_thread = self._start_logthread()
        self.log_thread.start()
        self.stop_keepalive = threading.Event()
        self.keepalive_thread = self._start_keepalive_thread()
        self.keepalive_thread.start()

    def _start_logthread(self) -> threading.Thread:
        return threading.Thread(target=self._log_main, args=(), daemon=True)

    def _start_keepalive_thread(self) -> threading.Thread:
        return threading.Thread(
            target=self._keepalive_main, args=(), daemon=True)

    def _log_main(self) -> None:
        log_stream = self.stub.Logstream(
            iter(self.request_queue.get, None), metadata=self._metadata)
        try:
            for record in log_stream:
                if record.level < 0:
                    self.stdstream(level=record.level, msg=record.msg)
                self.log(level=record.level, msg=record.msg)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                # Graceful shutdown. We've cancelled our own connection.
                logger.info("Cancelling logs channel")
            elif e.code() in (grpc.StatusCode.UNAVAILABLE,
                              grpc.StatusCode.RESOURCE_EXHAUSTED):
                # TODO(barakmich): The server may have
                # dropped. In theory, we can retry, as per
                # https://grpc.github.io/grpc/core/md_doc_statuscodes.html but
                # in practice we may need to think about the correct semantics
                # here.
                logger.info("Server disconnected from logs channel")
            else:
                # Some other, unhandled, gRPC error
                logger.exception(
                    f"Got Error from logger channel -- shutting down: {e}")

    def _keepalive_main(self) -> None:
        try:
            while not self.stop_keepalive.is_set():
                start = time.time()
                request = ray_client_pb2.KeepAliveRequest(
                    echo_request=random.randint(0, 2**16))
                duration = time.time() - start
                response = self.stub.KeepAlive(request)
                if response.echo_response != request.echo_request:
                    logger.warning("Logs client keepalive echo did not match.")
                wait_time = max(LOGSCLIENT_KEEPALIVE_INTERVAL - duration, 0)
                if self.stop_keepalive.wait(timeout=wait_time):
                    # Keep looping until the stop_keepalive event is set
                    break
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                # Graceful shutdown. We've cancelled our own connection.
                logger.info("Shutting down logs keep alive thread")
            elif e.code() in (grpc.StatusCode.UNAVAILABLE,
                              grpc.StatusCode.RESOURCE_EXHAUSTED):
                # Server may have dropped. Similar to _log_main we could
                # technically attempt a reconnect here
                logger.info("Server disconnected from logs channel")
            else:
                # Some other, unhandled, gRPC error
                logger.exception(
                    f"Got Error from logger channel -- shutting down: {e}")

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
        self.stop_keepalive.set()
        self.request_queue.put(None)
        if self.log_thread is not None:
            self.log_thread.join()
        if self.keepalive_thread is not None:
            self.keepalive_thread.join()

    def disable_logs(self) -> None:
        req = ray_client_pb2.LogSettingsRequest()
        req.enabled = False
        self.request_queue.put(req)
