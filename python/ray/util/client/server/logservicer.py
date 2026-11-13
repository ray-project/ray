"""This file responds to log stream requests and forwards logs
with its handler.
"""
import io
import logging
import queue
import threading
import uuid

import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray._private.ray_constants import (
    RAY_DEDUP_LOGS_AGG_WINDOW_S,
    RAY_DEDUP_LOGS_ALLOW_REGEX,
    RAY_DEDUP_LOGS_SKIP_REGEX,
)
from ray._private.ray_logging import (
    LogDeduplicator,
    global_worker_stdstream_dispatcher,
)
from ray._private.worker import print_worker_logs
from ray.util.client.common import CLIENT_SERVER_MAX_THREADS

logger = logging.getLogger(__name__)


class LogstreamHandler(logging.Handler):
    def __init__(self, queue, level):
        super().__init__()
        self.queue = queue
        self.level = level

    def emit(self, record: logging.LogRecord):
        logdata = ray_client_pb2.LogData()
        logdata.msg = record.getMessage()
        logdata.level = record.levelno
        logdata.name = record.name
        self.queue.put(logdata)


class StdStreamHandler:
    def __init__(self, queue):
        self.queue = queue
        self.id = str(uuid.uuid4())
        # Per-handler deduplicators: the dispatcher hands every batch to each
        # handler (and to the server's own print_to_stdstream), so sharing the
        # global ones would count each line once per handler and mix clients.
        dedup_args = (
            RAY_DEDUP_LOGS_AGG_WINDOW_S,
            RAY_DEDUP_LOGS_ALLOW_REGEX,
            RAY_DEDUP_LOGS_SKIP_REGEX,
        )
        self.stdout_deduplicator = LogDeduplicator(*dedup_args)
        self.stderr_deduplicator = LogDeduplicator(*dedup_args)

    def handle(self, data):
        is_err = data["is_err"]
        # Mirror ray._private.worker.print_to_stdstream so that Ray Client
        # drivers get the same log deduplication as regular drivers.
        if data.get("pid") in ["autoscaler"]:
            batches = [data]
        else:
            deduplicator = (
                self.stderr_deduplicator if is_err else self.stdout_deduplicator
            )
            batches = deduplicator.deduplicate(data)
        self._enqueue(batches, is_err)

    def _enqueue(self, batches, is_err: bool):
        for batch in batches:
            with io.StringIO() as file:
                print_worker_logs(batch, file)
                msg = file.getvalue()
            if not msg:
                continue
            logdata = ray_client_pb2.LogData()
            logdata.level = -2 if is_err else -1
            logdata.name = "stderr" if is_err else "stdout"
            logdata.msg = msg
            self.queue.put(logdata)

    def register_global(self):
        global_worker_stdstream_dispatcher.add_handler(self.id, self.handle)

    def unregister_global(self):
        global_worker_stdstream_dispatcher.remove_handler(self.id)
        # Forward any deduplicated lines still buffered in the aggregation window.
        self._enqueue(self.stdout_deduplicator.flush(), is_err=False)
        self._enqueue(self.stderr_deduplicator.flush(), is_err=True)


def log_status_change_thread(log_queue, request_iterator):
    std_handler = StdStreamHandler(log_queue)
    current_handler = None
    root_logger = logging.getLogger("ray")
    default_level = root_logger.getEffectiveLevel()
    try:
        for req in request_iterator:
            if current_handler is not None:
                root_logger.setLevel(default_level)
                root_logger.removeHandler(current_handler)
                std_handler.unregister_global()
            if not req.enabled:
                current_handler = None
                continue
            current_handler = LogstreamHandler(log_queue, req.loglevel)
            std_handler.register_global()
            root_logger.addHandler(current_handler)
            root_logger.setLevel(req.loglevel)
    except grpc.RpcError as e:
        logger.debug(f"closing log thread " f"grpc error reading request_iterator: {e}")
    finally:
        if current_handler is not None:
            root_logger.setLevel(default_level)
            root_logger.removeHandler(current_handler)
            std_handler.unregister_global()
        log_queue.put(None)


class LogstreamServicer(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def __init__(self):
        super().__init__()
        self.num_clients = 0
        self.client_lock = threading.Lock()

    def Logstream(self, request_iterator, context):
        initialized = False
        with self.client_lock:
            threshold = CLIENT_SERVER_MAX_THREADS / 2
            if self.num_clients + 1 >= threshold:
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                logger.warning(
                    f"Logstream: Num clients {self.num_clients} has reached "
                    f"the threshold {threshold}. Rejecting new connection."
                )
                return
            self.num_clients += 1
            initialized = True
            logger.info(
                "New logs connection established. " f"Total clients: {self.num_clients}"
            )
        log_queue = queue.Queue()
        thread = threading.Thread(
            target=log_status_change_thread,
            args=(log_queue, request_iterator),
            daemon=True,
        )
        thread.start()
        try:
            queue_iter = iter(log_queue.get, None)
            for record in queue_iter:
                if record is None:
                    break
                yield record
        except grpc.RpcError as e:
            logger.debug(f"Closing log channel: {e}")
        finally:
            thread.join()
            with self.client_lock:
                if initialized:
                    self.num_clients -= 1
