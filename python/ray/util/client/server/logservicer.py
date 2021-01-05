"""This file responds to log stream requests and forwards logs
with its handler.
"""
import io
import threading
import queue
import logging
import grpc
import uuid

from ray.worker import print_worker_logs
from ray.ray_logging import global_worker_stdstream_dispatcher
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc

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

    def handle(self, data):
        logdata = ray_client_pb2.LogData()
        logdata.level = -2 if data["is_err"] else -1
        logdata.name = "stderr" if data["is_err"] else "stdout"
        with io.StringIO() as file:
            print_worker_logs(data, file)
            logdata.msg = file.getvalue()
        self.queue.put(logdata)

    def register_global(self):
        global_worker_stdstream_dispatcher.add_handler(self.id, self.handle)

    def unregister_global(self):
        global_worker_stdstream_dispatcher.remove_handler(self.id)


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
        logger.debug(f"closing log thread "
                     f"grpc error reading request_iterator: {e}")
    finally:
        if current_handler is not None:
            root_logger.setLevel(default_level)
            root_logger.removeHandler(current_handler)
            std_handler.unregister_global()
        log_queue.put(None)


class LogstreamServicer(ray_client_pb2_grpc.RayletLogStreamerServicer):
    def Logstream(self, request_iterator, context):
        logger.info("New logs connection")
        log_queue = queue.Queue()
        thread = threading.Thread(
            target=log_status_change_thread,
            args=(log_queue, request_iterator),
            daemon=True)
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
