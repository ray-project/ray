import logging

import ray
import ray.streaming._streaming as _streaming
import ray.streaming.generated.remote_call_pb2 as remote_call_pb
import ray.streaming.runtime.processor as processor
from ray.streaming.config import Config
from ray.streaming.runtime.graph import ExecutionGraph
from ray.streaming.runtime.task import SourceStreamTask, OneInputStreamTask

logger = logging.getLogger(__name__)

# special flag to indicate this actor not ready
_NOT_READY_FLAG_ = b" " * 4


@ray.remote
class JobWorker(object):
    """A streaming job worker is used to execute user-defined function and
    interact with `JobMaster`"""

    def __init__(self):
        self.worker_context = None
        self.task_id = None
        self.config = None
        self.execution_graph = None
        self.execution_task = None
        self.execution_node = None
        self.stream_processor = None
        self.task = None
        self.reader_client = None
        self.writer_client = None

    def init(self, worker_context_bytes):
        worker_context = remote_call_pb.WorkerContext()
        worker_context.ParseFromString(worker_context_bytes)
        self.worker_context = worker_context
        self.task_id = worker_context.task_id
        self.config = worker_context.conf
        execution_graph = ExecutionGraph(worker_context.graph)
        self.execution_graph = execution_graph
        self.execution_task = self.execution_graph. \
            get_execution_task_by_task_id(self.task_id)
        self.execution_node = self.execution_graph. \
            get_execution_node_by_task_id(self.task_id)
        operator = self.execution_node.stream_operator
        self.stream_processor = processor.build_processor(operator)
        logger.info(
            "Initializing JobWorker, task_id: {}, operator: {}.".format(
                self.task_id, self.stream_processor))

        if self.config.get(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL):
            self.reader_client = _streaming.ReaderClient()
            self.writer_client = _streaming.WriterClient()

        self.task = self.create_stream_task()
        self.task.start()
        logger.info("JobWorker init succeed")
        return True

    def create_stream_task(self):
        if isinstance(self.stream_processor, processor.SourceProcessor):
            return SourceStreamTask(self.task_id, self.stream_processor, self)
        elif isinstance(self.stream_processor, processor.OneInputProcessor):
            return OneInputStreamTask(self.task_id, self.stream_processor,
                                      self)
        else:
            raise Exception("Unsupported processor type: " +
                            type(self.stream_processor))

    def on_reader_message(self, buffer: bytes):
        """Called by upstream queue writer to send data message to downstream
        queue reader.
        """
        self.reader_client.on_reader_message(buffer)

    def on_reader_message_sync(self, buffer: bytes):
        """Called by upstream queue writer to send control message to downstream
        downstream queue reader.
        """
        if self.reader_client is None:
            return _NOT_READY_FLAG_
        result = self.reader_client.on_reader_message_sync(buffer)
        return result.to_pybytes()

    def on_writer_message(self, buffer: bytes):
        """Called by downstream queue reader to send notify message to
        upstream queue writer.
        """
        self.writer_client.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        """Called by downstream queue reader to send control message to
        upstream queue writer.
        """
        if self.writer_client is None:
            return _NOT_READY_FLAG_
        result = self.writer_client.on_writer_message_sync(buffer)
        return result.to_pybytes()
