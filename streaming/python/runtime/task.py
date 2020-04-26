import logging
import pickle
import threading
from abc import ABC, abstractmethod

from ray.streaming.collector import OutputCollector
from ray.streaming.config import Config
from ray.streaming.context import RuntimeContextImpl
from ray.streaming.runtime.transfer import ChannelID, DataWriter, DataReader

logger = logging.getLogger(__name__)


class StreamTask(ABC):
    """Base class for all streaming tasks. Each task runs a processor."""

    def __init__(self, task_id, processor, worker):
        self.task_id = task_id
        self.processor = processor
        self.worker = worker
        self.reader = None  # DataReader
        self.writers = {}  # ExecutionEdge -> DataWriter
        self.thread = None
        self.prepare_task()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def prepare_task(self):
        channel_conf = dict(self.worker.config)
        channel_size = int(
            self.worker.config.get(Config.CHANNEL_SIZE,
                                   Config.CHANNEL_SIZE_DEFAULT))
        channel_conf[Config.CHANNEL_SIZE] = channel_size
        channel_conf[Config.CHANNEL_TYPE] = self.worker.config \
            .get(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL)

        execution_graph = self.worker.execution_graph
        execution_node = self.worker.execution_node
        # writers
        collectors = []
        for edge in execution_node.output_edges:
            output_actor_ids = {}
            task_id2_worker = execution_graph.get_task_id2_worker_by_node_id(
                edge.target_node_id)
            for target_task_id, target_actor in task_id2_worker.items():
                channel_name = ChannelID.gen_id(self.task_id, target_task_id,
                                                execution_graph.build_time())
                output_actor_ids[channel_name] = target_actor
            if len(output_actor_ids) > 0:
                channel_ids = list(output_actor_ids.keys())
                to_actor_ids = list(output_actor_ids.values())
                writer = DataWriter(channel_ids, to_actor_ids, channel_conf)
                logger.info("Create DataWriter succeed.")
                self.writers[edge] = writer
                collectors.append(
                    OutputCollector(channel_ids, writer, edge.partition))

        # readers
        input_actor_ids = {}
        for edge in execution_node.input_edges:
            task_id2_worker = execution_graph.get_task_id2_worker_by_node_id(
                edge.src_node_id)
            for src_task_id, src_actor in task_id2_worker.items():
                channel_name = ChannelID.gen_id(src_task_id, self.task_id,
                                                execution_graph.build_time())
                input_actor_ids[channel_name] = src_actor
        if len(input_actor_ids) > 0:
            channel_ids = list(input_actor_ids.keys())
            from_actor_ids = list(input_actor_ids.values())
            logger.info("Create DataReader, channels {}.".format(channel_ids))
            self.reader = DataReader(channel_ids, from_actor_ids, channel_conf)

            def exit_handler():
                # Make DataReader stop read data when MockQueue destructor
                # gets called to avoid crash
                self.cancel_task()

            import atexit
            atexit.register(exit_handler)

        runtime_context = RuntimeContextImpl(
            self.worker.execution_task.task_id,
            self.worker.execution_task.task_index, execution_node.parallelism)
        logger.info("open Processor {}".format(self.processor))
        self.processor.open(collectors, runtime_context)

    @abstractmethod
    def init(self):
        pass

    def start(self):
        self.thread.start()

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def cancel_task(self):
        pass


class InputStreamTask(StreamTask):
    """Base class for stream tasks that execute a
    :class:`runtime.processor.OneInputProcessor` or
    :class:`runtime.processor.TwoInputProcessor` """

    def __init__(self, task_id, processor_instance, worker):
        super().__init__(task_id, processor_instance, worker)
        self.running = True
        self.stopped = False
        self.read_timeout_millis = \
            int(worker.config.get(Config.READ_TIMEOUT_MS,
                                  Config.DEFAULT_READ_TIMEOUT_MS))

    def init(self):
        pass

    def run(self):
        while self.running:
            item = self.reader.read(self.read_timeout_millis)
            if item is not None:
                msg_data = item.body()
                msg = pickle.loads(msg_data)
                self.processor.process(msg)
        self.stopped = True

    def cancel_task(self):
        self.running = False
        while not self.stopped:
            pass


class OneInputStreamTask(InputStreamTask):
    """A stream task for executing :class:`runtime.processor.OneInputProcessor`
    """

    def __init__(self, task_id, processor_instance, worker):
        super().__init__(task_id, processor_instance, worker)


class SourceStreamTask(StreamTask):
    """A stream task for executing :class:`runtime.processor.SourceProcessor`
    """

    def __init__(self, task_id, processor_instance, worker):
        super().__init__(task_id, processor_instance, worker)

    def init(self):
        pass

    def run(self):
        self.processor.run()

    def cancel_task(self):
        pass
