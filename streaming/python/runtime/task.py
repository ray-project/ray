import logging
import pickle
import threading
import time
import typing
from abc import ABC, abstractmethod
from typing import Optional

from ray.streaming.collector import OutputCollector
from ray.streaming.config import Config
from ray.streaming.context import RuntimeContextImpl
from ray.streaming.generated import remote_call_pb2
from ray.streaming.runtime import serialization
from ray.streaming.runtime.command import WorkerCommitReport
from ray.streaming.runtime.failover import Barrier, OpCheckpointInfo
from ray.streaming.runtime.remote_call import RemoteCallMst
from ray.streaming.runtime.serialization import \
    PythonSerializer, CrossLangSerializer
from ray.streaming.runtime.transfer import CheckpointBarrier
from ray.streaming.runtime.transfer import DataMessage
from ray.streaming.runtime.transfer import ChannelID, DataWriter, DataReader
from ray.streaming.runtime.transfer import ChannelRecoverInfo
from ray.streaming.runtime.transfer import ChannelInterruptException

if typing.TYPE_CHECKING:
    from ray.streaming.runtime.worker import JobWorker
    from ray.streaming.runtime.processor import Processor, SourceProcessor

logger = logging.getLogger(__name__)


class StreamTask(ABC):
    """Base class for all streaming tasks. Each task runs a processor."""

    def __init__(self, task_id: int, processor: "Processor",
                 worker: "JobWorker", last_checkpoint_id: int):
        self.worker_context = worker.worker_context
        self.vertex_context = worker.execution_vertex_context
        self.task_id = task_id
        self.processor = processor
        self.worker = worker
        self.config: dict = worker.config
        self.reader: Optional[DataReader] = None
        self.writer: Optional[DataWriter] = None
        self.is_initial_state = True
        self.last_checkpoint_id: int = last_checkpoint_id
        self.thread = threading.Thread(target=self.run, daemon=True)

    def do_checkpoint(self, checkpoint_id: int, input_points):
        logger.info("Start do checkpoint, cp id {}, inputPoints {}.".format(
            checkpoint_id, input_points))

        output_points = None
        if self.writer is not None:
            output_points = self.writer.get_output_checkpoints()

        operator_checkpoint = self.processor.save_checkpoint()
        op_checkpoint_info = OpCheckpointInfo(
            operator_checkpoint, input_points, output_points, checkpoint_id)
        self.__save_cp_state_and_report(op_checkpoint_info, checkpoint_id)

        barrier_pb = remote_call_pb2.Barrier()
        barrier_pb.id = checkpoint_id
        byte_buffer = barrier_pb.SerializeToString()
        if self.writer is not None:
            self.writer.broadcast_barrier(checkpoint_id, byte_buffer)
        logger.info("Operator checkpoint {} finish.".format(checkpoint_id))

    def __save_cp_state_and_report(self, op_checkpoint_info, checkpoint_id):
        logger.info(
            "Start to save cp state and report, checkpoint id is {}.".format(
                checkpoint_id))
        self.__save_cp(op_checkpoint_info, checkpoint_id)
        self.__report_commit(checkpoint_id)
        self.last_checkpoint_id = checkpoint_id

    def __save_cp(self, op_checkpoint_info, checkpoint_id):
        logger.info("save operator cp, op_checkpoint_info={}".format(
            op_checkpoint_info))
        cp_bytes = pickle.dumps(op_checkpoint_info)
        self.worker.context_backend.put(
            self.__gen_op_checkpoint_key(checkpoint_id), cp_bytes)

    def __report_commit(self, checkpoint_id: int):
        logger.info("Report commit, checkpoint id {}.".format(checkpoint_id))
        report = WorkerCommitReport(self.vertex_context.actor_id.binary(),
                                    checkpoint_id)
        RemoteCallMst.report_job_worker_commit(self.worker.master_actor,
                                               report)

    def clear_expired_cp_state(self, checkpoint_id):
        cp_key = self.__gen_op_checkpoint_key(checkpoint_id)
        self.worker.context_backend.remove(cp_key)

    def clear_expired_queue_msg(self, checkpoint_id):
        # clear operator checkpoint
        if self.writer is not None:
            self.writer.clear_checkpoint(checkpoint_id)

    def request_rollback(self, exception_msg: str):
        self.worker.request_rollback(exception_msg)

    def __gen_op_checkpoint_key(self, checkpoint_id):
        op_checkpoint_key = Config.JOB_WORKER_OP_CHECKPOINT_PREFIX_KEY + str(
            self.vertex_context.job_name) + "_" + str(
                self.vertex_context.exe_vertex_name) + "_" + str(checkpoint_id)
        logger.info(
            "Generate op checkpoint key {}. ".format(op_checkpoint_key))
        return op_checkpoint_key

    def prepare_task(self, is_recreate: bool):
        logger.info(
            "Preparing stream task, is_recreate={}.".format(is_recreate))
        channel_conf = dict(self.worker.config)
        channel_size = int(
            self.worker.config.get(Config.CHANNEL_SIZE,
                                   Config.CHANNEL_SIZE_DEFAULT))
        channel_conf[Config.CHANNEL_SIZE] = channel_size
        channel_conf[Config.CHANNEL_TYPE] = self.worker.config \
            .get(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL)

        execution_vertex_context = self.worker.execution_vertex_context
        build_time = execution_vertex_context.build_time

        # when use memory state, if actor throw exception, will miss state
        op_checkpoint_info = OpCheckpointInfo()

        cp_bytes = None
        # get operator checkpoint
        if is_recreate:
            cp_key = self.__gen_op_checkpoint_key(self.last_checkpoint_id)
            logger.info("Getting task checkpoints from state, "
                        "cpKey={}, checkpointId={}.".format(
                            cp_key, self.last_checkpoint_id))
            cp_bytes = self.worker.context_backend.get(cp_key)
            if cp_bytes is None:
                msg = "Task recover failed, checkpoint is null!"\
                     "cpKey={}".format(cp_key)
                raise RuntimeError(msg)

        if cp_bytes is not None:
            op_checkpoint_info = pickle.loads(cp_bytes)
            self.processor.load_checkpoint(op_checkpoint_info.operator_point)
            logger.info("Stream task recover from checkpoint state,"
                        "checkpoint bytes len={}, checkpointInfo={}.".format(
                            cp_bytes.__len__(), op_checkpoint_info))

        # writers
        collectors = []
        output_actors_map = {}
        for edge in execution_vertex_context.output_execution_edges:
            target_task_id = edge.target_execution_vertex_id
            target_actor = execution_vertex_context \
                .get_target_actor_by_execution_vertex_id(target_task_id)
            channel_name = ChannelID.gen_id(self.task_id, target_task_id,
                                            build_time)
            output_actors_map[channel_name] = target_actor

        if len(output_actors_map) > 0:
            channel_str_ids = list(output_actors_map.keys())
            target_actors = list(output_actors_map.values())
            logger.info("Create DataWriter channel_ids {},"
                        "target_actors {}, output_points={}.".format(
                            channel_str_ids, target_actors,
                            op_checkpoint_info.output_points))
            self.writer = DataWriter(channel_str_ids, target_actors,
                                     channel_conf)
            logger.info("Create DataWriter succeed channel_ids {}, "
                        "target_actors {}.".format(channel_str_ids,
                                                   target_actors))
            for edge in execution_vertex_context.output_execution_edges:
                collectors.append(
                    OutputCollector(self.writer, channel_str_ids,
                                    target_actors, edge.partition))

        # readers
        input_actor_map = {}
        for edge in execution_vertex_context.input_execution_edges:
            source_task_id = edge.source_execution_vertex_id
            source_actor = execution_vertex_context \
                .get_source_actor_by_execution_vertex_id(source_task_id)
            channel_name = ChannelID.gen_id(source_task_id, self.task_id,
                                            build_time)
            input_actor_map[channel_name] = source_actor

        if len(input_actor_map) > 0:
            channel_str_ids = list(input_actor_map.keys())
            from_actors = list(input_actor_map.values())
            logger.info("Create DataReader, channels {},"
                        "input_actors {}, input_points={}.".format(
                            channel_str_ids, from_actors,
                            op_checkpoint_info.input_points))
            self.reader = DataReader(channel_str_ids, from_actors,
                                     channel_conf)

            def exit_handler():
                # Make DataReader stop read data when MockQueue destructor
                # gets called to avoid crash
                self.cancel_task()

            import atexit
            atexit.register(exit_handler)

        runtime_context = RuntimeContextImpl(
            self.worker.task_id,
            execution_vertex_context.execution_vertex.execution_vertex_index,
            execution_vertex_context.get_parallelism(),
            config=channel_conf,
            job_config=channel_conf)
        logger.info("open Processor {}".format(self.processor))
        self.processor.open(collectors, runtime_context)

        # immediately save cp. In case of FO in cp 0
        # or use old cp in multi node FO.
        self.__save_cp(op_checkpoint_info, self.last_checkpoint_id)

    def recover(self, is_recreate: bool):
        self.prepare_task(is_recreate)

        recover_info = ChannelRecoverInfo()
        if self.reader is not None:
            recover_info = self.reader.get_channel_recover_info()

        self.thread.start()

        logger.info("Start operator success.")
        return recover_info

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def cancel_task(self):
        pass

    @abstractmethod
    def commit_trigger(self, barrier: Barrier) -> bool:
        pass


class InputStreamTask(StreamTask):
    """Base class for stream tasks that execute a
    :class:`runtime.processor.OneInputProcessor` or
    :class:`runtime.processor.TwoInputProcessor` """

    def commit_trigger(self, barrier):
        raise RuntimeError(
            "commit_trigger is only supported in SourceStreamTask.")

    def __init__(self, task_id, processor_instance, worker,
                 last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)
        self.running = True
        self.stopped = False
        self.read_timeout_millis = \
            int(worker.config.get(Config.READ_TIMEOUT_MS,
                                  Config.DEFAULT_READ_TIMEOUT_MS))
        self.python_serializer = PythonSerializer()
        self.cross_lang_serializer = CrossLangSerializer()

    def run(self):
        logger.info("Input task thread start.")
        try:
            while self.running:
                self.worker.initial_state_lock.acquire()
                try:
                    item = self.reader.read(self.read_timeout_millis)
                    self.is_initial_state = False
                finally:
                    self.worker.initial_state_lock.release()

                if item is None:
                    continue

                if isinstance(item, DataMessage):
                    msg_data = item.body
                    type_id = msg_data[0]
                    if type_id == serialization.PYTHON_TYPE_ID:
                        msg = self.python_serializer.deserialize(msg_data[1:])
                    else:
                        msg = self.cross_lang_serializer.deserialize(
                            msg_data[1:])
                    self.processor.process(msg)
                elif isinstance(item, CheckpointBarrier):
                    logger.info("Got barrier:{}".format(item))
                    logger.info("Start to do checkpoint {}.".format(
                        item.checkpoint_id))

                    input_points = item.get_input_checkpoints()

                    self.do_checkpoint(item.checkpoint_id, input_points)
                    logger.info("Do checkpoint {} success.".format(
                        item.checkpoint_id))
                else:
                    raise RuntimeError(
                        "Unknown item type! item={}".format(item))

        except ChannelInterruptException:
            logger.info("queue has stopped.")
        except BaseException as e:
            logger.exception(
                "Last success checkpointId={}, now occur error.".format(
                    self.last_checkpoint_id))
            self.request_rollback(str(e))

        logger.info("Source fetcher thread exit.")
        self.stopped = True

    def cancel_task(self):
        self.running = False
        while not self.stopped:
            time.sleep(0.5)
            pass


class OneInputStreamTask(InputStreamTask):
    """A stream task for executing :class:`runtime.processor.OneInputProcessor`
    """

    def __init__(self, task_id, processor_instance, worker,
                 last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)


class SourceStreamTask(StreamTask):
    """A stream task for executing :class:`runtime.processor.SourceProcessor`
    """
    processor: "SourceProcessor"

    def __init__(self, task_id: int, processor_instance: "SourceProcessor",
                 worker: "JobWorker", last_checkpoint_id):
        super().__init__(task_id, processor_instance, worker,
                         last_checkpoint_id)
        self.running = True
        self.stopped = False
        self.__pending_barrier: Optional[Barrier] = None

    def run(self):
        logger.info("Source task thread start.")
        try:
            while self.running:
                self.processor.fetch()
                # check checkpoint
                if self.__pending_barrier is not None:
                    # source fetcher only have outputPoints
                    barrier = self.__pending_barrier
                    logger.info("Start to do checkpoint {}.".format(
                        barrier.id))
                    self.do_checkpoint(barrier.id, barrier)
                    logger.info("Finish to do checkpoint {}.".format(
                        barrier.id))
                    self.__pending_barrier = None

        except ChannelInterruptException:
            logger.info("queue has stopped.")
        except Exception as e:
            logger.exception(
                "Last success checkpointId={}, now occur error.".format(
                    self.last_checkpoint_id))
            self.request_rollback(str(e))

        logger.info("Source fetcher thread exit.")
        self.stopped = True

    def commit_trigger(self, barrier):
        if self.__pending_barrier is not None:
            logger.warning(
                "Last barrier is not broadcast now, skip this barrier trigger."
            )
            return False

        self.__pending_barrier = barrier
        return True

    def cancel_task(self):
        self.running = False
        while not self.stopped:
            time.sleep(0.5)
            pass
