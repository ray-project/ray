import enum
import logging.config
import os
import threading
import time
from typing import Optional

import ray
import ray.streaming.runtime.processor as processor
from ray.actor import ActorHandle
from ray.streaming.generated import remote_call_pb2
from ray.streaming.runtime.command import WorkerRollbackRequest
from ray.streaming.runtime.failover import Barrier
from ray.streaming.runtime.graph import ExecutionVertexContext, ExecutionVertex
from ray.streaming.runtime.remote_call import CallResult, RemoteCallMst
from ray.streaming.runtime.context_backend import ContextBackendFactory
from ray.streaming.runtime.task import SourceStreamTask, OneInputStreamTask
from ray.streaming.runtime.transfer import channel_bytes_to_str
from ray.streaming.config import Config
import ray.streaming._streaming as _streaming

logger = logging.getLogger(__name__)

# special flag to indicate this actor not ready
_NOT_READY_FLAG_ = b" " * 4


@ray.remote
class JobWorker(object):
    """A streaming job worker is used to execute user-defined function and
    interact with `JobMaster`"""
    master_actor: Optional[ActorHandle]
    worker_context: Optional[remote_call_pb2.PythonJobWorkerContext]
    execution_vertex_context: Optional[ExecutionVertexContext]
    __need_rollback: bool

    def __init__(self, execution_vertex_pb_bytes):
        logger.info("Creating job worker, pid={}".format(os.getpid()))
        execution_vertex_pb = remote_call_pb2\
            .ExecutionVertexContext.ExecutionVertex()
        execution_vertex_pb.ParseFromString(execution_vertex_pb_bytes)
        self.execution_vertex = ExecutionVertex(execution_vertex_pb)
        self.config = self.execution_vertex.config
        self.worker_context = None
        self.execution_vertex_context = None
        self.task_id = None
        self.task = None
        self.stream_processor = None
        self.master_actor = None
        self.context_backend = ContextBackendFactory.get_context_backend(
            self.config)
        self.initial_state_lock = threading.Lock()
        self.__rollback_cnt: int = 0
        self.__is_recreate: bool = False
        self.__state = WorkerState()
        self.__need_rollback = True
        self.reader_client = None
        self.writer_client = None
        try:
            # load checkpoint
            was_reconstructed = ray.get_runtime_context(
            ).was_current_actor_reconstructed

            logger.info(
                "Worker was reconstructed: {}".format(was_reconstructed))
            if was_reconstructed:
                job_worker_context_key = self.__get_job_worker_context_key()
                logger.info("Worker get checkpoint state by key: {}.".format(
                    job_worker_context_key))
                context_bytes = self.context_backend.get(
                    job_worker_context_key)
                if context_bytes is not None and context_bytes.__len__() > 0:
                    self.init(context_bytes)
                    self.request_rollback(
                        "Python worker recover from checkpoint.")
                else:
                    logger.error(
                        "Error! Worker get checkpoint state by key {}"
                        " returns None, please check your state backend"
                        ", only reliable state backend supports fail-over."
                        .format(job_worker_context_key))
        except Exception:
            logger.exception("Error in __init__ of JobWorker")
        logger.info("Creating job worker succeeded. worker config {}".format(
            self.config))

    def init(self, worker_context_bytes):
        logger.info("Start to init job worker")
        try:
            # deserialize context
            worker_context = remote_call_pb2.PythonJobWorkerContext()
            worker_context.ParseFromString(worker_context_bytes)
            self.worker_context = worker_context
            self.master_actor = ActorHandle._deserialization_helper(
                worker_context.master_actor)

            # build vertex context from pb
            self.execution_vertex_context = ExecutionVertexContext(
                worker_context.execution_vertex_context)
            self.execution_vertex = self\
                .execution_vertex_context.execution_vertex

            # save context
            job_worker_context_key = self.__get_job_worker_context_key()
            self.context_backend.put(job_worker_context_key,
                                     worker_context_bytes)

            # use vertex id as task id
            self.task_id = self.execution_vertex_context.get_task_id()
            # build and get processor from operator
            operator = self.execution_vertex_context.stream_operator
            self.stream_processor = processor.build_processor(operator)
            logger.info("Initializing job worker, exe_vertex_name={},"
                        "task_id: {}, operator: {}, pid={}".format(
                            self.execution_vertex_context.exe_vertex_name,
                            self.task_id, self.stream_processor, os.getpid()))

            # get config from vertex
            self.config = self.execution_vertex_context.config

            if self.config.get(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL):
                self.reader_client = _streaming.ReaderClient()
                self.writer_client = _streaming.WriterClient()

            logger.info("Job worker init succeeded.")
        except Exception:
            logger.exception("Error when init job worker.")
            return False
        return True

    def create_stream_task(self, checkpoint_id):
        if isinstance(self.stream_processor, processor.SourceProcessor):
            return SourceStreamTask(self.task_id, self.stream_processor, self,
                                    checkpoint_id)
        elif isinstance(self.stream_processor, processor.OneInputProcessor):
            return OneInputStreamTask(self.task_id, self.stream_processor,
                                      self, checkpoint_id)
        else:
            raise Exception("Unsupported processor type: " +
                            str(type(self.stream_processor)))

    def rollback(self, checkpoint_id_bytes):
        checkpoint_id_pb = remote_call_pb2.CheckpointId()
        checkpoint_id_pb.ParseFromString(checkpoint_id_bytes)
        checkpoint_id = checkpoint_id_pb.checkpoint_id

        logger.info("Start rollback, checkpoint_id={}".format(checkpoint_id))

        self.__rollback_cnt += 1
        if self.__rollback_cnt > 1:
            self.__is_recreate = True
        # skip useless rollback
        self.initial_state_lock.acquire()
        try:
            if self.task is not None and self.task.thread.is_alive()\
                    and checkpoint_id == self.task.last_checkpoint_id\
                    and self.task.is_initial_state:
                logger.info(
                    "Task is already in initial state, skip this rollback.")
                return self.__gen_call_result(
                    CallResult.skipped(
                        "Task is already in initial state, skip this rollback."
                    ))
        finally:
            self.initial_state_lock.release()

        # restart task
        try:
            if self.task is not None:
                # make sure the runner is closed
                self.task.cancel_task()
                del self.task

            self.task = self.create_stream_task(checkpoint_id)

            q_recover_info = self.task.recover(self.__is_recreate)

            self.__state.set_type(StateType.RUNNING)
            self.__need_rollback = False

            logger.info(
                "Rollback success, checkpoint is {}, qRecoverInfo is {}.".
                format(checkpoint_id, q_recover_info))

            return self.__gen_call_result(CallResult.success(q_recover_info))
        except Exception:
            logger.exception("Rollback has exception.")
            return self.__gen_call_result(CallResult.fail())

    def on_reader_message(self, *buffers):
        """Called by upstream queue writer to send data message to downstream
        queue reader.
        """
        if self.reader_client is None:
            logger.info("reader_client is None, skip writer transfer")
            return
        self.reader_client.on_reader_message(*buffers)

    def on_reader_message_sync(self, buffer: bytes):
        """Called by upstream queue writer to send
        control message to downstream downstream queue reader.
        """
        if self.reader_client is None:
            logger.info("task is None, skip reader transfer")
            return _NOT_READY_FLAG_
        result = self.reader_client.on_reader_message_sync(buffer)
        return result.to_pybytes()

    def on_writer_message(self, buffer: bytes):
        """Called by downstream queue reader to send notify message to
        upstream queue writer.
        """
        if self.writer_client is None:
            logger.info("writer_client is None, skip writer transfer")
            return
        self.writer_client.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        """Called by downstream queue reader to send control message to
        upstream queue writer.
        """
        if self.writer_client is None:
            return _NOT_READY_FLAG_
        result = self.writer_client.on_writer_message_sync(buffer)
        return result.to_pybytes()

    def shutdown_without_reconstruction(self):
        logger.info("Python worker shutdown without reconstruction.")
        ray.actor.exit_actor()

    def notify_checkpoint_timeout(self, checkpoint_id_bytes):
        pass

    def commit(self, barrier_bytes):
        barrier_pb = remote_call_pb2.Barrier()
        barrier_pb.ParseFromString(barrier_bytes)
        barrier = Barrier(barrier_pb.id)
        logger.info("Receive trigger, barrier is {}.".format(barrier))

        if self.task is not None:
            self.task.commit_trigger(barrier)
        ret = remote_call_pb2.BoolResult()
        ret.boolRes = True
        return ret.SerializeToString()

    def clear_expired_cp(self, state_checkpoint_id_bytes,
                         queue_checkpoint_id_bytes):
        state_checkpoint_id = self.__parse_to_checkpoint_id(
            state_checkpoint_id_bytes)
        queue_checkpoint_id = self.__parse_to_checkpoint_id(
            queue_checkpoint_id_bytes)
        logger.info("Start to clear expired checkpoint, checkpoint_id={},"
                    "queue_checkpoint_id={}, exe_vertex_name={}.".format(
                        state_checkpoint_id, queue_checkpoint_id,
                        self.execution_vertex_context.exe_vertex_name))

        ret = remote_call_pb2.BoolResult()
        ret.boolRes = self.__clear_expired_cp_state(state_checkpoint_id) \
            if state_checkpoint_id > 0 else True
        ret.boolRes &= self.__clear_expired_queue_msg(queue_checkpoint_id)
        logger.info(
            "Clear expired checkpoint done, result={}, checkpoint_id={},"
            "queue_checkpoint_id={}, exe_vertex_name={}.".format(
                ret.boolRes, state_checkpoint_id, queue_checkpoint_id,
                self.execution_vertex_context.exe_vertex_name))
        return ret.SerializeToString()

    def __clear_expired_cp_state(self, checkpoint_id):
        if self.__need_rollback:
            logger.warning("Need rollback, skip clear_expired_cp_state"
                           ", checkpoint id: {}".format(checkpoint_id))
            return False

        logger.info("Clear expired checkpoint state, cp id is {}.".format(
            checkpoint_id))

        if self.task is not None:
            self.task.clear_expired_cp_state(checkpoint_id)
        return True

    def __clear_expired_queue_msg(self, checkpoint_id):
        if self.__need_rollback:
            logger.warning("Need rollback, skip clear_expired_queue_msg"
                           ", checkpoint id: {}".format(checkpoint_id))
            return False

        logger.info("Clear expired queue msg, checkpoint_id is {}.".format(
            checkpoint_id))

        if self.task is not None:
            self.task.clear_expired_queue_msg(checkpoint_id)
        return True

    def __parse_to_checkpoint_id(self, checkpoint_id_bytes):
        checkpoint_id_pb = remote_call_pb2.CheckpointId()
        checkpoint_id_pb.ParseFromString(checkpoint_id_bytes)
        return checkpoint_id_pb.checkpoint_id

    def check_if_need_rollback(self):
        ret = remote_call_pb2.BoolResult()
        ret.boolRes = self.__need_rollback
        return ret.SerializeToString()

    def request_rollback(self, exception_msg="Python exception."):
        logger.info("Request rollback.")

        self.__need_rollback = True
        self.__is_recreate = True

        request_ret = False
        for i in range(Config.REQUEST_ROLLBACK_RETRY_TIMES):
            logger.info("request rollback {} time".format(i))
            try:
                request_ret = RemoteCallMst.request_job_worker_rollback(
                    self.master_actor,
                    WorkerRollbackRequest(
                        self.execution_vertex_context.actor_id.binary(),
                        "Exception msg=%s, retry time=%d." % (exception_msg,
                                                              i)))
            except Exception:
                logger.exception("Unexpected error when rollback")
            logger.info("request rollback {} time, ret={}".format(
                i, request_ret))
            if not request_ret:
                logger.warning(
                    "Request rollback return false"
                    ", maybe it's invalid request, try to sleep 1s.")
                time.sleep(1)
            else:
                break
        if not request_ret:
            logger.warning("Request failed after retry {} times,"
                           "now worker shutdown without reconstruction."
                           .format(Config.REQUEST_ROLLBACK_RETRY_TIMES))
            self.shutdown_without_reconstruction()

        self.__state.set_type(StateType.WAIT_ROLLBACK)

    def __gen_call_result(self, call_result):
        call_result_pb = remote_call_pb2.CallResult()

        call_result_pb.success = call_result.success
        call_result_pb.result_code = call_result.result_code.value
        if call_result.result_msg is not None:
            call_result_pb.result_msg = call_result.result_msg

        if call_result.result_obj is not None:
            q_recover_info = call_result.result_obj
            for q, status in q_recover_info.get_creation_status().items():
                call_result_pb.result_obj.creation_status[channel_bytes_to_str(
                    q)] = status.value

        return call_result_pb.SerializeToString()

    def _gen_unique_key(self, key_prefix):
        return key_prefix \
               + str(self.config.get(Config.STREAMING_JOB_NAME)) \
               + "_" + str(self.execution_vertex.execution_vertex_id)

    def __get_job_worker_context_key(self) -> str:
        return self._gen_unique_key(Config.JOB_WORKER_CONTEXT_KEY)


class WorkerState:
    """
    worker state
    """

    def __init__(self):
        self.__type = StateType.INIT

    def set_type(self, type):
        self.__type = type

    def get_type(self):
        return self.__type


class StateType(enum.Enum):
    """
    state type
    """

    INIT = 1
    RUNNING = 2
    WAIT_ROLLBACK = 3
