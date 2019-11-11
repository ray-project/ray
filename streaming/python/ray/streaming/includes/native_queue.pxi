from libc.stdint cimport *
from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, make_shared
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from cython.operator import dereference, postincrement

from ray.includes.common cimport (
    CLanguage,
    CRayObject,
    CRayStatus,
    CGcsClientOptions,
    CTaskArg,
    CTaskType,
    CRayFunction,
    LANGUAGE_CPP,
    LANGUAGE_JAVA,
    LANGUAGE_PYTHON,
    LocalMemoryBuffer,
    TASK_TYPE_NORMAL_TASK,
    TASK_TYPE_ACTOR_CREATION_TASK,
    TASK_TYPE_ACTOR_TASK,
    WORKER_TYPE_WORKER,
    WORKER_TYPE_DRIVER,
)

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CTaskID,
    CObjectID,
    ObjectID
)

from ray.includes.libcoreworker cimport CCoreWorker, CoreWorker

from ray.function_manager import FunctionDescriptor

import ray.streaming.includes.libstreaming as libstreaming
from ray.streaming.includes.libstreaming cimport (
    CStreamingStatus,
    CStreamingMessageType,
    CStreamingSerializable,
    CStreamingMessageBundleType,
    CStreamingMessageBundleMeta,
    CStreamingMessageBundleMetaPtr,
    CStreamingReaderBundle,
    CStreamingWriter,
    CStreamingWriterDirectCall,
    CStreamingReader,
    CStreamingReaderDirectCall,
    CQueueManager,
    CQueueClient,
    CLocalMemoryBuffer,
    StatusOK
)

from ray.streaming.queue.exception import QueueInitException

logger = logging.getLogger(__name__)

cdef class QueueLink:
    cdef:
        CCoreWorker *core_worker
        shared_ptr[CQueueManager] queue_manager
        CQueueClient *queue_client
        QueueProducer producer
        QueueConsumer consumer

    def __cinit__(self, CoreWorker worker):
        cdef:
            CCoreWorker *core_worker = worker.core_worker.get()
            CActorID actor_id = core_worker.GetActorId()
            shared_ptr[CQueueManager] queue_manager = CQueueManager.GetInstance(actor_id)
            CQueueClient *queue_client = new CQueueClient(queue_manager)
        self.core_worker = core_worker
        self.queue_manager = queue_manager
        self.queue_client = queue_client
        self.producer = None
        self.consumer = None

    def __dealloc__(self):
        del self.queue_client
        self.queue_client = NULL

    def create_producer(self,
                        list[bytes] py_output_queues,
                        list[uint64_t] output_actor_ids,
                        uint64_t queue_size,
                        list[int] py_seq_ids,
                        bytes config_bytes,
                        async_func: FunctionDescriptor,
                        sync_func: FunctionDescriptor):
        if self.producer:
            return self.producer
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_output_queues)
            c_vector[CActorID] actor_ids
            c_vector[uint64_t] seq_ids
            CRayFunction async_native_func
            CRayFunction sync_native_func
            CStreamingWriter *writer
            cdef const unsigned char[:] config_data
            uint64_t ptr
        for actor_id in output_actor_ids:
            ptr = actor_id
            actor_ids.push_back(dereference(<CActorID *>ptr))
        for py_seq_id in py_seq_ids:
            seq_ids.push_back(<uint64_t>py_seq_id)
        async_native_func = CRayFunction(
            LANGUAGE_PYTHON, string_vector_from_list(async_func.get_function_descriptor_list()))
        sync_native_func = CRayFunction(
            LANGUAGE_PYTHON, string_vector_from_list(sync_func.get_function_descriptor_list()))
        writer = new CStreamingWriterDirectCall(self.core_worker, queue_id_vec, actor_ids,
                                                async_native_func, sync_native_func)
        if config_bytes:
            config_data = config_bytes
            logger.info("load config, config bytes size: %s", config_data.nbytes)
            writer.SetConfig(<uint8_t *>(&config_data[0]), config_data.nbytes)
        cdef:
            c_vector[CObjectID] remain_id_vec
            c_vector[uint64_t] queue_size_vec
        for i in range(queue_id_vec.size()):
            queue_size_vec.push_back(queue_size)
        cdef CStreamingStatus status = writer.Init(queue_id_vec, seq_ids, queue_size_vec)
        if remain_id_vec.size() != 0:
            logger.warning("failed queue amounts => %s", remain_id_vec.size())
        if <uint32_t>status != <uint32_t>StatusOK:
            msg = "initialize writer failed, status={}".format(<uint32_t>status)
            del writer
            raise QueueInitException(msg, qid_vector_to_list(remain_id_vec))
        logger.info("init producer ok, status => %s", <uint32_t>status)
        writer.Run()
        self.producer = QueueProducer()
        self.producer.writer = writer
        return self.producer

    def create_consumer(self,
                        list[bytes] py_input_queues,
                        list[uint64_t] input_actor_ids,
                        list[int] py_seq_ids,
                        list[int] py_msg_ids,
                        uint64_t timer_interval,
                        c_bool is_recreate,
                        bytes config_bytes,
                        async_func: FunctionDescriptor,
                        sync_func: FunctionDescriptor):
        if self.consumer:
            return self.consumer
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_input_queues)
            c_vector[CActorID] actor_ids
            c_vector[uint64_t] seq_ids
            c_vector[uint64_t] msg_ids
            CRayFunction async_native_func
            CRayFunction sync_native_func
            CStreamingWriter *writer
            cdef const unsigned char[:] config_data
            uint64_t ptr
        for actor_id in input_actor_ids:
            ptr = actor_id
            actor_ids.push_back(dereference(<CActorID *>ptr))
        for py_seq_id in py_seq_ids:
            seq_ids.push_back(<uint64_t>py_seq_id)
        for py_msg_id in py_msg_ids:
            msg_ids.push_back(<uint64_t>py_msg_id)
        async_native_func = CRayFunction(
            LANGUAGE_PYTHON, string_vector_from_list(async_func.get_function_descriptor_list()))
        sync_native_func = CRayFunction(
            LANGUAGE_PYTHON, string_vector_from_list(sync_func.get_function_descriptor_list()))
        reader = new CStreamingReaderDirectCall(self.core_worker, queue_id_vec, actor_ids,
                                                async_native_func, sync_native_func)
        if config_bytes:
            config_data = config_bytes
            logger.info("load config, config bytes size: %s", config_data.nbytes)
            reader.SetConfig(<uint8_t *>(&(config_data[0])), config_data.nbytes)
        reader.Init(queue_id_vec, seq_ids, msg_ids, timer_interval)
        self.consumer = QueueConsumer()
        self.consumer.reader = reader
        return self.consumer

    def on_streaming_transfer(self, const unsigned char[:] value):
        # support bytes, bytearray, array of unsigned char
        cdef:
            uint32_t length = value.nbytes
            shared_ptr[CLocalMemoryBuffer] buffer =\
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), length)
        self.queue_client.OnMessage(buffer)

    def on_streaming_transfer_sync(self, const unsigned char[:] value):
        cdef:
            uint32_t length = value.nbytes
            shared_ptr[CLocalMemoryBuffer] buffer =\
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), length)
            shared_ptr[CLocalMemoryBuffer] result_buffer = self.queue_client.OnMessageSync(buffer)
            uint8_t* result_data = result_buffer.get().Data()
            int32_t result_data_size = result_buffer.get().Size()
        return result_data[:result_data_size]

cdef class QueueProducer:
    cdef:
        CStreamingWriter *writer

    def __cinit__(self):
        pass

    def __dealloc__(self):
        if self.writer != NULL:
            del self.writer
            self.writer = NULL

    def produce(self, ObjectID qid, const unsigned char[:] value):
        """support zero-copy bytes, bytearray, array of unsigned char"""
        cdef CObjectID native_id = qid.data
        self.writer.WriteMessageToBufferRing(native_id, <uint8_t *>(&value[0]), value.nbytes)

    def stop(self):
        self.writer.Stop()

cdef class QueueConsumer:
    cdef:
        CStreamingReader *reader

    def __init__(self):
        pass

    def __dealloc__(self):
        if self.reader != NULL:
            del self.reader
            self.reader = NULL

    def pull(self):
        pass

    def stop(self):
        self.writer.Stop()

cdef c_vector[c_string] string_vector_from_list(list string_list):
    cdef:
        c_vector[c_string] out
    for s in string_list:
        if not isinstance(s, bytes):
            raise TypeError("string_list elements must be bytes")
        out.push_back(s)
    return out

cdef c_vector[CObjectID] bytes_list_to_qid_vec(list[bytes] py_queue_ids):
    cdef:
        c_vector[CObjectID] queue_id_vec
        c_string q_id_data
    for q_id in py_queue_ids:
        q_id_data = q_id
        assert q_id_data.size() == CObjectID.Size()
        obj_id = CObjectID.FromBinary(q_id_data)
        queue_id_vec.push_back(obj_id)
    return queue_id_vec

cdef c_vector[c_string] qid_vector_to_list(c_vector[CObjectID] queue_id_vec):
    queues = []
    for obj_id in queue_id_vec:
        queues.append(obj_id.Binary())
    return queues