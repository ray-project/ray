# flake8: noqa

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, make_shared, dynamic_pointer_cast
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.list cimport list as c_list
from libcpp.unordered_map cimport unordered_map as c_unordered_map
from cython.operator cimport dereference, postincrement

from ray.includes.common cimport (
    CRayFunction,
    LANGUAGE_PYTHON,
    LANGUAGE_JAVA,
    CBuffer
)

from ray.includes.unique_ids cimport (
    CActorID,
    CObjectID
)
from ray._raylet cimport (
    Buffer,
    ActorID,
    ObjectRef,
    FunctionDescriptor,
)

cimport ray.streaming.includes.libstreaming as libstreaming
from ray.streaming.includes.libstreaming cimport (
    CStreamingStatus,
    CStreamingMessage,
    CStreamingMessageBundle,
    CRuntimeContext,
    CDataBundle,
    CDataWriter,
    CDataReader,
    CReaderClient,
    CWriterClient,
    CLocalMemoryBuffer,
    CChannelCreationParameter,
    CTransferCreationStatus,
    CConsumerChannelInfo,
    CStreamingBarrierHeader,
    kBarrierHeaderSize,
)
from ray._raylet import JavaFunctionDescriptor

import logging


channel_logger = logging.getLogger(__name__)

cdef class ChannelCreationParameter:
    cdef:
        CChannelCreationParameter parameter

    def __cinit__(self, ActorID actor_id, FunctionDescriptor async_func, FunctionDescriptor sync_func):
        cdef:
            shared_ptr[CRayFunction] async_func_ptr
            shared_ptr[CRayFunction] sync_func_ptr
        self.parameter = CChannelCreationParameter()
        self.parameter.actor_id = (<ActorID>actor_id).data
        if isinstance(async_func, JavaFunctionDescriptor):
            self.parameter.async_function = make_shared[CRayFunction](LANGUAGE_JAVA, async_func.descriptor)
        else:
            self.parameter.async_function = make_shared[CRayFunction](LANGUAGE_PYTHON, async_func.descriptor)
        if isinstance(sync_func, JavaFunctionDescriptor):
            self.parameter.sync_function = make_shared[CRayFunction](LANGUAGE_JAVA, sync_func.descriptor)
        else:
            self.parameter.sync_function = make_shared[CRayFunction](LANGUAGE_PYTHON, sync_func.descriptor)

    cdef CChannelCreationParameter get_parameter(self):
        return self.parameter

cdef class ReaderClient:
    cdef:
        CReaderClient *client

    def __cinit__(self):
        self.client = new CReaderClient()

    def __dealloc__(self):
        del self.client
        self.client = NULL

    def on_reader_message(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
        with nogil:
            self.client.OnReaderMessage(local_buf)

    def on_reader_message_sync(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
            shared_ptr[CLocalMemoryBuffer] result_buffer
        with nogil:
            result_buffer = self.client.OnReaderMessageSync(local_buf)
        return Buffer.make(dynamic_pointer_cast[CBuffer, CLocalMemoryBuffer](result_buffer))


cdef class WriterClient:
    cdef:
        CWriterClient * client

    def __cinit__(self):
        self.client = new CWriterClient()

    def __dealloc__(self):
        del self.client
        self.client = NULL

    def on_writer_message(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
        with nogil:
            self.client.OnWriterMessage(local_buf)

    def on_writer_message_sync(self, const unsigned char[:] value):
        cdef:
            size_t size = value.nbytes
            shared_ptr[CLocalMemoryBuffer] local_buf = \
                make_shared[CLocalMemoryBuffer](<uint8_t *>(&value[0]), size, True)
            shared_ptr[CLocalMemoryBuffer] result_buffer
        with nogil:
            result_buffer = self.client.OnWriterMessageSync(local_buf)
        return Buffer.make(dynamic_pointer_cast[CBuffer, CLocalMemoryBuffer](result_buffer))


cdef class DataWriter:
    cdef:
        CDataWriter *writer

    def __init__(self):
        raise Exception("use create() to create DataWriter")

    @staticmethod
    def create(list py_output_channels,
               list output_creation_parameters: list[ChannelCreationParameter],
               uint64_t queue_size,
               list py_msg_ids,
               bytes config_bytes,
               c_bool is_mock):
        cdef:
            c_vector[CObjectID] channel_ids = bytes_list_to_qid_vec(py_output_channels)
            c_vector[CChannelCreationParameter] initial_parameters
            c_vector[uint64_t] msg_ids
            CDataWriter *c_writer
            ChannelCreationParameter parameter
            cdef const unsigned char[:] config_data
        for param in output_creation_parameters:
            parameter = param
            initial_parameters.push_back(parameter.get_parameter())
        for py_msg_id in py_msg_ids:
            msg_ids.push_back(<uint64_t>py_msg_id)

        cdef shared_ptr[CRuntimeContext] ctx = make_shared[CRuntimeContext]()
        if is_mock:
            ctx.get().MarkMockTest()
        if config_bytes:
            config_data = config_bytes
            channel_logger.info("DataWriter load config, config bytes size: %s", config_data.nbytes)
            ctx.get().SetConfig(<uint8_t *>(&config_data[0]), config_data.nbytes)
        c_writer = new CDataWriter(ctx)
        cdef:
            c_vector[CObjectID] remain_id_vec
            c_vector[uint64_t] queue_size_vec
        for i in range(channel_ids.size()):
            queue_size_vec.push_back(queue_size)
        cdef CStreamingStatus status = c_writer.Init(channel_ids, initial_parameters, msg_ids, queue_size_vec)
        if remain_id_vec.size() != 0:
            channel_logger.warning("failed queue amounts => %s", remain_id_vec.size())
        if <uint32_t>status != <uint32_t> libstreaming.StatusOK:
            msg = "initialize writer failed, status={}".format(<uint32_t>status)
            channel_logger.error(msg)
            del c_writer
            import ray.streaming.runtime.transfer as transfer
            raise transfer.ChannelInitException(msg, qid_vector_to_list(remain_id_vec))

        c_writer.Run()
        channel_logger.info("create native writer succeed")
        cdef DataWriter writer = DataWriter.__new__(DataWriter)
        writer.writer = c_writer
        return writer

    def __dealloc__(self):
        if self.writer != NULL:
            del self.writer
            channel_logger.info("deleted DataWriter")
            self.writer = NULL

    def write(self, ObjectRef qid, const unsigned char[:] value):
        """support zero-copy bytes, byte array, array of unsigned char"""
        cdef:
            CObjectID native_id = qid.data
            uint64_t msg_id
            uint8_t *data = <uint8_t *>(&value[0])
            uint32_t size = value.nbytes
        with nogil:
            msg_id = self.writer.WriteMessageToBufferRing(native_id, data, size)
        return msg_id

    def broadcast_barrier(self, uint64_t checkpoint_id, const unsigned char[:] value):
        cdef:
            uint8_t *data = <uint8_t *>(&value[0])
            uint32_t size = value.nbytes
        with nogil:
            self.writer.BroadcastBarrier(checkpoint_id, data, size)

    def get_output_checkpoints(self):
        cdef:
            c_vector[uint64_t] results
        self.writer.GetChannelOffset(results)
        return results

    def clear_checkpoint(self, checkpoint_id):
        cdef:
            uint64_t c_checkpoint_id = checkpoint_id
        with nogil:
            self.writer.ClearCheckpoint(c_checkpoint_id)

    def stop(self):
        self.writer.Stop()
        channel_logger.info("stopped DataWriter")


cdef class DataReader:
    cdef:
        CDataReader *reader
        readonly bytes meta
        readonly bytes data

    def __init__(self):
        raise Exception("use create() to create DataReader")

    @staticmethod
    def create(list py_input_queues,
               list input_creation_parameters: list[ChannelCreationParameter],
               list py_msg_ids,
               int64_t timer_interval,
               bytes config_bytes,
               c_bool is_mock):
        cdef:
            c_vector[CObjectID] queue_id_vec = bytes_list_to_qid_vec(py_input_queues)
            c_vector[CChannelCreationParameter] initial_parameters
            c_vector[uint64_t] msg_ids
            c_vector[CTransferCreationStatus] c_creation_status
            CDataReader *c_reader
            ChannelCreationParameter parameter
            cdef const unsigned char[:] config_data
        for param in input_creation_parameters:
            parameter = param
            initial_parameters.push_back(parameter.get_parameter())

        for py_msg_id in py_msg_ids:
            msg_ids.push_back(<uint64_t>py_msg_id)
        cdef shared_ptr[CRuntimeContext] ctx = make_shared[CRuntimeContext]()
        if config_bytes:
            config_data = config_bytes
            channel_logger.info("DataReader load config, config bytes size: %s", config_data.nbytes)
            ctx.get().SetConfig(<uint8_t *>(&(config_data[0])), config_data.nbytes)
        if is_mock:
            ctx.get().MarkMockTest()
        c_reader = new CDataReader(ctx)
        c_reader.Init(queue_id_vec, initial_parameters, msg_ids, c_creation_status, timer_interval)

        creation_status_map = {}
        if not c_creation_status.empty():
            for i in range(queue_id_vec.size()):
                k = queue_id_vec[i].Binary()
                v = <uint64_t>c_creation_status[i]
                creation_status_map[k] = v

        channel_logger.info("create native reader succeed")
        cdef DataReader reader = DataReader.__new__(DataReader)
        reader.reader = c_reader
        return reader, creation_status_map

    def __dealloc__(self):
        if self.reader != NULL:
            del self.reader
            channel_logger.info("deleted DataReader")
            self.reader = NULL

    def read(self, uint32_t timeout_millis):
        cdef:
            shared_ptr[CDataBundle] bundle
            CStreamingStatus status
        with nogil:
            status = self.reader.GetBundle(timeout_millis, bundle)
        if <uint32_t> status != <uint32_t> libstreaming.StatusOK:
            if <uint32_t> status == <uint32_t> libstreaming.StatusInterrupted:
                # avoid cyclic import
                import ray.streaming.runtime.transfer as transfer
                raise transfer.ChannelInterruptException("reader interrupted")
            elif <uint32_t> status == <uint32_t> libstreaming.StatusInitQueueFailed:
                import ray.streaming.runtime.transfer as transfer
                raise transfer.ChannelInitException("init channel failed")
            elif <uint32_t> status == <uint32_t> libstreaming.StatusGetBundleTimeOut:
                return []
            else:
                raise Exception("no such status " + str(<uint32_t>status))
        cdef:
            uint32_t msg_nums
            CObjectID queue_id = bundle.get().c_from
            c_list[shared_ptr[CStreamingMessage]] msg_list
            list msgs = []
            uint64_t timestamp
            uint64_t msg_id
            c_unordered_map[CObjectID, CConsumerChannelInfo] *offset_map = NULL
            shared_ptr[CStreamingMessage] barrier
            CStreamingBarrierHeader barrier_header
            c_unordered_map[CObjectID, CConsumerChannelInfo].iterator it

        cdef uint32_t bundle_type = <uint32_t>(bundle.get().meta.get().GetBundleType())
        # avoid cyclic import
        from ray.streaming.runtime.transfer import DataMessage
        if bundle_type == <uint32_t> libstreaming.BundleTypeBundle:
            msg_nums = bundle.get().meta.get().GetMessageListSize()
            CStreamingMessageBundle.GetMessageListFromRawData(
                bundle.get().data + libstreaming.kMessageBundleHeaderSize,
                bundle.get().data_size - libstreaming.kMessageBundleHeaderSize,
                msg_nums,
                msg_list)
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            for msg in msg_list:
                msg_bytes = msg.get().Payload()[:msg.get().PayloadSize()]
                qid_bytes = queue_id.Binary()
                msg_id = msg.get().GetMessageId()
                msgs.append(
                    DataMessage(msg_bytes, timestamp, msg_id, qid_bytes))
            return msgs
        elif bundle_type == <uint32_t> libstreaming.BundleTypeEmpty:
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            msg_id = bundle.get().meta.get().GetLastMessageId()
            return [DataMessage(None, timestamp, msg_id, queue_id.Binary(), True)]
        elif bundle.get().meta.get().IsBarrier():
            py_offset_map = {}
            self.reader.GetOffsetInfo(offset_map)
            it = offset_map.begin()
            while it != offset_map.end():
                queue_id_bytes = dereference(it).first.Binary()
                current_message_id = dereference(it).second.current_message_id
                py_offset_map[queue_id_bytes] = current_message_id
                postincrement(it)
            msg_nums = bundle.get().meta.get().GetMessageListSize()
            CStreamingMessageBundle.GetMessageListFromRawData(
                bundle.get().data + libstreaming.kMessageBundleHeaderSize,
                bundle.get().data_size - libstreaming.kMessageBundleHeaderSize,
                msg_nums,
                msg_list)
            timestamp = bundle.get().meta.get().GetMessageBundleTs()
            barrier = msg_list.front()
            msg_id = barrier.get().GetMessageId()
            CStreamingMessage.GetBarrierIdFromRawData(barrier.get().Payload(), &barrier_header)
            barrier_id = barrier_header.barrier_id
            barrier_data = (barrier.get().Payload() + kBarrierHeaderSize)[
                           :barrier.get().PayloadSize() - kBarrierHeaderSize]
            barrier_type = <uint64_t> barrier_header.barrier_type
            py_queue_id = queue_id.Binary()
            from ray.streaming.runtime.transfer import CheckpointBarrier
            return [CheckpointBarrier(
                barrier_data, timestamp, msg_id, py_queue_id, py_offset_map,
                barrier_id, barrier_type)]
        else:
            raise Exception("Unsupported bundle type {}".format(bundle_type))


    def stop(self):
        self.reader.Stop()
        channel_logger.info("stopped DataReader")


cdef c_vector[CObjectID] bytes_list_to_qid_vec(list py_queue_ids) except *:
    assert len(py_queue_ids) > 0
    cdef:
        c_vector[CObjectID] queue_id_vec
        c_string q_id_data
    for q_id in py_queue_ids:
        q_id_data = q_id
        assert q_id_data.size() == CObjectID.Size(), f"{q_id_data.size()}, {CObjectID.Size()}"
        obj_id = CObjectID.FromBinary(q_id_data)
        queue_id_vec.push_back(obj_id)
    return queue_id_vec

cdef c_vector[c_string] qid_vector_to_list(c_vector[CObjectID] queue_id_vec):
    queues = []
    for obj_id in queue_id_vec:
        queues.append(obj_id.Binary())
    return queues
