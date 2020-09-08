# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# flake8: noqa

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector as c_vector
from libcpp.list cimport list as c_list
from cpython cimport PyObject
cimport cpython
from libcpp.unordered_map cimport unordered_map as c_unordered_map
from cython.operator cimport dereference, postincrement


cdef inline object PyObject_to_object(PyObject* o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result

from ray.includes.common cimport (
    CLanguage,
    CRayObject,
    CRayStatus,
    CRayFunction
)

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CTaskID,
    CObjectID,
)

cdef extern from "common/status.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingStatus "ray::streaming::StreamingStatus":
        pass
    cdef CStreamingStatus StatusOK "ray::streaming::StreamingStatus::OK"
    cdef CStreamingStatus StatusReconstructTimeOut "ray::streaming::StreamingStatus::ReconstructTimeOut"
    cdef CStreamingStatus StatusQueueIdNotFound "ray::streaming::StreamingStatus::QueueIdNotFound"
    cdef CStreamingStatus StatusResubscribeFailed "ray::streaming::StreamingStatus::ResubscribeFailed"
    cdef CStreamingStatus StatusEmptyRingBuffer "ray::streaming::StreamingStatus::EmptyRingBuffer"
    cdef CStreamingStatus StatusFullChannel "ray::streaming::StreamingStatus::FullChannel"
    cdef CStreamingStatus StatusNoSuchItem "ray::streaming::StreamingStatus::NoSuchItem"
    cdef CStreamingStatus StatusInitQueueFailed "ray::streaming::StreamingStatus::InitQueueFailed"
    cdef CStreamingStatus StatusGetBundleTimeOut "ray::streaming::StreamingStatus::GetBundleTimeOut"
    cdef CStreamingStatus StatusSkipSendEmptyMessage "ray::streaming::StreamingStatus::SkipSendEmptyMessage"
    cdef CStreamingStatus StatusInterrupted "ray::streaming::StreamingStatus::Interrupted"
    cdef CStreamingStatus StatusWaitQueueTimeOut "ray::streaming::StreamingStatus::WaitQueueTimeOut"
    cdef CStreamingStatus StatusOutOfMemory "ray::streaming::StreamingStatus::OutOfMemory"
    cdef CStreamingStatus StatusInvalid "ray::streaming::StreamingStatus::Invalid"
    cdef CStreamingStatus StatusUnknownError "ray::streaming::StreamingStatus::UnknownError"
    cdef CStreamingStatus StatusTailStatus "ray::streaming::StreamingStatus::TailStatus"

    cdef cppclass CStreamingCommon "ray::streaming::StreamingCommon":
        void SetConfig(const uint8_t *, uint32_t size)


cdef extern from "runtime_context.h" namespace "ray::streaming" nogil:
    cdef cppclass CRuntimeContext "ray::streaming::RuntimeContext":
        CRuntimeContext()
        void SetConfig(const uint8_t *data, uint32_t size)
        inline void MarkMockTest()
        inline c_bool IsMockTest()

cdef extern from "message/message.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageType "ray::streaming::StreamingMessageType":
        pass
    cdef CStreamingMessageType MessageTypeBarrier "ray::streaming::StreamingMessageType::Barrier"
    cdef CStreamingMessageType MessageTypeMessage "ray::streaming::StreamingMessageType::Message"
    cdef cppclass CStreamingMessage "ray::streaming::StreamingMessage":
        inline uint8_t *RawData() const
        inline uint8_t *Payload() const
        inline uint32_t PayloadSize() const
        inline uint32_t GetDataSize() const
        inline CStreamingMessageType GetMessageType() const
        inline uint64_t GetMessageId() const
        @staticmethod
        inline void GetBarrierIdFromRawData(const uint8_t *data,
                                            CStreamingBarrierHeader *barrier_header)
    cdef struct CStreamingBarrierHeader "ray::streaming::StreamingBarrierHeader":
        CStreamingBarrierType barrier_type;
        uint64_t barrier_id;
    cdef cppclass CStreamingBarrierType "ray::streaming::StreamingBarrierType":
        pass
    cdef uint32_t kMessageHeaderSize;
    cdef uint32_t kBarrierHeaderSize;

cdef extern from "message/message_bundle.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageBundleType "ray::streaming::StreamingMessageBundleType":
        pass
    cdef CStreamingMessageBundleType BundleTypeEmpty "ray::streaming::StreamingMessageBundleType::Empty"
    cdef CStreamingMessageBundleType BundleTypeBarrier "ray::streaming::StreamingMessageBundleType::Barrier"
    cdef CStreamingMessageBundleType BundleTypeBundle "ray::streaming::StreamingMessageBundleType::Bundle"

    cdef cppclass CStreamingMessageBundleMeta "ray::streaming::StreamingMessageBundleMeta":
        CStreamingMessageBundleMeta()
        inline uint64_t GetMessageBundleTs() const
        inline uint64_t GetLastMessageId() const
        inline uint32_t GetMessageListSize() const
        inline CStreamingMessageBundleType GetBundleType() const
        inline c_bool IsBarrier()
        inline c_bool IsBundle()

    ctypedef shared_ptr[CStreamingMessageBundleMeta] CStreamingMessageBundleMetaPtr
    uint32_t kMessageBundleHeaderSize "ray::streaming::kMessageBundleHeaderSize"
    cdef cppclass CStreamingMessageBundle "ray::streaming::StreamingMessageBundle"(CStreamingMessageBundleMeta):
         @staticmethod
         void GetMessageListFromRawData(const uint8_t *data, uint32_t size, uint32_t msg_nums,
                                        c_list[shared_ptr[CStreamingMessage]] &msg_list);

cdef extern from "channel/channel.h" namespace "ray::streaming" nogil:
    cdef struct CChannelCreationParameter "ray::streaming::ChannelCreationParameter":
        CChannelCreationParameter()
        CActorID actor_id;
        shared_ptr[CRayFunction] async_function;
        shared_ptr[CRayFunction] sync_function;

    cdef struct CStreamingQueueInfo "ray::streaming::StreamingQueueInfo":
        uint64_t first_seq_id;
        uint64_t last_message_id;
        uint64_t target_message_id;
        uint64_t consumed_message_id;

    cdef struct CConsumerChannelInfo "ray::streaming::ConsumerChannelInfo":
        CObjectID channel_id;
        uint64_t current_message_id;
        uint64_t barrier_id;
        uint64_t partial_barrier_id;
        CStreamingQueueInfo queue_info;
        uint64_t last_queue_item_delay;
        uint64_t last_queue_item_latency;
        uint64_t last_queue_target_diff;
        uint64_t get_queue_item_times;
        uint64_t notify_cnt;
        CChannelCreationParameter parameter;

    cdef enum CTransferCreationStatus "ray::streaming::TransferCreationStatus":
        FreshStarted = 0
        PullOk = 1
        Timeout = 2
        DataLost = 3
        Invalid = 999


cdef extern from "queue/queue_client.h" namespace "ray::streaming" nogil:
    cdef cppclass CReaderClient "ray::streaming::ReaderClient":
        CReaderClient()
        void OnReaderMessage(shared_ptr[CLocalMemoryBuffer] buffer);
        shared_ptr[CLocalMemoryBuffer] OnReaderMessageSync(shared_ptr[CLocalMemoryBuffer] buffer);

    cdef cppclass CWriterClient "ray::streaming::WriterClient":
        CWriterClient()
        void OnWriterMessage(shared_ptr[CLocalMemoryBuffer] buffer);
        shared_ptr[CLocalMemoryBuffer] OnWriterMessageSync(shared_ptr[CLocalMemoryBuffer] buffer);


cdef extern from "data_reader.h" namespace "ray::streaming" nogil:
    cdef cppclass CDataBundle "ray::streaming::DataBundle":
        uint8_t *data
        uint32_t data_size
        CObjectID c_from "from"
        uint64_t seq_id
        CStreamingMessageBundleMetaPtr meta

    cdef cppclass CDataReader "ray::streaming::DataReader"(CStreamingCommon):
        CDataReader(shared_ptr[CRuntimeContext] &runtime_context)
        void Init(const c_vector[CObjectID] &input_ids,
                  const c_vector[CChannelCreationParameter] &params,
                  const c_vector[uint64_t] &msg_ids,
                  c_vector[CTransferCreationStatus] &creation_status,
                  int64_t timer_interval);
        CStreamingStatus GetBundle(const uint32_t timeout_ms,
                                   shared_ptr[CDataBundle] &message)
        void GetOffsetInfo(c_unordered_map[CObjectID, CConsumerChannelInfo] *&offset_map);
        void Stop()


cdef extern from "data_writer.h" namespace "ray::streaming" nogil:
    cdef cppclass CDataWriter "ray::streaming::DataWriter"(CStreamingCommon):
        CDataWriter(shared_ptr[CRuntimeContext] &runtime_context)
        CStreamingStatus Init(const c_vector[CObjectID] &channel_ids,
                              const c_vector[CChannelCreationParameter] &params,
                              const c_vector[uint64_t] &message_ids,
                              const c_vector[uint64_t] &queue_size_vec);
        long WriteMessageToBufferRing(
                const CObjectID &q_id, uint8_t *data, uint32_t data_size)
        void BroadcastBarrier(uint64_t checkpoint_id, const uint8_t *data, uint32_t data_size)
        void GetChannelOffset(c_vector[uint64_t] &result)
        void ClearCheckpoint(uint64_t checkpoint_id)
        void Run()
        void Stop()


cdef extern from "ray/common/buffer.h" nogil:
    cdef cppclass CLocalMemoryBuffer "ray::LocalMemoryBuffer":
        uint8_t *Data() const
        size_t Size() const
