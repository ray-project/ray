# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from cpython cimport PyObject
cimport cpython

cdef inline object PyObject_to_object(PyObject* o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result

from ray.includes.common cimport (
    CLanguage,
    CRayObject,
    CRayStatus,
    CGcsClientOptions,
    CTaskType,
    CRayFunction
)

from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CTaskID,
    CObjectID,
)
from ray.includes.libcoreworker cimport CCoreWorker

cdef extern from "streaming.h" namespace "ray::streaming" nogil:
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

cdef extern from "streaming_message.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageType "ray::streaming::StreamingMessageType":
        pass
    cdef CStreamingMessageType MessageTypeBarrier "ray::streaming::StreamingMessageType::Barrier"
    cdef CStreamingMessageType MessageTypeMessage "ray::streaming::StreamingMessageType::Message"

cdef extern from "streaming_serializable.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingSerializable "ray::streaming::StreamingSerializable":
        void ToBytes(uint8_t *)
        uint32_t ClassBytesSize()

cdef extern from "streaming_message_bundle.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageBundleType "ray::streaming::StreamingMessageBundleType":
        pass
    cdef CStreamingMessageBundleType BundleTypeEmpty "ray::streaming::StreamingMessageBundleType::Empty"
    cdef CStreamingMessageBundleType BundleTypeBarrier "ray::streaming::StreamingMessageBundleType::Barrier"
    cdef CStreamingMessageBundleType BundleTypeBundle "ray::streaming::StreamingMessageBundleType::Bundle"

cdef extern from "streaming_reader.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingMessageBundleMeta "ray::streaming::StreamingMessageBundleMeta"(CStreamingSerializable):
        inline uint64_t GetMessageBundleTs() const
        inline uint64_t GetLastMessageId() const
        inline uint32_t GetMessageListSize() const
        inline CStreamingMessageBundleType GetBundleType() const
        inline c_bool IsBarrier()
        inline c_bool IsBundle()

    ctypedef shared_ptr[CStreamingMessageBundleMeta] CStreamingMessageBundleMetaPtr

    cdef cppclass CStreamingReaderBundle "ray::streaming::StreamingReaderBundle":
        uint8_t *data
        uint32_t data_size
        CObjectID c_from "from"
        uint64_t seq_id
        CStreamingMessageBundleMetaPtr meta

    cdef cppclass CStreamingReader "ray::streaming::StreamingReader"(CStreamingCommon):
        void Init(const c_vector[CObjectID] &input_ids,
                  const c_vector[uint64_t] &seq_ids,
                  const c_vector[uint64_t] &streaming_msg_ids,
                  int64_t timer_interval);
        CStreamingStatus GetBundle(const uint32_t timeout_ms,
                                   shared_ptr[CStreamingReaderBundle] &message)
        void Stop()

    cdef cppclass CStreamingReaderDirectCall "ray::streaming::StreamingReaderDirectCall"(CStreamingReader):
        CStreamingReaderDirectCall(CCoreWorker *core_worker,
                                   const c_vector[CObjectID] &queue_ids,
                                   const c_vector[CActorID] &actor_ids,
                                   CRayFunction async_func,
                                   CRayFunction sync_func)

cdef extern from "streaming_writer.h" namespace "ray::streaming" nogil:
    cdef cppclass CStreamingWriter "ray::streaming::StreamingWriter"(CStreamingCommon):
        long WriteMessageToBufferRing(
                const CObjectID &q_id, uint8_t *data, uint32_t data_size)
        CStreamingStatus Init(c_vector[CObjectID] &queue_ids,
                              c_vector[uint64_t] &message_ids,
                              const c_vector[uint64_t] &queue_size_vec);
        void Run()
        void Stop()

    cdef cppclass CStreamingWriterDirectCall "ray::streaming::StreamingWriterDirectCall"(CStreamingWriter):
        CStreamingWriterDirectCall(CCoreWorker *core_worker,
                                   const c_vector[CObjectID] &queue_ids,
                                   const c_vector[CActorID] &actor_ids,
                                   CRayFunction async_func,
                                   CRayFunction sync_func)

cdef extern from "ray/common/buffer.h" nogil:
    cdef cppclass CLocalMemoryBuffer "ray::LocalMemoryBuffer":
        uint8_t *Data() const
        size_t Size() const

cdef extern from "queue/queue_manager.h" namespace "ray::streaming" nogil:
    cdef cppclass CQueueManager "ray::streaming::QueueManager":
        @staticmethod
        shared_ptr[CQueueManager] GetInstance(const CActorID &actor_id)

cdef extern from "queue/streaming_queue_client.h" namespace "ray::streaming" nogil:
    cdef cppclass CQueueClient "ray::streaming::QueueClient":
        CQueueClient(shared_ptr[CQueueManager] manager)
        void OnMessage(shared_ptr[CLocalMemoryBuffer] buffer)
        shared_ptr[CLocalMemoryBuffer] OnMessageSync(shared_ptr[CLocalMemoryBuffer] buffer)


