#include "com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl.h"
#include <cstdlib>
#include "streaming_writer.h"
#include "streaming_jni_common.h"
#include "streaming_reader.h"

using namespace ray::streaming;
using namespace ray;

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_clearCheckpointNative
    (JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qids, jlongArray offsets) {
  STREAMING_LOG(INFO) << "jni: clearCheckpoints.";
  STREAMING_LOG(INFO) << "clear checkpoint done.";
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_getBundleNative
    (JNIEnv *env, jobject, jlong ptr, jlong timeoutMillis, jlong out, jlong meta_addr) {
  std::shared_ptr<ray::streaming::StreamingReaderBundle> msg;
  auto reader = reinterpret_cast<ray::streaming::StreamingReader *>(ptr);
  auto status = reader->GetBundle((uint32_t) timeoutMillis, msg);

  // over timeout, return empty array.
  if (StreamingStatus::Interrupted == status) {
    throwQueueInterruptException(env, "consumer interrupted.");
  } else if (StreamingStatus::GetBundleTimeOut == status) {
  } else if (StreamingStatus::InitQueueFailed == status){
    throwRuntimeException(env, "init queue failed");
  } else if (StreamingStatus::WaitQueueTimeOut == status) {
    throwRuntimeException(env, "wait queue object timeout");
  }

  if (StreamingStatus::OK != status) {
    *reinterpret_cast<uint64_t *>(out) = 0;
    *reinterpret_cast<uint32_t *>(out + 8) = 0;
    return;
  }

  // bundle data
  // In streaming queue, bundle data and metadata will be different args of direct call, so we separate it
  // here for future extensibility.
  *reinterpret_cast<uint64_t *>(out) =
      reinterpret_cast<uint64_t>(msg->data + kMessageBundleHeaderSize);
  *reinterpret_cast<uint32_t *>(out + 8) = msg->data_size - kMessageBundleHeaderSize;

  // bundle metadata
  auto meta = reinterpret_cast<uint8_t *>(meta_addr);
  // bundle header written by writer
  std::memcpy(meta, msg->data, kMessageBundleHeaderSize);
  // append qid
  std::memcpy(meta + kMessageBundleHeaderSize, msg->from.Data(), plasma::kUniqueIDSize);
}

JNIEXPORT jbyteArray JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_getOffsetsInfo
  (JNIEnv *env, jobject, jlong ptr) {
  auto reader = reinterpret_cast<ray::streaming::StreamingReader *>(ptr);
  std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map = nullptr;
  reader->GetOffsetInfo(offset_map);
  STREAMING_CHECK(offset_map);
  // queue nums + (plasma queue id + seq id + message id) * queue nums
  int offset_data_size = sizeof(uint32_t) + (plasma::kUniqueIDSize + sizeof(uint64_t) * 2) * offset_map->size();
  jbyteArray offsets_info = env->NewByteArray(offset_data_size);
  int offset = 0;
  // total queue nums
  auto queue_nums = static_cast<uint32_t>(offset_map->size());
  env->SetByteArrayRegion(offsets_info, offset, sizeof(uint32_t), reinterpret_cast<jbyte *>(&queue_nums));
  offset += sizeof(uint32_t);
  // queue name & offset
  for (auto& p : *offset_map) {
    env->SetByteArrayRegion(offsets_info, offset, plasma::kUniqueIDSize,
        reinterpret_cast<const jbyte*>(p.first.Data()));
    offset += plasma::kUniqueIDSize;
    // seq_id
    env->SetByteArrayRegion(offsets_info, offset, sizeof(uint64_t),
        reinterpret_cast<jbyte *>(&p.second.current_seq_id));
    offset += sizeof(uint64_t);
    // msg_id
    env->SetByteArrayRegion(offsets_info, offset, sizeof(uint64_t),
        reinterpret_cast<jbyte *>(&p.second.current_message_id));
    offset += sizeof(uint64_t);
  }
  return offsets_info;
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_stopConsumerNative
        (JNIEnv *env, jobject thisObj, jlong ptr) {
  auto reader = reinterpret_cast<StreamingReader *>(ptr);
  reader->Stop();
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_closeConsumerNative
    (JNIEnv *env, jobject thisObj, jlong ptr) {
  delete reinterpret_cast<StreamingReader *>(ptr);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_hotUpdateConsumerNative
  (JNIEnv *env, jobject obj, jlong ptr, jobjectArray id_array) {
  auto reader = reinterpret_cast<StreamingReader *>(ptr);
  std::vector<ray::ObjectID> input_vec = jarray_to_plasma_object_id_vec(env, id_array);
  reader->Rescale(input_vec);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueConsumerImpl_clearPartialCheckpointNative
  (JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id, jlong partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Consumer] jni: clearPartialCheckpoint.";
  auto reader = reinterpret_cast<StreamingReader *>(ptr);
  reader->ClearPartialCheckpoint(global_barrier_id, partial_barrier_id);
  STREAMING_LOG(INFO) << "[Consumer] clearPartialCheckpoint done.";
}
