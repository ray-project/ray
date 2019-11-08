#include <cstdlib>
#include "org_ray_streaming_queue_impl_QueueConsumerImpl.h"
#include "streaming_writer.h"
#include "streaming_jni_common.h"
#include "streaming_reader.h"

using namespace ray::streaming;
using namespace ray;

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_queue_impl_QueueConsumerImpl_getBundleNative
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
  std::memcpy(meta + kMessageBundleHeaderSize, msg->from.Data(), kUniqueIDSize);
}

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_queue_impl_QueueConsumerImpl_stopConsumerNative
        (JNIEnv *env, jobject thisObj, jlong ptr) {
  auto reader = reinterpret_cast<StreamingReader *>(ptr);
  reader->Stop();
}

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_queue_impl_QueueConsumerImpl_closeConsumerNative
    (JNIEnv *env, jobject thisObj, jlong ptr) {
  delete reinterpret_cast<StreamingReader *>(ptr);
}

