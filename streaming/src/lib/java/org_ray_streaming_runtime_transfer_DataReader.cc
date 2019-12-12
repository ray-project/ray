#include <cstdlib>
#include "streaming_jni_common.h"
#include "org_ray_streaming_runtime_transfer_DataReader.h"

using namespace ray;
using namespace ray::streaming;

JNIEXPORT jlong JNICALL Java_org_ray_streaming_runtime_transfer_DataReader_createDataReaderNative
  (JNIEnv *env, jclass cls, jlong core_worker_ptr, jobjectArray input_channels,
   jobjectArray input_actor_ids, jlongArray seq_id_array, jlongArray msg_id_array,
    jlong timer_interval, jboolean isRecreate, jbyteArray config_bytes,
    jboolean is_mock) {
  STREAMING_LOG(INFO) << "[JNI]: create DataReader.";
  std::vector<ray::ObjectID> input_channels_ids =
      jarray_to_object_id_vec(env, input_channels);
  std::vector<ray::ActorID> actor_ids =
    jarray_to_actor_id_vec(env, input_actor_ids);
  std::vector<uint64_t> queue_seq_ids =
      LongVectorFromJLongArray(env, seq_id_array).data;
  std::vector<uint64_t> msg_ids =
      LongVectorFromJLongArray(env, msg_id_array).data;

  auto ctx = std::make_shared<>(RuntimeContext);
  const jbyte *conf_bytes = env->GetByteArrayElements(config_bytes, 0);
  uint32_t conf_size = env->GetArrayLength(config_bytes);
  if (conf_size > 0) {
    STREAMING_LOG(INFO) << "load config, config bytes size: " << conf_size;
    ctx->SetConfig(conf_bytes, conf_size);
  }
  if (is_mock) {
    ctx->MarkMockTest();
  }
  auto reader = new DataReader(ctx);
  reader->Init(input_channels_ids, actor_ids, seq_ids, msg_ids, timer_interval);
  TREAMING_LOG(INFO) << "create native DataReader succeed";
  return reinterpret_cast<jlong>(reader);
}

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_transfer_DataReader_getBundleNative
    (JNIEnv *env, jobject, jlong reader_ptr, jlong timeout_millis, jlong out, jlong meta_addr) {
  std::shared_ptr<ray::streaming::DataBundle> bundle;
  auto reader = reinterpret_cast<ray::streaming::DataReader *>(reader_ptr);
  auto status = reader->GetBundle((uint32_t) timeout_millis, bundle);

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

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_transfer_DataReader_stopReaderNative
        (JNIEnv *env, jobject thisObj, jlong ptr) {
  auto reader = reinterpret_cast<DataReader *>(ptr);
  reader->Stop();
}

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_transfer_DataReader_closeReaderNative
    (JNIEnv *env, jobject thisObj, jlong ptr) {
  delete reinterpret_cast<DataReader *>(ptr);
}

