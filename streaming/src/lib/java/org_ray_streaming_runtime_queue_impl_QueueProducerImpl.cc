#include <memory>
#include "org_ray_streaming_runtime_queue_impl_QueueProducerImpl.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_runtime_queue_impl_QueueProducerImpl_writeMessageNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong address, jint size) {
  auto *writer_client = reinterpret_cast<StreamingWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  auto data = reinterpret_cast<uint8_t *>(address);
  auto data_size = static_cast<uint32_t>(size);
  jlong result = writer_client->WriteMessageToBufferRing(qid, data, data_size,
                                                         StreamingMessageType::Message);

  if (result == 0) {
    STREAMING_LOG(INFO) << "producer interrupted, return 0.";
    throwQueueInterruptException(env, "producer interrupted.");
  }
  return result;
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_queue_impl_QueueProducerImpl_stopProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  STREAMING_LOG(INFO) << "jni: stop producer.";
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  writer_client->Stop();
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_queue_impl_QueueProducerImpl_closeProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  delete writer_client;
}
