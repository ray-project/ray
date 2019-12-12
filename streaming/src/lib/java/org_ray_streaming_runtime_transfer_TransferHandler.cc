#include "org_ray_streaming_runtime_transfer_TransferHandler.h"
#include "queue/queue_client.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

static std::shared_ptr<ray::LocalMemoryBuffer> JByteArrayToBuffer(JNIEnv *env,
                                                                  jbyteArray bytes) {
  jbyte *buffer_bytes = env->GetByteArrayElements(bytes, 0);
  uint32_t buffer_len = env->GetArrayLength(bytes);
  STREAMING_CHECK(buffer_bytes != nullptr);

  return std::make_shared<ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(buffer_bytes), buffer_len);
}

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_createWriterClientNative(
    JNIEnv *env, jobject this_obj, jlong core_worker_ptr, jobject async_func,
    jobject sync_func) {
  ray::RayFunction ray_async_func = FunctionDescriptorToRayFunction(env, async_func);
  ray::RayFunction ray_sync_func = FunctionDescriptorToRayFunction(env, sync_func);
  auto *writer_client =
      new WriterClient(reinterpret_cast<ray::CoreWorker *>(core_worker_ptr),
                       ray_async_func, ray_sync_func);
  return reinterpret_cast<jlong>(writer_client);
}

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_createReaderClientNative(
    JNIEnv *env, jobject this_obj, jlong core_worker_ptr, jobject async_func,
    jobject sync_func) {
  ray::RayFunction ray_async_func = FunctionDescriptorToRayFunction(env, async_func);
  ray::RayFunction ray_sync_func = FunctionDescriptorToRayFunction(env, sync_func);
  auto *reader_client =
      new ReaderClient(reinterpret_cast<ray::CoreWorker *>(core_worker_ptr),
                       ray_async_func, ray_sync_func);
  return reinterpret_cast<jlong>(reader_client);
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleWriterMessageNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_TransferHandler_"
                         "handleWriterMessageNative";
  WriterClient *writer_client = reinterpret_cast<WriterClient *>(ptr);

  writer_client->OnWriterMessage(JByteArrayToBuffer(env, bytes));
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleWriterMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_TransferHandler_"
                         "handleWriterMessageSyncNative";
  WriterClient *writer_client = reinterpret_cast<WriterClient *>(ptr);

  std::shared_ptr<ray::LocalMemoryBuffer> result_buffer =
      writer_client->OnWriterMessageSync(JByteArrayToBuffer(env, bytes));

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(),
                          reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleReaderMessageNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_TransferHandler_"
                         "handleReaderMessageNative";
  ReaderClient *reader_client = reinterpret_cast<ReaderClient *>(ptr);

  reader_client->OnReaderMessage(JByteArrayToBuffer(env, bytes));
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleReaderMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_TransferHandler_"
                         "handleReaderMessageSyncNative";
  ReaderClient *reader_client = reinterpret_cast<ReaderClient *>(ptr);

  std::shared_ptr<ray::LocalMemoryBuffer> result_buffer =
      reader_client->OnReaderMessageSync(JByteArrayToBuffer(env, bytes));

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(),
                          reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}