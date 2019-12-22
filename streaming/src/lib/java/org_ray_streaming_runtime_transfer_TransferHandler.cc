#include "org_ray_streaming_runtime_transfer_TransferHandler.h"
#include "queue/queue_client.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

static std::shared_ptr<ray::LocalMemoryBuffer> JByteArrayToBuffer(JNIEnv *env,
                                                                  jbyteArray bytes) {
  RawDataFromJByteArray buf(env, bytes);
  STREAMING_CHECK(buf.data != nullptr);

  return std::make_shared<ray::LocalMemoryBuffer>(buf.data, buf.data_size, true);
}

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_createWriterClientNative(
    JNIEnv *env, jobject this_obj, jlong core_worker_ptr, jobject async_func,
    jobject sync_func) {
  auto ray_async_func = FunctionDescriptorToRayFunction(env, async_func);
  auto ray_sync_func = FunctionDescriptorToRayFunction(env, sync_func);
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
  auto *writer_client = reinterpret_cast<WriterClient *>(ptr);
  writer_client->OnWriterMessage(JByteArrayToBuffer(env, bytes));
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleWriterMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  auto *writer_client = reinterpret_cast<WriterClient *>(ptr);
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
  auto *reader_client = reinterpret_cast<ReaderClient *>(ptr);
  reader_client->OnReaderMessage(JByteArrayToBuffer(env, bytes));
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_streaming_runtime_transfer_TransferHandler_handleReaderMessageSyncNative(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  auto *reader_client = reinterpret_cast<ReaderClient *>(ptr);
  auto result_buffer = reader_client->OnReaderMessageSync(JByteArrayToBuffer(env, bytes));

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(),
                          reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}