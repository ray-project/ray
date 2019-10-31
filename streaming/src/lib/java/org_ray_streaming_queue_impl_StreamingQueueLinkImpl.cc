#include <cstdlib>
#include <sstream>
#include <string>

#include "org_ray_streaming_queue_impl_StreamingQueueLinkImpl.h"
#include "streaming_reader.h"
#include "streaming_jni_common.h"
#include "queue/streaming_queue_client.h"

using namespace ray::streaming;

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_queue_impl_StreamingQueueLinkImpl_newConsumer(
    JNIEnv *env, jobject this_obj,
    jlong core_worker, jlongArray actor_handles_array,
    jobject async_func, jobject sync_func,
    jobjectArray input_queue_id_array,  // byte[][]
    jlongArray seq_id_array, jlongArray streaming_msg_id_array,
    jlong timer_interval, jboolean is_recreate,
    jbyteArray fbs_conf_byte_array) {
  STREAMING_LOG(INFO) << "[JNI]: newConsumer.";
  std::vector<ray::ObjectID> input_queue_ids =
      jarray_to_object_id_vec(env, input_queue_id_array);
  std::vector<uint64_t> queue_seq_ids =
      LongVectorFromJLongArray(env, seq_id_array).data;
  std::vector<uint64_t> streaming_msg_ids =
      LongVectorFromJLongArray(env, streaming_msg_id_array).data;
  std::vector<uint64_t> actor_handles = 
      LongVectorFromJLongArray(env, actor_handles_array).data;

  std::vector<ray::ActorID> actor_ids;
  for (auto &handle : actor_handles) {
    ray::ActorHandle *handle_ptr = reinterpret_cast<ray::ActorHandle*>(handle);
    actor_ids.push_back(handle_ptr->GetActorID());
  }
  // std::vector<ray::ObjectID> abnormal_queues;
  auto *streaming_reader = new StreamingReaderDirectCall(
      reinterpret_cast<ray::CoreWorker *>(core_worker), 
      input_queue_ids, actor_ids,
      FunctionDescriptorToRayFunction(env, async_func),
      FunctionDescriptorToRayFunction(env, sync_func));
  const jbyte *fbs_conf_bytes = env->GetByteArrayElements(fbs_conf_byte_array, 0);
  uint32_t fbs_len = env->GetArrayLength(fbs_conf_byte_array);
  if (fbs_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from flatbuffer, fbs len => " << fbs_len;
    streaming_reader->SetConfig(reinterpret_cast<const uint8_t *>(fbs_conf_bytes),
                                fbs_len);
  }
  streaming_reader->Init(input_queue_ids, queue_seq_ids,
                         streaming_msg_ids, timer_interval);

  return reinterpret_cast<jlong>(streaming_reader);
}

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_queue_impl_StreamingQueueLinkImpl_newProducer(
    JNIEnv *env, jobject this_obj,
    jlong core_worker, jlongArray actor_handles,
    jobject async_func, jobject sync_func,
    jobjectArray output_queue_ids,  // byte[][]
    jlongArray seq_ids, jlong queue_size, jlongArray creator_type,
    jbyteArray fsb_conf_byte_array) {
  STREAMING_LOG(INFO) << "[JNI]: newProducer.";

  std::vector<ray::ObjectID> queue_id_vec =
      jarray_to_object_id_vec(env, output_queue_ids);
  for (auto qid : queue_id_vec) {
    STREAMING_LOG(INFO) << "output qid: " << qid.Hex();
  }
  STREAMING_LOG(INFO) << "total queue size: " << queue_size << "*" << queue_id_vec.size()
                      << "=" << queue_id_vec.size() * queue_size;
  LongVectorFromJLongArray long_array_obj(env, seq_ids);

  std::vector<uint64_t> queue_size_vec(long_array_obj.data.size(), queue_size);

  std::vector<ray::ObjectID> remain_id_vec;

  LongVectorFromJLongArray create_types_vec(env, creator_type);
  std::vector<uint64_t> actor_handle_vec = LongVectorFromJLongArray(env, actor_handles).data;

  std::vector<ray::ActorID> actor_ids;
  for (auto &handle : actor_handle_vec) {
    ray::ActorHandle *handle_ptr = reinterpret_cast<ray::ActorHandle*>(handle);
    actor_ids.push_back(handle_ptr->GetActorID());
  }

  auto *streaming_writer = new StreamingWriterDirectCall(
      reinterpret_cast<ray::CoreWorker *>(core_worker), 
      queue_id_vec, actor_ids,
      FunctionDescriptorToRayFunction(env, async_func),
      FunctionDescriptorToRayFunction(env, sync_func));
  const jbyte *fbs_conf_bytes = env->GetByteArrayElements(fsb_conf_byte_array, 0);
  uint32_t fbs_len = env->GetArrayLength(fsb_conf_byte_array);
  if (fbs_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from flatbuffer, fbs len => " << fbs_len;
    streaming_writer->SetConfig(reinterpret_cast<const uint8_t *>(fbs_conf_bytes),
                                fbs_len);
  }
  StreamingStatus status =
      streaming_writer->Init(queue_id_vec, long_array_obj.data,
                             queue_size_vec);
  if (!remain_id_vec.empty()) {
    STREAMING_LOG(WARNING) << "create remaining queue size => " << remain_id_vec.size();
  }
  // throw exception if status is not ok
  if (status != StreamingStatus::OK) {
    STREAMING_LOG(ERROR) << "initialize writer failed, status="
                         << static_cast<uint32_t>(status);
    std::stringstream error_msg;
    error_msg << "initialize writer failed, status=" << static_cast<uint32_t>(status);
    throwQueueInitException(env, const_cast<char *>(error_msg.str().c_str()),
                            remain_id_vec);
    delete streaming_writer;
    return -1;
  }
  STREAMING_LOG(INFO) << "init producer ok, status =>" << static_cast<uint32_t>(status);
  streaming_writer->Run();

  return reinterpret_cast<jlong>(streaming_writer);
}


JNIEXPORT jlong JNICALL 
Java_org_ray_streaming_queue_impl_StreamingQueueLinkImpl_newMessageHandler(
    JNIEnv *env, jobject this_obj, jlong core_worker) {
  ray::CoreWorker* core_worker_ptr = reinterpret_cast<ray::CoreWorker*>(core_worker);
  STREAMING_CHECK(nullptr != core_worker_ptr);
  ActorID actor_id = core_worker_ptr->GetWorkerContext().GetCurrentActorID();
  std::shared_ptr<ray::streaming::QueueManager> queue_manager = 
    ray::streaming::QueueManager::GetInstance(actor_id);
  QueueClient* client = new QueueClient(queue_manager);
  return reinterpret_cast<jlong>(client);
}

JNIEXPORT void JNICALL 
Java_org_ray_streaming_queue_impl_StreamingQueueLinkImpl_onQueueTransfer(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_StreamingQueueLinkImpl_onQueueTransfer";
  QueueClient* client = reinterpret_cast<QueueClient*>(ptr);

  jbyte* buffer_bytes = env->GetByteArrayElements(bytes, 0);
  uint32_t buffer_len = env->GetArrayLength(bytes);
  if (!buffer_bytes) {
    STREAMING_LOG(ERROR) << "buffer_bytes null!";
    return;
  }
  
  std::shared_ptr<ray::LocalMemoryBuffer> buffer = 
    std::make_shared<ray::LocalMemoryBuffer>(reinterpret_cast<uint8_t*>(buffer_bytes), buffer_len);
  client->OnMessage(buffer);
}

JNIEXPORT jbyteArray JNICALL 
Java_org_ray_streaming_queue_impl_StreamingQueueLinkImpl_onQueueTransferSync(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_com_alipay_streaming_runtime_queue_impl_streamingqueue_StreamingQueueLinkImpl_onQueueTransferSync";
  QueueClient* client = reinterpret_cast<QueueClient*>(ptr);

  jbyte* buffer_bytes = env->GetByteArrayElements(bytes, 0);
  uint32_t buffer_len = env->GetArrayLength(bytes);
  if (!buffer_bytes) {
    STREAMING_LOG(ERROR) << "buffer_bytes null!";
    return env->NewByteArray(0);
  }
  
  std::shared_ptr<ray::LocalMemoryBuffer> buffer = 
    std::make_shared<ray::LocalMemoryBuffer>(reinterpret_cast<uint8_t*>(buffer_bytes), buffer_len);
  std::shared_ptr<ray::LocalMemoryBuffer> result_buffer = client->OnMessageSync(buffer);

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(), 
      reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}