#include "com_alipay_streaming_runtime_queue_impl_plasma_QueueLinkImpl.h"

#include <cstdlib>
#include <sstream>
#include <string>

#include "ray/raylet/raylet_client.h"

#include "streaming_reader.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueLinkImpl_newConsumer(
    JNIEnv *env, jobject this_obj,
    jobjectArray input_queue_id_array,  // byte[][]
    jlongArray plasma_seq_id_array, jlongArray streaming_msg_id_array,
    jstring store_path_str, jlong timer_interval, jboolean is_recreate,
    jbyteArray fbs_conf_byte_array) {
  STREAMING_LOG(INFO) << "[JNI]: newConsumer.";
  std::string store_path = StringFromJString(env, store_path_str).str;
  std::vector<ray::ObjectID> input_queue_ids =
      jarray_to_plasma_object_id_vec(env, input_queue_id_array);
  std::vector<uint64_t> plasma_queue_seq_ids =
      LongVectorFromJLongArray(env, plasma_seq_id_array).data;
  std::vector<uint64_t> streaming_msg_ids =
      LongVectorFromJLongArray(env, streaming_msg_id_array).data;

  std::vector<ray::ObjectID> abnormal_queues;
  auto *streaming_reader = new StreamingReader();
  const jbyte *fbs_conf_bytes = env->GetByteArrayElements(fbs_conf_byte_array, 0);
  uint32_t fbs_len = env->GetArrayLength(fbs_conf_byte_array);
  if (fbs_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from flatbuffer, fbs len => " << fbs_len;
    streaming_reader->SetConfig(reinterpret_cast<const uint8_t *>(fbs_conf_bytes),
                                fbs_len);
  }
  streaming_reader->Init(store_path, input_queue_ids, plasma_queue_seq_ids,
                         streaming_msg_ids, timer_interval, is_recreate, abnormal_queues);
  if (abnormal_queues.size()) {
    STREAMING_LOG(INFO) << "[JNI]: QueueInitException thrown.";
    throwQueueInitException(env, "Consumer init failed.", abnormal_queues);
    delete streaming_reader;
    return -1;
  }
  return reinterpret_cast<jlong>(streaming_reader);
}

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueLinkImpl_newProducer(
    JNIEnv *env, jobject this_obj,
    jobjectArray output_queue_ids,  // byte[][]
    jlongArray seq_ids, jstring store_path, jlong queue_size, jlongArray creator_type,
    jbyteArray fsb_conf_byte_array) {
  STREAMING_LOG(INFO) << "[JNI]: newProducer.";
  auto *streaming_writer = new StreamingWriter();
  const jbyte *fbs_conf_bytes = env->GetByteArrayElements(fsb_conf_byte_array, 0);
  uint32_t fbs_len = env->GetArrayLength(fsb_conf_byte_array);
  if (fbs_conf_bytes) {
    STREAMING_LOG(INFO) << "jni : load config from flatbuffer, fbs len => " << fbs_len;
    streaming_writer->SetConfig(reinterpret_cast<const uint8_t *>(fbs_conf_bytes),
                                fbs_len);
  }

  std::vector<ray::ObjectID> queue_id_vec =
      jarray_to_plasma_object_id_vec(env, output_queue_ids);
  for (auto qid : queue_id_vec) {
    STREAMING_LOG(INFO) << "output qid: " << qid.Hex();
  }
  STREAMING_LOG(INFO) << "total queue size: " << queue_size << "*" << queue_id_vec.size()
                      << "=" << queue_id_vec.size() * queue_size;
  LongVectorFromJLongArray long_array_obj(env, seq_ids);

  StringFromJString str_from_j(env, store_path);

  std::vector<uint64_t> queue_size_vec(long_array_obj.data.size(), queue_size);

  std::vector<ray::ObjectID> remain_id_vec;

  LongVectorFromJLongArray create_types_vec(env, creator_type);
  std::vector<StreamingQueueCreationType> creator_vec;
  for (auto &it : create_types_vec.data) {
    creator_vec.push_back(static_cast<StreamingQueueCreationType>(it));
  }
  StreamingStatus status =
      streaming_writer->Init(queue_id_vec, str_from_j.str, long_array_obj.data,
                             queue_size_vec, remain_id_vec, creator_vec);
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
