#include "io_ray_streaming_runtime_transfer_DataWriter.h"

#include "config/streaming_config.h"
#include "data_writer.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

JNIEXPORT jlong JNICALL
Java_io_ray_streaming_runtime_transfer_DataWriter_createWriterNative(
    JNIEnv *env, jclass, jobject initial_parameters, jobjectArray output_queue_ids,
    jlongArray msg_ids, jlong channel_size, jbyteArray conf_bytes_array,
    jboolean is_mock) {
  STREAMING_LOG(INFO) << "[JNI]: createDataWriterNative.";

  std::vector<ray::streaming::ChannelCreationParameter> parameter_vec;
  ParseChannelInitParameters(env, initial_parameters, parameter_vec);
  std::vector<ray::ObjectID> queue_id_vec =
      jarray_to_object_id_vec(env, output_queue_ids);
  for (auto id : queue_id_vec) {
    STREAMING_LOG(INFO) << "output channel id: " << id.Hex();
  }
  STREAMING_LOG(INFO) << "total channel size: " << channel_size << "*"
                      << queue_id_vec.size() << "=" << queue_id_vec.size() * channel_size;
  LongVectorFromJLongArray long_array_obj(env, msg_ids);
  std::vector<uint64_t> msg_ids_vec = LongVectorFromJLongArray(env, msg_ids).data;
  std::vector<uint64_t> queue_size_vec(long_array_obj.data.size(), channel_size);
  std::vector<ray::ObjectID> remain_id_vec;

  RawDataFromJByteArray conf(env, conf_bytes_array);
  STREAMING_CHECK(conf.data != nullptr);
  auto runtime_context = std::make_shared<RuntimeContext>();
  if (conf.data_size > 0) {
    runtime_context->SetConfig(conf.data, conf.data_size);
  }
  if (is_mock) {
    runtime_context->MarkMockTest();
  }
  auto *data_writer = new DataWriter(runtime_context);
  auto status =
      data_writer->Init(queue_id_vec, parameter_vec, msg_ids_vec, queue_size_vec);
  if (status != StreamingStatus::OK) {
    STREAMING_LOG(WARNING) << "DataWriter init failed.";
  } else {
    STREAMING_LOG(INFO) << "DataWriter init success";
  }

  data_writer->Run();
  return reinterpret_cast<jlong>(data_writer);
}

JNIEXPORT jlong JNICALL
Java_io_ray_streaming_runtime_transfer_DataWriter_writeMessageNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong address, jint size) {
  auto *data_writer = reinterpret_cast<DataWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  auto data = reinterpret_cast<uint8_t *>(address);
  auto data_size = static_cast<uint32_t>(size);
  jlong result = data_writer->WriteMessageToBufferRing(qid, data, data_size,
                                                       StreamingMessageType::Message);

  if (result == 0) {
    STREAMING_LOG(INFO) << "writer interrupted, return 0.";
    throwChannelInterruptException(env, "writer interrupted.");
  }
  return result;
}

JNIEXPORT void JNICALL Java_io_ray_streaming_runtime_transfer_DataWriter_stopWriterNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  STREAMING_LOG(INFO) << "jni: stop writer.";
  auto *data_writer = reinterpret_cast<DataWriter *>(ptr);
  data_writer->Stop();
}

JNIEXPORT void JNICALL
Java_io_ray_streaming_runtime_transfer_DataWriter_closeWriterNative(JNIEnv *env,
                                                                    jobject thisObj,
                                                                    jlong ptr) {
  auto *data_writer = reinterpret_cast<DataWriter *>(ptr);
  delete data_writer;
}