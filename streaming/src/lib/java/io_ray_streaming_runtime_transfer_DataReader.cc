#include "io_ray_streaming_runtime_transfer_DataReader.h"

#include <cstdlib>

#include "data_reader.h"
#include "runtime_context.h"
#include "streaming_jni_common.h"

using namespace ray;
using namespace ray::streaming;

JNIEXPORT jlong JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_createDataReaderNative(
    JNIEnv *env, jclass, jobject streaming_queue_initial_parameters,
    jobjectArray input_channels, jlongArray msg_id_array, jlong timer_interval,
    jobject creation_status, jbyteArray config_bytes, jboolean is_mock) {
  STREAMING_LOG(INFO) << "[JNI]: create DataReader.";
  std::vector<ray::streaming::ChannelCreationParameter> parameter_vec;
  ParseChannelInitParameters(env, streaming_queue_initial_parameters, parameter_vec);
  std::vector<ray::ObjectID> input_channels_ids =
      jarray_to_object_id_vec(env, input_channels);
  std::vector<uint64_t> msg_ids = LongVectorFromJLongArray(env, msg_id_array).data;

  auto ctx = std::make_shared<RuntimeContext>();
  RawDataFromJByteArray conf(env, config_bytes);
  if (conf.data_size > 0) {
    STREAMING_LOG(INFO) << "load config, config bytes size: " << conf.data_size;
    ctx->SetConfig(conf.data, conf.data_size);
  }
  if (is_mock) {
    ctx->MarkMockTest();
  }

  // init reader
  auto reader = new DataReader(ctx);
  std::vector<TransferCreationStatus> creation_status_vec;
  reader->Init(input_channels_ids, parameter_vec, msg_ids, creation_status_vec,
               timer_interval);

  // add creation status to Java's List
  jclass array_list_cls = env->GetObjectClass(creation_status);
  jclass integer_cls = env->FindClass("java/lang/Integer");
  jmethodID array_list_add =
      env->GetMethodID(array_list_cls, "add", "(Ljava/lang/Object;)Z");
  for (auto &status : creation_status_vec) {
    jmethodID integer_init = env->GetMethodID(integer_cls, "<init>", "(I)V");
    jobject integer_obj =
        env->NewObject(integer_cls, integer_init, static_cast<int>(status));
    env->CallBooleanMethod(creation_status, array_list_add, integer_obj);
  }
  STREAMING_LOG(INFO) << "create native DataReader succeed";
  return reinterpret_cast<jlong>(reader);
}

JNIEXPORT void JNICALL Java_io_ray_streaming_runtime_transfer_DataReader_getBundleNative(
    JNIEnv *env, jobject, jlong reader_ptr, jlong timeout_millis, jlong out,
    jlong meta_addr) {
  std::shared_ptr<ray::streaming::DataBundle> bundle;
  auto reader = reinterpret_cast<ray::streaming::DataReader *>(reader_ptr);
  auto status = reader->GetBundle((uint32_t)timeout_millis, bundle);

  // over timeout, return empty array.
  if (StreamingStatus::Interrupted == status) {
    throwChannelInterruptException(env, "reader interrupted.");
  } else if (StreamingStatus::GetBundleTimeOut == status) {
  } else if (StreamingStatus::InitQueueFailed == status) {
    throwRuntimeException(env, "init channel failed");
  }

  if (StreamingStatus::OK != status) {
    *reinterpret_cast<uint64_t *>(out) = 0;
    *reinterpret_cast<uint32_t *>(out + 8) = 0;
    return;
  }

  // bundle data
  // In streaming queue, bundle data and metadata will be different args of direct call,
  // so we separate it here for future extensibility.
  *reinterpret_cast<uint64_t *>(out) =
      reinterpret_cast<uint64_t>(bundle->data + kMessageBundleHeaderSize);
  *reinterpret_cast<uint32_t *>(out + 8) = bundle->data_size - kMessageBundleHeaderSize;

  // bundle metadata
  auto meta = reinterpret_cast<uint8_t *>(meta_addr);
  // bundle header written by writer
  std::memcpy(meta, bundle->data, kMessageBundleHeaderSize);
  // append qid
  std::memcpy(meta + kMessageBundleHeaderSize, bundle->from.Data(), kUniqueIDSize);
}

JNIEXPORT void JNICALL Java_io_ray_streaming_runtime_transfer_DataReader_stopReaderNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  auto reader = reinterpret_cast<DataReader *>(ptr);
  reader->Stop();
}

JNIEXPORT void JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_closeReaderNative(JNIEnv *env,
                                                                    jobject thisObj,
                                                                    jlong ptr) {
  delete reinterpret_cast<DataReader *>(ptr);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_getOffsetsInfoNative(JNIEnv *env,
                                                                       jobject thisObj,
                                                                       jlong ptr) {
  auto reader = reinterpret_cast<ray::streaming::DataReader *>(ptr);
  std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map = nullptr;
  reader->GetOffsetInfo(offset_map);
  STREAMING_CHECK(offset_map);
  // queue nums + (queue id + seq id + message id) * queue nums
  int offset_data_size =
      sizeof(uint32_t) + (kUniqueIDSize + sizeof(uint64_t) * 2) * offset_map->size();
  jbyteArray offsets_info = env->NewByteArray(offset_data_size);
  int offset = 0;
  // total queue nums
  auto queue_nums = static_cast<uint32_t>(offset_map->size());
  env->SetByteArrayRegion(offsets_info, offset, sizeof(uint32_t),
                          reinterpret_cast<jbyte *>(&queue_nums));
  offset += sizeof(uint32_t);
  // queue name & offset
  for (auto &p : *offset_map) {
    env->SetByteArrayRegion(offsets_info, offset, kUniqueIDSize,
                            reinterpret_cast<const jbyte *>(p.first.Data()));
    offset += kUniqueIDSize;
    // msg_id
    env->SetByteArrayRegion(offsets_info, offset, sizeof(uint64_t),
                            reinterpret_cast<jbyte *>(&p.second.current_message_id));
    offset += sizeof(uint64_t);
  }
  return offsets_info;
}