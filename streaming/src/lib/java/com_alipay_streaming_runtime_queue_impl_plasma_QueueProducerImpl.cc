#include <memory>

#include "com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl.h"
#include "streaming_jni_common.h"

using namespace ray::streaming;

JNIEXPORT jlongArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_getOutputSeqIdNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qIds) {
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  std::vector<ray::ObjectID> queue_vec = jarray_to_plasma_object_id_vec(env, qIds);

  std::vector<uint64_t> result;
  writer_client->GetChannelOffset(queue_vec, result);

  jlongArray jArray = env->NewLongArray(queue_vec.size());
  jlong jdata[queue_vec.size()];
  for (size_t i = 0; i < result.size(); ++i) {
    *(jdata + i) = result[i];
  }
  env->SetLongArrayRegion(jArray, 0, result.size(), jdata);
  return jArray;
}

JNIEXPORT jdoubleArray JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_getBackPressureRatioNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jobjectArray qIds) {
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  std::vector<ray::ObjectID> queue_vec = jarray_to_plasma_object_id_vec(env, qIds);

  std::vector<double> result;
  writer_client->GetChannelSetBackPressureRatio(queue_vec, result);

  jdoubleArray ratio_array = env->NewDoubleArray(queue_vec.size());
  jdouble jdata[queue_vec.size()];
  for (size_t i = 0; i < result.size(); ++i) {
    *(jdata + i) = result[i];
  }
  env->SetDoubleArrayRegion(ratio_array, 0, result.size(), jdata);
  return ratio_array;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_broadcastBarrierNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jlong checkpointId, jlong barrierId,
    jbyteArray bytes) {
  STREAMING_LOG(INFO) << "jni: broadcast barrier, cp_id=" << checkpointId
                      << ", barrier_id=" << barrierId;
  RawDataFromJByteArray raw_data(env, bytes);
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  writer_client->BroadcastBarrier(checkpointId, barrierId, raw_data.data,
                                  raw_data.data_size);
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_getBufferNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong min_size, jlong out) {
  auto *writer_client = reinterpret_cast<StreamingWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  auto buffer_pool = writer_client->GetBufferPool(qid);
  ray::streaming::Buffer buffer;
  auto status = buffer_pool->GetBufferBlocked(static_cast<uint64_t>(min_size), &buffer);
  if (status != StreamingStatus::OK) {
    std::stringstream ss;
    ss << "get buffer failed, status code: " << status;
    throwRuntimeException(env, ss.str().c_str());
  }
  *reinterpret_cast<uint64_t *>(out) = reinterpret_cast<uint64_t>(buffer.Data());
  *reinterpret_cast<uint32_t *>(out + 8) = static_cast<uint32_t>(buffer.Size());
}

JNIEXPORT jlong JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_writeMessageNative(
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
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_stopProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  STREAMING_LOG(INFO) << "jni: stop producer.";
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  writer_client->Stop();
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_closeProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  StreamingWriter *writer_client = reinterpret_cast<StreamingWriter *>(ptr);
  delete writer_client;
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_clearCheckpointNative(
    JNIEnv *env, jobject thisObj, jlong ptr, jlong checkpoint_id) {
  STREAMING_LOG(INFO) << "[Producer] jni: clearCheckpoints.";
  auto *writer = reinterpret_cast<StreamingWriter *>(ptr);
  writer->ClearCheckpoint(checkpoint_id);
  STREAMING_LOG(INFO) << "[Producer] clear checkpoint done.";
}

JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_hotUpdateProducerNative(
    JNIEnv *env, jobject obj, jlong ptr, jobjectArray id_array) {
  auto writer = reinterpret_cast<StreamingWriter *>(ptr);
  std::vector<ray::ObjectID> output_vect = jarray_to_plasma_object_id_vec(env, id_array);
  writer->Rescale(output_vect);
}

/*
 * Class:     com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl
 * Method:    broadcastPartialBarrierNative
 * Signature: (JJJ[B)V
 */
JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_broadcastPartialBarrierNative(
    JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id,
    jlong partial_barrier_id, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "[Producer] jni: broadcastPartialBarrier.";
  auto writer = reinterpret_cast<StreamingWriter *>(ptr);
  RawDataFromJByteArray raw_data(env, bytes);
  writer->BroadcastPartialBarrier(global_barrier_id, partial_barrier_id, raw_data.data,
                                  raw_data.data_size);
  STREAMING_LOG(INFO) << "[Producer] broadcastPartialBarrier done.";
}

/*
 * Class:     com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl
 * Method:    clearPartialCheckpointNative
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL
Java_com_alipay_streaming_runtime_queue_impl_plasma_QueueProducerImpl_clearPartialCheckpointNative(
    JNIEnv *env, jobject obj, jlong ptr, jlong global_barrier_id,
    jlong partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Producer] jni: clearPartialBarrier.";
  auto writer = reinterpret_cast<StreamingWriter *>(ptr);
  writer->ClearPartialCheckpoint(global_barrier_id, partial_barrier_id);
  STREAMING_LOG(INFO) << "[Producer] clearPartialBarrier done.";
}
