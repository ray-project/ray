#pragma once

#include <jni.h>

#include <string>

#include "channel/channel.h"
#include "ray/core_worker/common.h"
#include "util/streaming_logging.h"

class UniqueIdFromJByteArray {
 private:
  JNIEnv *_env;
  jbyteArray _bytes;
  jbyte *b;

 public:
  ray::ObjectID PID;

  UniqueIdFromJByteArray(JNIEnv *env, jbyteArray wid) {
    _env = env;
    _bytes = wid;

    b = reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    PID = ray::ObjectID::FromBinary(
        std::string(reinterpret_cast<const char *>(b), ray::ObjectID::Size()));
  }

  ~UniqueIdFromJByteArray() { _env->ReleaseByteArrayElements(_bytes, b, 0); }
};

class RawDataFromJByteArray {
 private:
  JNIEnv *_env;
  jbyteArray _bytes;

 public:
  uint8_t *data;
  uint32_t data_size;

  RawDataFromJByteArray(JNIEnv *env, jbyteArray bytes) {
    _env = env;
    _bytes = bytes;
    data_size = _env->GetArrayLength(_bytes);
    jbyte *b = reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    data = reinterpret_cast<uint8_t *>(b);
  }

  ~RawDataFromJByteArray() {
    _env->ReleaseByteArrayElements(_bytes, reinterpret_cast<jbyte *>(data), 0);
  }
};

class StringFromJString {
 private:
  JNIEnv *_env;
  const char *j_str;
  jstring jni_str;

 public:
  std::string str;

  StringFromJString(JNIEnv *env, jstring jni_str_) {
    jni_str = jni_str_;
    _env = env;
    j_str = env->GetStringUTFChars(jni_str, nullptr);
    str = std::string(j_str);
  }

  ~StringFromJString() { _env->ReleaseStringUTFChars(jni_str, j_str); }
};

class LongVectorFromJLongArray {
 private:
  JNIEnv *_env;
  jlongArray long_array;
  jlong *long_array_ptr = nullptr;

 public:
  std::vector<uint64_t> data;

  LongVectorFromJLongArray(JNIEnv *env, jlongArray long_array_) {
    _env = env;
    long_array = long_array_;

    long_array_ptr = env->GetLongArrayElements(long_array, nullptr);
    jsize seq_id_size = env->GetArrayLength(long_array);
    data = std::vector<uint64_t>(long_array_ptr, long_array_ptr + seq_id_size);
  }

  ~LongVectorFromJLongArray() {
    _env->ReleaseLongArrayElements(long_array, long_array_ptr, 0);
  }
};

std::vector<ray::ObjectID> jarray_to_object_id_vec(JNIEnv *env, jobjectArray jarr);
std::vector<ray::ActorID> jarray_to_actor_id_vec(JNIEnv *env, jobjectArray jarr);

jint throwRuntimeException(JNIEnv *env, const char *message);
jint throwChannelInitException(JNIEnv *env, const char *message,
                               const std::vector<ray::ObjectID> &abnormal_queues);
jint throwChannelInterruptException(JNIEnv *env, const char *message);
std::shared_ptr<ray::RayFunction> FunctionDescriptorToRayFunction(
    JNIEnv *env, jobject functionDescriptor);
void ParseChannelInitParameters(
    JNIEnv *env, jobject param_obj,
    std::vector<ray::streaming::ChannelCreationParameter> &parameter_vec);
