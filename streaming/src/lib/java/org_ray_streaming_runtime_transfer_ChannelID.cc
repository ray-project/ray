#include "org_ray_streaming_runtime_transfer_ChannelID.h"
#include "streaming_jni_common.h"
using namespace ray::streaming;

JNIEXPORT jlong JNICALL Java_org_ray_streaming_runtime_transfer_ChannelID_createNativeID(
    JNIEnv *env, jclass cls, jlong qid_address) {
  auto id = ray::ObjectID::FromBinary(
      std::string(reinterpret_cast<const char *>(qid_address), ray::ObjectID::Size()));
  return reinterpret_cast<jlong>(new ray::ObjectID(id));
}

JNIEXPORT void JNICALL Java_org_ray_streaming_runtime_transfer_ChannelID_destroyNativeID(
    JNIEnv *env, jclass cls, jlong native_id_ptr) {
  auto id = reinterpret_cast<ray::ObjectID *>(native_id_ptr);
  STREAMING_CHECK(id != nullptr);
  delete id;
}
