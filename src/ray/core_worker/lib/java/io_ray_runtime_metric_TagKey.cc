#include "ray/core_worker/lib/java/io_ray_runtime_metric_TagKey.h"
#include "ray/core_worker/lib/java/jni_utils.h"

#include <jni.h>

#include "opencensus/tags/tag_key.h"

using TagKeyType = opencensus::tags::TagKey;

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT void JNICALL Java_io_ray_runtime_metric_TagKey_registerTagkeyNative(
    JNIEnv *env, jobject obj, jstring str) {
  std::string tag_key_name = JavaStringToNativeString(env, static_cast<jstring>(str));
  RAY_IGNORE_EXPR(TagKeyType::Register(tag_key_name));
}
#ifdef __cplusplus
}
#endif
