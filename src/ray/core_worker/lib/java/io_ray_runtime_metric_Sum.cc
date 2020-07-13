#include "ray/core_worker/lib/java/io_ray_runtime_metric_Sum.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/stats/metric.h"

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_Sum_registerSumNative(
    JNIEnv *env, jobject obj, jstring j_name, jstring j_description, jstring j_unit,
    jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env, j_name, j_description, j_unit, tag_key_list, &metric_name,
                  &description, &unit, tag_keys);
  auto *sum = new ray::stats::Sum(metric_name, description, unit, tag_keys);
  return reinterpret_cast<long>(sum);
}
#ifdef __cplusplus
}
#endif
