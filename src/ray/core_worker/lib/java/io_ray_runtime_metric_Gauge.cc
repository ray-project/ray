#include "ray/core_worker/lib/java/io_ray_runtime_metric_Gauge.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/stats/metric.h"

#include <jni.h>
#include <algorithm>
#include "opencensus/tags/tag_key.h"

using TagKeyType = opencensus::tags::TagKey;

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_Gauge_registerGaugeNative(
    JNIEnv *env, jobject obj, jstring j_name, jstring j_description, jstring j_unit,
    jobject tag_key_list) {
  std::string metric_name = JavaStringToNativeString(env, static_cast<jstring>(j_name));
  std::string description =
      JavaStringToNativeString(env, static_cast<jstring>(j_description));
  std::string unit = JavaStringToNativeString(env, static_cast<jstring>(j_unit));
  std::vector<std::string> tag_key_str_list;
  JavaStringListToNativeStringVector(env, tag_key_list, &tag_key_str_list);
  std::vector<TagKeyType> tag_keys;

  std::transform(tag_key_str_list.begin(), tag_key_str_list.end(),
                 std::back_inserter(tag_keys),
                 [](std::string tag_key) { return TagKeyType::Register(tag_key); });
  auto *gauge = new ray::stats::Gauge(metric_name, description, unit, tag_keys);
  return reinterpret_cast<long>(gauge);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_Gauge_unregisterGauge(
    JNIEnv *env, jobject obj, jlong gauge_pointer) {
  ray::stats::Gauge *gauge = reinterpret_cast<ray::stats::Gauge *>(gauge_pointer);
  delete gauge;
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_Gauge_recordNative(
    JNIEnv *env, jobject obj, jlong gauge_pointer, jdouble value, jobject tag_key_list,
    jobject tag_value_list) {}
