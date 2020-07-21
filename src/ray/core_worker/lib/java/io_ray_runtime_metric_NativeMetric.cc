#include "io_ray_runtime_metric_NativeMetric.h"
#include "jni_utils.h"
#include "ray/stats/metric.h"

#include <jni.h>

#include <algorithm>
#include "opencensus/tags/tag_key.h"

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<opencensus::tags::TagKey, std::string>>;

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_NativeMetric_registerTagkeyNative(
    JNIEnv *env, jclass obj, jstring str) {
  std::string tag_key_name = JavaStringToNativeString(env, static_cast<jstring>(str));
  RAY_IGNORE_EXPR(TagKeyType::Register(tag_key_name));
}

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_NativeMetric_registerGaugeNative(
    JNIEnv *env, jclass obj, jstring j_name, jstring j_description, jstring j_unit,
    jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env, j_name, j_description, j_unit, tag_key_list, &metric_name,
                  &description, &unit, tag_keys);
  auto *gauge = new ray::stats::Gauge(metric_name, description, unit, tag_keys);
  return reinterpret_cast<long>(gauge);
}

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_NativeMetric_registerCountNative(
    JNIEnv *env, jclass obj, jstring j_name, jstring j_description, jstring j_unit,
    jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env, j_name, j_description, j_unit, tag_key_list, &metric_name,
                  &description, &unit, tag_keys);
  auto *count = new ray::stats::Count(metric_name, description, unit, tag_keys);
  return reinterpret_cast<long>(count);
}

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_NativeMetric_registerSumNative(
    JNIEnv *env, jclass obj, jstring j_name, jstring j_description, jstring j_unit,
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

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_NativeMetric_registerHistogramNative(
    JNIEnv *env, jclass obj, jstring j_name, jstring j_description, jstring j_unit,
    jdoubleArray j_boundaries, jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env, j_name, j_description, j_unit, tag_key_list, &metric_name,
                  &description, &unit, tag_keys);
  std::vector<double> boundaries;

  JavaDoubleArrayToNativeDoubleVector(env, j_boundaries, &boundaries);

  auto *histogram =
      new ray::stats::Histogram(metric_name, description, unit, boundaries, tag_keys);
  return reinterpret_cast<long>(histogram);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_NativeMetric_unregisterMetricNative(
    JNIEnv *env, jclass obj, jlong metric_native_pointer) {
  ray::stats::Metric *metric =
      reinterpret_cast<ray::stats::Metric *>(metric_native_pointer);
  delete metric;
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_NativeMetric_recordNative(
    JNIEnv *env, jclass obj, jlong metric_native_pointer, jdouble value,
    jobject tag_key_list, jobject tag_value_list) {
  ray::stats::Metric *metric =
      reinterpret_cast<ray::stats::Metric *>(metric_native_pointer);
  std::vector<std::string> tag_key_str_list;
  std::vector<std::string> tag_value_str_list;
  JavaStringListToNativeStringVector(env, tag_key_list, &tag_key_str_list);
  JavaStringListToNativeStringVector(env, tag_value_list, &tag_value_str_list);
  TagsType tags;
  for (size_t i = 0; i < tag_key_str_list.size(); ++i) {
    tags.push_back({TagKeyType::Register(tag_key_str_list[i]), tag_value_str_list[i]});
  }
  metric->Record(value, tags);
}

#ifdef __cplusplus
}
#endif
