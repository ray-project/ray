// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io_ray_runtime_metric_NativeMetric.h"

#include <jni.h>

#include <algorithm>

#include "jni_utils.h"
#include "opencensus/tags/tag_key.h"
#include "ray/stats/metric.h"

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<opencensus::tags::TagKey, std::string>>;

/// Convert jni metric related data to native type for stats.
/// \param[in] j_name metric name in jni string.
/// \param[in] j_description metric description in jni string.
/// \param[in] j_unit metric measurement unit in jni string.
/// \param[in] tag_key_list tag key list in java list.
/// \param[out] metric_name metric name in native string.
/// \param[out] description metric description in native string.
/// \param[out] unit metric measurement unit in native string.
/// \param[out] tag_keys metric tag key vector unit in native vector.
inline void MetricTransform(JNIEnv *env,
                            jstring j_name,
                            jstring j_description,
                            jstring j_unit,
                            jobject tag_key_list,
                            std::string *metric_name,
                            std::string *description,
                            std::string *unit,
                            std::vector<TagKeyType> &tag_keys) {
  *metric_name = JavaStringToNativeString(env, static_cast<jstring>(j_name));
  *description = JavaStringToNativeString(env, static_cast<jstring>(j_description));
  *unit = JavaStringToNativeString(env, static_cast<jstring>(j_unit));
  std::vector<std::string> tag_key_str_list;
  JavaStringListToNativeStringVector(env, tag_key_list, &tag_key_str_list);
  // We just call TagKeyType::Register to get tag object since opencensus tags
  // registry is thread-safe and registry can return a new tag or registered
  // item when it already exists.
  std::transform(tag_key_str_list.begin(),
                 tag_key_str_list.end(),
                 std::back_inserter(tag_keys),
                 [](std::string &tag_key) { return TagKeyType::Register(tag_key); });
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_NativeMetric_registerTagkeyNative(
    JNIEnv *env, jclass obj, jstring str) {
  std::string tag_key_name = JavaStringToNativeString(env, static_cast<jstring>(str));
  RAY_IGNORE_EXPR(TagKeyType::Register(tag_key_name));
}

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_metric_NativeMetric_registerGaugeNative(JNIEnv *env,
                                                            jclass obj,
                                                            jstring j_name,
                                                            jstring j_description,
                                                            jstring j_unit,
                                                            jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env,
                  j_name,
                  j_description,
                  j_unit,
                  tag_key_list,
                  &metric_name,
                  &description,
                  &unit,
                  tag_keys);
  auto *gauge = new stats::Gauge(metric_name, description, unit, tag_keys);
  return reinterpret_cast<jlong>(gauge);
}

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_metric_NativeMetric_registerCountNative(JNIEnv *env,
                                                            jclass obj,
                                                            jstring j_name,
                                                            jstring j_description,
                                                            jstring j_unit,
                                                            jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env,
                  j_name,
                  j_description,
                  j_unit,
                  tag_key_list,
                  &metric_name,
                  &description,
                  &unit,
                  tag_keys);
  auto *count = new stats::Count(metric_name, description, unit, tag_keys);
  return reinterpret_cast<jlong>(count);
}

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_metric_NativeMetric_registerSumNative(JNIEnv *env,
                                                          jclass obj,
                                                          jstring j_name,
                                                          jstring j_description,
                                                          jstring j_unit,
                                                          jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env,
                  j_name,
                  j_description,
                  j_unit,
                  tag_key_list,
                  &metric_name,
                  &description,
                  &unit,
                  tag_keys);
  auto *sum = new stats::Sum(metric_name, description, unit, tag_keys);
  return reinterpret_cast<jlong>(sum);
}

JNIEXPORT jlong JNICALL
Java_io_ray_runtime_metric_NativeMetric_registerHistogramNative(JNIEnv *env,
                                                                jclass obj,
                                                                jstring j_name,
                                                                jstring j_description,
                                                                jstring j_unit,
                                                                jdoubleArray j_boundaries,
                                                                jobject tag_key_list) {
  std::string metric_name;
  std::string description;
  std::string unit;
  std::vector<TagKeyType> tag_keys;
  MetricTransform(env,
                  j_name,
                  j_description,
                  j_unit,
                  tag_key_list,
                  &metric_name,
                  &description,
                  &unit,
                  tag_keys);
  std::vector<double> boundaries;

  JavaDoubleArrayToNativeDoubleVector(env, j_boundaries, &boundaries);

  auto *histogram =
      new stats::Histogram(metric_name, description, unit, boundaries, tag_keys);
  return reinterpret_cast<jlong>(histogram);
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_NativeMetric_unregisterMetricNative(
    JNIEnv *env, jclass obj, jlong metric_native_pointer) {
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_native_pointer);
  delete metric;
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_metric_NativeMetric_recordNative(JNIEnv *env,
                                                     jclass obj,
                                                     jlong metric_native_pointer,
                                                     jdouble value,
                                                     jobject tag_key_list,
                                                     jobject tag_value_list) {
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_native_pointer);
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
