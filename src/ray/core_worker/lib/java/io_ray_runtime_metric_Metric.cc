#include "ray/core_worker/lib/java/io_ray_runtime_metric_Metric.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/stats/metric.h"

#include <jni.h>
#include <algorithm>
#include "opencensus/tags/tag_key.h"

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<opencensus::tags::TagKey, std::string>>;

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_Metric_unregisterMetricNative(
    JNIEnv *env, jobject obj, jlong metric_native_pointer) {
  ray::stats::Metric *metric =
      reinterpret_cast<ray::stats::Metric *>(metric_native_pointer);
  delete metric;
}

JNIEXPORT void JNICALL Java_io_ray_runtime_metric_Metric_recordNative(
    JNIEnv *env, jobject obj, jlong metric_native_pointer, jdouble value,
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
