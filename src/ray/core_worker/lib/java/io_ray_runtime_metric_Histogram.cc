#include "ray/core_worker/lib/java/io_ray_runtime_metric_Histogram.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/stats/metric.h"

#include <jni.h>

JNIEXPORT jlong JNICALL Java_io_ray_runtime_metric_Histogram_registerHistogramNative(
    JNIEnv *env, jobject obj, jstring j_name, jstring j_description, jstring j_unit,
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
