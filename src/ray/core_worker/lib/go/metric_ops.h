// Copyright 2025 The Ray Authors.
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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_METRIC_OPS_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_METRIC_OPS_H_

/**
 * @file metric_ops.h
 * @brief Business logic layer for metric registration / recording.
 *
 * Pure C++ bridge to the stats::Metric family (Count / Gauge / Sum /
 * Histogram). The C boundary functions below are the Go-facing contract,
 * callable via cgo.
 */

#include <cstdint>
#include <string>
#include <vector>

#include "ray/observability/metric_interface.h"

namespace ray {
namespace go {

/**
 * @brief Static helpers for bridging Go metric requests to stats::Metric.
 *
 * The helpers are pure (no CoreWorker dependency), so they are directly
 * unit-testable.
 */
class MetricOps {
 public:
  /**
   * @brief Build a stats::TagsType from parallel key / value arrays.
   *
   * Entries with a null key or null value are skipped. A zero or negative
   * count (or null arrays) yields an empty result.
   *
   * @param keys Array of tag key C strings.
   * @param values Array of tag value C strings.
   * @param count Number of key/value entries.
   * @return stats::TagsType of registered tag key -> value pairs.
   */
  static stats::TagsType BuildTags(const char *const *keys,
                                   const char *const *values,
                                   int count);

  /**
   * @brief Convert a C string array into a vector<string>.
   *
   * @param strs Array of C strings (null entries become empty strings).
   * @param count Number of entries.
   * @return vector<string> with one element per entry.
   */
  static std::vector<std::string> ToStringVector(const char *const *strs, int count);

  /**
   * @brief Convert a C double array into a vector<double>.
   *
   * @param values Array of doubles.
   * @param count Number of entries.
   * @return vector<double> with one element per entry.
   */
  static std::vector<double> ToDoubleVector(const double *values, int count);

  /**
   * @brief Validate a metric name against the stats metric name pattern
   * (^[a-zA-Z_:][a-zA-Z0-9_:]*$).
   *
   * stats::Metric enforces this pattern via RAY_CHECK_WITH_DISPLAY, which
   * terminates the process on invalid names. Boundary functions must reject
   * such input up front (returning an error) instead of letting the FATAL
   * check kill the process.
   *
   * @param name Metric name C string (may be null).
   * @return true if non-empty and matches the metric name pattern.
   */
  static bool IsValidMetricName(const char *name);
};

}  // namespace go
}  // namespace ray

extern "C" {
// Register a tag key. Returns 1 on success, 0 on failure (error set, caller
// frees with free()).
int ray_metric_register_tag_key(const char *tag_key, char **error);

// Register metrics. Returns an opaque handle (int64_t) on success, -1 on
// failure (error set, caller frees with free()).
//
// Handle lifetime: a handle stays valid until ray_metric_unregister is called
// for it. After unregister the handle is invalid: reusing it with
// ray_metric_record / ray_metric_unregister returns an error (0) and sets
// error, it never dereferences freed memory.
int64_t ray_metric_register_count(const char *name,
                                  const char *description,
                                  const char *unit,
                                  const char *const *tag_keys,
                                  int tag_key_count,
                                  char **error);
int64_t ray_metric_register_gauge(const char *name,
                                  const char *description,
                                  const char *unit,
                                  const char *const *tag_keys,
                                  int tag_key_count,
                                  char **error);
int64_t ray_metric_register_sum(const char *name,
                                const char *description,
                                const char *unit,
                                const char *const *tag_keys,
                                int tag_key_count,
                                char **error);
int64_t ray_metric_register_histogram(const char *name,
                                      const char *description,
                                      const char *unit,
                                      const double *boundaries,
                                      int boundary_count,
                                      const char *const *tag_keys,
                                      int tag_key_count,
                                      char **error);

// Record a value for a registered metric handle. Returns 1 on success, 0 on
// failure (error set, caller frees with free()).
int ray_metric_record(int64_t handle,
                      double value,
                      const char *const *tag_keys,
                      const char *const *tag_values,
                      int tag_count,
                      char **error);

// Unregister (destroy) a registered metric handle. Returns 1 on success, 0 on
// failure (error set, caller frees with free()).
int ray_metric_unregister(int64_t handle, char **error);
}

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_METRIC_OPS_H_
