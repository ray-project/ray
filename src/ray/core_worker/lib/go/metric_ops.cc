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

#include "ray/core_worker/lib/go/metric_ops.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/lib/go/cgo_error.h"
#include "ray/stats/metric.h"

namespace ray {
namespace go {

stats::TagsType MetricOps::BuildTags(const char *const *keys,
                                     const char *const *values,
                                     int count) {
  stats::TagsType tags;
  if (keys == nullptr || values == nullptr || count <= 0) {
    return tags;
  }
  tags.reserve(count);
  for (int i = 0; i < count; ++i) {
    if (keys[i] == nullptr || values[i] == nullptr) {
      continue;
    }
    tags.emplace_back(stats::TagKeyType::Register(keys[i]), std::string(values[i]));
  }
  return tags;
}

std::vector<std::string> MetricOps::ToStringVector(const char *const *strs, int count) {
  std::vector<std::string> result;
  if (strs == nullptr || count <= 0) {
    return result;
  }
  result.reserve(count);
  for (int i = 0; i < count; ++i) {
    result.emplace_back(strs[i] ? strs[i] : "");
  }
  return result;
}

std::vector<double> MetricOps::ToDoubleVector(const double *values, int count) {
  std::vector<double> result;
  if (values == nullptr || count <= 0) {
    return result;
  }
  result.assign(values, values + count);
  return result;
}

bool MetricOps::IsValidMetricName(const char *name) {
  if (name == nullptr) {
    return false;
  }
  // Delegates to the authoritative regex used by stats::Metric's
  // RAY_CHECK_WITH_DISPLAY so the boundary validation can never drift from the
  // regex that would otherwise terminate the process on a bad name.
  return std::regex_match(name, ray::stats::Metric::GetMetricNameRegex());
}

}  // namespace go
}  // namespace ray

namespace {

// Global registry of live metric handles. A handle is valid only while it is
// present in this map; after unregister it is removed and any reuse (record or
// unregister) is rejected with an error instead of a use-after-free /
// double-free.
//
// The map owns every metric through a shared_ptr and is guarded by a mutex.
// Handles are monotonic values assigned by g_next_metric_handle, never raw
// object addresses, so a freed metric can never be aliased by a later
// allocation reusing the same address (ABA).
std::mutex g_metric_registry_mutex;
std::unordered_map<int64_t, std::shared_ptr<ray::stats::Metric>> g_metric_registry;

// Monotonic handle allocator; starts at 1 so a handle is always positive.
std::atomic<int64_t> g_next_metric_handle{1};

// RegisterMetric validates the metric name, constructs a stats metric of type
// MetricT with the given trailing arguments (e.g. histogram buckets), and
// stores it in the registry under a freshly allocated handle. Returns the
// handle or -1 on failure.
template <typename MetricT, typename... Args>
int64_t RegisterMetric(const char *name,
                       const char *description,
                       const char *unit,
                       const char *const *tag_keys,
                       int tag_key_count,
                       char **error,
                       Args &&...args) {
  if (error) {
    *error = nullptr;
  }
  try {
    if (!ray::go::MetricOps::IsValidMetricName(name)) {
      SetBoundaryError(error, "invalid metric name");
      return -1;
    }
    auto tags = ray::go::MetricOps::ToStringVector(tag_keys, tag_key_count);
    auto metric = std::make_shared<MetricT>(name,
                                            description ? description : "",
                                            unit ? unit : "",
                                            std::forward<Args>(args)...,
                                            tags);
    int64_t handle = g_next_metric_handle.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(g_metric_registry_mutex);
      g_metric_registry[handle] = std::move(metric);
    }
    return handle;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return -1;
  }
}

}  // namespace

extern "C" {

int ray_metric_register_tag_key(const char *tag_key, char **error) {
  if (error) {
    *error = nullptr;
  }
  if (tag_key == nullptr || tag_key[0] == '\0') {
    SetBoundaryError(error, "tag key must be a non-empty string");
    return 0;
  }
  try {
    ray::stats::TagKeyType::Register(tag_key);
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}

int64_t ray_metric_register_count(const char *name,
                                  const char *description,
                                  const char *unit,
                                  const char *const *tag_keys,
                                  int tag_key_count,
                                  char **error) {
  return RegisterMetric<ray::stats::Count>(
      name, description, unit, tag_keys, tag_key_count, error);
}

int64_t ray_metric_register_gauge(const char *name,
                                  const char *description,
                                  const char *unit,
                                  const char *const *tag_keys,
                                  int tag_key_count,
                                  char **error) {
  return RegisterMetric<ray::stats::Gauge>(
      name, description, unit, tag_keys, tag_key_count, error);
}

int64_t ray_metric_register_sum(const char *name,
                                const char *description,
                                const char *unit,
                                const char *const *tag_keys,
                                int tag_key_count,
                                char **error) {
  return RegisterMetric<ray::stats::Sum>(
      name, description, unit, tag_keys, tag_key_count, error);
}

int64_t ray_metric_register_histogram(const char *name,
                                      const char *description,
                                      const char *unit,
                                      const double *boundaries,
                                      int boundary_count,
                                      const char *const *tag_keys,
                                      int tag_key_count,
                                      char **error) {
  if (error) {
    *error = nullptr;
  }
  if (boundaries == nullptr || boundary_count <= 0) {
    SetBoundaryError(error, "histogram boundaries required");
    return -1;
  }
  bool has_nan = false;
  for (int i = 0; i < boundary_count; ++i) {
    has_nan = has_nan || std::isnan(boundaries[i]);
  }
  if (has_nan || !std::is_sorted(boundaries, boundaries + boundary_count)) {
    SetBoundaryError(error, "histogram boundaries must be monotonically non-decreasing");
    return -1;
  }
  auto buckets = ray::go::MetricOps::ToDoubleVector(boundaries, boundary_count);
  return RegisterMetric<ray::stats::Histogram>(
      name, description, unit, tag_keys, tag_key_count, error, buckets);
}

int ray_metric_record(int64_t handle,
                      double value,
                      const char *const *tag_keys,
                      const char *const *tag_values,
                      int tag_count,
                      char **error) {
  if (error) {
    *error = nullptr;
  }
  try {
    // Copy the shared_ptr out under the lock, then Record outside of it: the
    // registry lock must never be held across Metric::Record, which can block
    // on stats-internal state (and on the opencensus registration mutex on the
    // first record). Holding the shared_ptr keeps the metric alive for the
    // whole Record even if a concurrent unregister drops the registry's
    // reference; a miss hits the unknown-handle error path.
    std::shared_ptr<ray::stats::Metric> metric;
    {
      std::lock_guard<std::mutex> lock(g_metric_registry_mutex);
      auto it = g_metric_registry.find(handle);
      if (it == g_metric_registry.end()) {
        SetBoundaryError(error, "unknown metric handle");
        return 0;
      }
      metric = it->second;
    }
    auto tags = ray::go::MetricOps::BuildTags(tag_keys, tag_values, tag_count);
    metric->Record(value, tags);
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}

int ray_metric_unregister(int64_t handle, char **error) {
  if (error) {
    *error = nullptr;
  }
  try {
    std::shared_ptr<ray::stats::Metric> metric;
    {
      std::lock_guard<std::mutex> lock(g_metric_registry_mutex);
      auto it = g_metric_registry.find(handle);
      if (it == g_metric_registry.end()) {
        SetBoundaryError(error, "unknown metric handle");
        return 0;
      }
      metric = std::move(it->second);
      g_metric_registry.erase(it);
    }
    // Release the registry's reference outside the lock. Normally this runs the
    // destructor (the last reference); if a concurrent Record still holds a
    // copy, it just drops the reference and the destructor runs when that
    // Record finishes, so the metric is never destroyed mid-record.
    //
    // Destroying the metric does NOT reclaim the opencensus view registered by
    // Record (see src/ray/stats/metric.cc -> RegisterAsView ->
    // ViewDescriptor::RegisterForExport): C++ opencensus exposes no view
    // deregistration API, so a registered view lives for the process lifetime.
    // Re-registering a metric with the same name reuses that view, so growth is
    // bounded by the number of distinct metric names -- the same semantics as
    // the Python and Java workers. No cleanup is attempted here by design.
    metric.reset();
    return 1;
  } catch (const std::exception &e) {
    SetBoundaryError(error, e.what());
    return 0;
  }
}

}  // extern "C"
