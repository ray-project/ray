// Copyright 2026 The Ray Authors.
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

#include "ray/observability/metric_utils.h"

#include <algorithm>

namespace ray {
namespace observability {

std::optional<double> LatencySlidingWindow::Add(absl::Time now, double latency_ms) {
  samples_.push_back({now, latency_ms});

  // Evict samples that have fallen out of the window. The sample we just appended is
  // at `now`, so it is always retained.
  const absl::Time cutoff = now - window_duration_;
  while (!samples_.empty() && samples_.front().time < cutoff) {
    samples_.pop_front();
  }

  // Recompute the max over the remaining window. samples_ is guaranteed non-empty
  // because we just appended a sample that cannot be evicted.
  double max = samples_.front().latency_ms;
  for (const auto &sample : samples_) {
    max = std::max(max, sample.latency_ms);
  }

  if (!last_reported_max_.has_value() || *last_reported_max_ != max) {
    last_reported_max_ = max;
    return max;
  }
  return std::nullopt;
}

std::optional<double> LatencySlidingWindow::WindowedMax() const {
  if (samples_.empty()) {
    return std::nullopt;
  }
  // last_reported_max_ always reflects the current window max after the most recent
  // Add(); but WindowedMax() may be called before any Add(), handled by the empty
  // check.
  double max = samples_.front().latency_ms;
  for (const auto &sample : samples_) {
    max = std::max(max, sample.latency_ms);
  }
  return max;
}

}  // namespace observability
}  // namespace ray
