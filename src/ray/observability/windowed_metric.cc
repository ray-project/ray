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

#include "ray/observability/windowed_metric.h"

#include <algorithm>

namespace ray {
namespace observability {

std::optional<double> WindowedMetric::Observe(absl::Time now, double value) {
  samples_.push_back({now, value});

  // Evict samples that have fallen out of the window. Never evict the last remaining
  // sample: a non-positive window_duration_ would otherwise empty the deque and make
  // the max computation below read from an empty container.
  const absl::Time cutoff = now - window_duration_;
  while (samples_.size() > 1 && samples_.front().time < cutoff) {
    samples_.pop_front();
  }

  // Recompute the max over the remaining window. samples_ is guaranteed non-empty
  // because we just appended a sample and never evict the last one.
  double max = samples_.front().value;
  for (const auto &sample : samples_) {
    max = std::max(max, sample.value);
  }

  // Only return the max when it changed, so callers re-export the metric only when
  // it moves.
  if (!current_max_.has_value() || *current_max_ != max) {
    current_max_ = max;
    return max;
  }
  return std::nullopt;
}

}  // namespace observability
}  // namespace ray
