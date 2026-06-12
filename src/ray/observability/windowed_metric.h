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

#pragma once

#include <deque>
#include <optional>

#include "absl/time/time.h"

namespace ray {
namespace observability {

/// Tracks the maximum value observed over a sliding time window.
///
/// Samples are kept in a deque ordered oldest-to-newest. Each call to Observe()
/// appends the new sample, evicts samples that have fallen outside the window (older
/// than `now - window_duration`), and recomputes the maximum value over the
/// remaining samples.
///
/// This is meant to smooth a point-in-time metric: rather than exporting every
/// sample, callers feed samples in via Observe() and only re-export when the value
/// it returns is set (i.e. when the windowed max changed).
///
/// Not thread-safe; callers must synchronize externally if shared across threads.
class WindowedMetric {
 public:
  explicit WindowedMetric(absl::Duration window_duration)
      : window_duration_(window_duration) {}

  /// Record a new sample with value `value` observed at `now`.
  ///
  /// Appends the sample, evicts samples older than the window, and recomputes the
  /// max over the window. Returns the windowed max iff it differs from the value
  /// returned by the previous Observe() call (i.e. the metric should be re-exported);
  /// otherwise std::nullopt.
  std::optional<double> Observe(absl::Time now, double value);

 private:
  struct Sample {
    absl::Time time;
    double value;
  };

  // Sliding window duration.
  const absl::Duration window_duration_;
  // Samples ordered oldest (front) to newest (back).
  std::deque<Sample> samples_;
  // The windowed max as of the last Observe() call. Unset until the first Observe().
  std::optional<double> current_max_;
};

}  // namespace observability
}  // namespace ray
