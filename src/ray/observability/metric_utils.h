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

#include <list>
#include <optional>

#include "absl/time/time.h"

namespace ray {
namespace observability {

/// Tracks the maximum value observed over a sliding time window.
///
/// Samples are kept in a linked list ordered oldest-to-newest. Each call to Add()
/// appends the new sample, evicts samples that have fallen outside the window (older
/// than `now - window_duration`), and recomputes the maximum value over the
/// remaining samples.
///
/// This is meant to smooth a point-in-time metric: rather than exporting every
/// sample, callers feed samples in and only re-export when the windowed max changes
/// (Add() returns the new max only when it differs from the last reported value).
///
/// Not thread-safe; callers must synchronize externally if shared across threads.
class MetricSlidingWindow {
 public:
  explicit MetricSlidingWindow(absl::Duration window_duration)
      : window_duration_(window_duration) {}

  /// Record a new sample with value `value` observed at `now`.
  ///
  /// Appends the sample, evicts samples older than the window, and recomputes the
  /// max over the window. Returns the current max iff it differs from the value
  /// previously returned by Add() (i.e. the metric should be re-exported); returns
  /// std::nullopt when the max is unchanged.
  std::optional<double> Add(absl::Time now, double value);

  /// The current max value over the window, or std::nullopt if the window is empty.
  std::optional<double> WindowedMax() const;

 private:
  struct Sample {
    absl::Time time;
    double value;
  };

  const absl::Duration window_duration_;
  // Samples ordered oldest (front) to newest (back).
  std::list<Sample> samples_;
  // The last max value returned by Add(), used to detect changes. Unset until the
  // first Add().
  std::optional<double> last_reported_max_;
};

}  // namespace observability
}  // namespace ray
