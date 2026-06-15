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
/// This is meant to smooth a point-in-time metric: callers feed samples in via
/// Observe() and export the windowed max it returns. The max is returned on every
/// call (not just when it changes) so callers re-export each cycle, which the metric
/// pipeline requires -- gauge values are cleared after each export/scrape, so a value
/// that is not re-recorded disappears from subsequent scrapes.
///
/// Not thread-safe; callers must synchronize externally if shared across threads.
class WindowedMax {
 public:
  explicit WindowedMax(absl::Duration window_duration)
      : window_duration_(window_duration) {}

  /// Record a new sample with value `value` observed at `now`.
  ///
  /// Appends the sample, evicts samples older than the window, and returns the max
  /// over the remaining window.
  double Observe(absl::Time now, double value);

 private:
  struct Sample {
    absl::Time time;
    double value;
  };

  // Sliding window duration.
  const absl::Duration window_duration_;
  // Samples ordered oldest (front) to newest (back).
  std::deque<Sample> samples_;
};

}  // namespace observability
}  // namespace ray
