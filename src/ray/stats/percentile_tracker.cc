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

#include "ray/stats/percentile_tracker.h"

#include <memory>

namespace ray {
namespace stats {

// PercentileTracker

/**
  Flush the current histogram and return the old histogram.

  In order to keep this light, this amounts to a pointer swap. A pointer
  to the old histogram is returned and a new histogram is constructed and
  saved on this instance.

  @return The old histogram containing accumulated data.
 */
std::unique_ptr<BucketHistogram> PercentileTracker::GetAndFlushHistogram() {
  std::unique_ptr<BucketHistogram> old_histogram = std::move(histogram_);
  histogram_ = std::make_unique<BucketHistogram>(bucket_scheme_.get());
  return old_histogram;
}

}  // namespace stats
}  // namespace ray
