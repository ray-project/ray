// Copyright 2022 The Ray Authors.
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

#include <algorithm>
#include <cstdint>
#include <limits>

#include "ray/util/logging.h"

namespace ray {

/// Provides the exponential backoff algorithm that is typically used
/// for throttling.
class ExponentialBackoff {
 public:
  /// Construct an exponential back off counter.
  ///
  /// \param[in] initial_value The start value for this counter
  /// \param[in] multiplier The multiplier for this counter.
  /// \param[in] max_value The maximum value for this counter. By default it's
  ///    infinite double.
  ExponentialBackoff(uint64_t initial_value,
                     double multiplier,
                     uint64_t max_value = std::numeric_limits<uint64_t>::max())
      : curr_value_(initial_value),
        initial_value_(initial_value),
        max_value_(max_value),
        multiplier_(multiplier) {
    RAY_CHECK(multiplier > 0.0) << "Multiplier must be greater than 0";
  }

  /// Computes the backoff delay using the exponential backoff algorithm,
  /// using the formula
  ///
  /// delay = min(base * 2 ^ attempt, max_backoff)
  ///
  ///
  /// @param max_backoff_ms the maximum backoff value
  /// @return the delay in ms based on the formula
  static uint64_t GetBackoffMs(uint64_t attempt,
                               uint64_t base_ms,
                               uint64_t max_backoff_ms = kDefaultMaxBackoffMs);

  uint64_t Next() {
    auto ret = curr_value_;
    curr_value_ = curr_value_ * multiplier_;
    curr_value_ = std::min(curr_value_, max_value_);
    return ret;
  }

  uint64_t Current() { return curr_value_; }

  void Reset() { curr_value_ = initial_value_; }

 private:
  uint64_t curr_value_;
  uint64_t initial_value_;
  uint64_t max_value_;
  double multiplier_;

  // The default cap on the backoff delay.
  static constexpr uint64_t kDefaultMaxBackoffMs = 1 * 60 * 1000;
};

}  // namespace ray
