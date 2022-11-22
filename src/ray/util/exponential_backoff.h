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

#include <gtest/gtest_prod.h>
#include <stdint.h>

namespace ray {

/// Provides the exponential backoff algorithm that is typically used
/// for throttling.
class ExponentialBackoff {
 public:
  /// Computes the backoff delay using the exponential backoff algorithm,
  /// using the formula
  /// delay = base * 2 ^ attempt
  ///
  /// @param max_attempt the maximum acceptable attempt count
  /// @param max_backoff_ms the maximum backoff value
  /// @return the delay in ms based on the formula
  static uint64_t GetBackoffMs(uint64_t attempt,
                               uint64_t base_ms,
                               uint64_t max_attempt = kMaxAttempt,
                               uint64_t max_backoff_ms = kMaxBackoffMs);

 private:
  // The default cap on the attempt number;
  static constexpr uint64_t kMaxAttempt = 50;

  // The default cap on the backoff delay.
  static constexpr uint64_t kMaxBackoffMs = 10 * 60 * 1000;
};

}  // namespace ray
