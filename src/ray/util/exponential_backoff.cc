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

#include "ray/util/exponential_backoff.h"

#include <math.h>

#include "ray/util/logging.h"

namespace ray {

uint64_t ExponentialBackoff::GetBackoffMs(uint64_t attempt,
                                          uint64_t base_ms,
                                          uint64_t max_attempt,
                                          uint64_t max_backoff_ms) {
  if (attempt > max_attempt) {
    attempt = max_attempt;
    RAY_LOG_EVERY_MS(INFO, 60000) << "Backoff attempt exceeded max, attempt= " << attempt
                                  << " max attempt= " << max_backoff_ms;
  }
  uint64_t delay = static_cast<uint64_t>(pow(2, attempt));
  // Use max_backoff_ms if there is an overflow.
  if (delay == 0) {
    return max_backoff_ms;
  }
  return std::min(base_ms * delay, max_backoff_ms);
};

}  // namespace ray
