// Copyright 2017 The Ray Authors.
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

#include <cstdint>
#include <functional>
#include <string>

namespace ray {

/**
 * @brief Interface for scheduling a function to run periodically.
 */
class PeriodicalRunnerInterface {
 public:
  virtual ~PeriodicalRunnerInterface() = default;

  /**
   * @brief Schedule `fn` to be invoked every `period_ms` milliseconds.
   *
   * @param fn the function to invoke periodically.
   * @param period_ms the interval between invocations, in milliseconds.
   * @param name a human-readable name identifying the periodic task.
   */
  virtual void RunFnPeriodically(std::function<void()> fn,
                                 uint64_t period_ms,
                                 std::string name) = 0;
};

}  // namespace ray
