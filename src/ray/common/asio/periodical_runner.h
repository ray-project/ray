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

#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include "ray/common/asio/instrumented_io_context.h"

namespace ray {

/// \class PeriodicalRunner
/// A periodical runner attached with an io_context.
/// It can run functions with specified period. Each function is triggered by its timer.
/// To run a function, call `RunFnPeriodically(fn, period_ms)`.
/// All registered functions will stop running once this object is destructed.
class PeriodicalRunner {
 public:
  PeriodicalRunner(instrumented_io_context &io_service);

  ~PeriodicalRunner();

  void RunFnPeriodically(std::function<void()> fn, uint64_t period_ms,
                         const std::string name = "UNKNOWN");

 private:
  void DoRunFnPeriodically(const std::function<void()> &fn,
                           boost::posix_time::milliseconds period,
                           boost::asio::deadline_timer &timer);

  void DoRunFnPeriodicallyInstrumented(const std::function<void()> &fn,
                                       boost::posix_time::milliseconds period,
                                       boost::asio::deadline_timer &timer,
                                       const std::string name);

  instrumented_io_context &io_service_;
  std::vector<std::shared_ptr<boost::asio::deadline_timer>> timers_;
};

}  // namespace ray
