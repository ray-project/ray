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
#include <functional>
#include <memory>
#include <string>

#include "ray/common/asio/periodical_runner.h"

namespace ray {

class FakePeriodicalRunner : public PeriodicalRunnerInterface {
 public:
  void RunFnPeriodically(std::function<void()> fn,
                         uint64_t period_ms,
                         std::string name) override {}

 protected:
  void DoRunFnPeriodically(std::function<void()> fn,
                           boost::posix_time::milliseconds period,
                           std::shared_ptr<boost::asio::deadline_timer> timer) override {}

  void DoRunFnPeriodicallyInstrumented(std::function<void()> fn,
                                       boost::posix_time::milliseconds period,
                                       std::shared_ptr<boost::asio::deadline_timer> timer,
                                       std::string name) override {}
};

}  // namespace ray
