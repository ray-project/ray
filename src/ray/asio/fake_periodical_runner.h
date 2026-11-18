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
#include <unordered_map>

#include "ray/asio/periodical_runner_interface.h"
#include "ray/util/logging.h"

namespace ray {

class FakePeriodicalRunner : public PeriodicalRunnerInterface {
 public:
  void RunFnPeriodically(std::function<void()> fn,
                         uint64_t period_ms,
                         std::string name) override {
    fns_[name] = std::move(fn);
  }

  /// Invoke the lambda that was registered under the given name.
  /// Used by tests to simulate a periodic timer tick at a controlled time.
  void Trigger(const std::string &name) {
    auto it = fns_.find(name);
    RAY_CHECK(it != fns_.end()) << "No function registered with name: " << name;
    it->second();
  }

 protected:
  void DoRunFnPeriodically(std::function<void()> fn,
                           boost::posix_time::milliseconds period,
                           std::shared_ptr<boost::asio::deadline_timer> timer) override {}

  void DoRunFnPeriodicallyInstrumented(std::function<void()> fn,
                                       boost::posix_time::milliseconds period,
                                       std::shared_ptr<boost::asio::deadline_timer> timer,
                                       std::string name) override {}

 private:
  std::unordered_map<std::string, std::function<void()>> fns_;
};

}  // namespace ray
