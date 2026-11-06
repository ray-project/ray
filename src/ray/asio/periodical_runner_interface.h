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

#include <boost/asio/deadline_timer.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <functional>
#include <memory>
#include <string>

namespace ray {

/// \class PeriodicalRunnerInterface
/// Interface for periodical runner functionality.
class PeriodicalRunnerInterface {
 public:
  virtual ~PeriodicalRunnerInterface() = default;

  virtual void RunFnPeriodically(std::function<void()> fn,
                                 uint64_t period_ms,
                                 std::string name) = 0;

 protected:
  virtual void DoRunFnPeriodically(
      std::function<void()> fn,
      boost::posix_time::milliseconds period,
      std::shared_ptr<boost::asio::deadline_timer> timer) = 0;

  virtual void DoRunFnPeriodicallyInstrumented(
      std::function<void()> fn,
      boost::posix_time::milliseconds period,
      std::shared_ptr<boost::asio::deadline_timer> timer,
      std::string name) = 0;
};

}  // namespace ray
