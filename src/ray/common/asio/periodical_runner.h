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
#include <memory>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"

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

/// \class PeriodicalRunner
/// A periodical runner attached with an io_context.
/// It can run functions with specified period. Each function is triggered by its timer.
/// To run a function, call `RunFnPeriodically(fn, period_ms)`.
/// All registered functions will stop running once this object is destructed.
//
// Lifetime: once a PeriodicalRunner is destructed, all its timers are cancelled. The
// scheduled asio tasks keep a weak_ptr to the PeriodicalRunner, and they won't run after
// the PeriodicalRunner is destructed.
class PeriodicalRunner : public PeriodicalRunnerInterface,
                         public std::enable_shared_from_this<PeriodicalRunner> {
 public:
  static std::shared_ptr<PeriodicalRunner> Create(instrumented_io_context &io_service) {
    // Sadly we can't use std::make_shared because the constructor is private.
    return std::shared_ptr<PeriodicalRunner>(new PeriodicalRunner(io_service));
  }

  ~PeriodicalRunner();

  void RunFnPeriodically(std::function<void()> fn,
                         uint64_t period_ms,
                         std::string name) override ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  explicit PeriodicalRunner(instrumented_io_context &io_service);

  void DoRunFnPeriodically(std::function<void()> fn,
                           boost::posix_time::milliseconds period,
                           std::shared_ptr<boost::asio::deadline_timer> timer) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void DoRunFnPeriodicallyInstrumented(std::function<void()> fn,
                                       boost::posix_time::milliseconds period,
                                       std::shared_ptr<boost::asio::deadline_timer> timer,
                                       std::string name) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  instrumented_io_context &io_service_;
  mutable absl::Mutex mutex_;
  std::vector<std::shared_ptr<boost::asio::deadline_timer>> timers_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray
