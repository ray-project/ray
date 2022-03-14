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

#include "ray/common/asio/periodical_runner.h"

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {

PeriodicalRunner::PeriodicalRunner(instrumented_io_context &io_service)
    : io_service_(io_service), mutex_() {}

PeriodicalRunner::~PeriodicalRunner() {
  absl::MutexLock lock(&mutex_);
  for (const auto &timer : timers_) {
    timer->cancel();
  }
  timers_.clear();
  RAY_LOG(DEBUG) << "PeriodicalRunner is destructed";
}

void PeriodicalRunner::RunFnPeriodically(std::function<void()> fn,
                                         uint64_t period_ms,
                                         const std::string name) {
  if (period_ms > 0) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
    {
      absl::MutexLock lock(&mutex_);
      timers_.push_back(timer);
    }
    io_service_.post(
        [this, fn = std::move(fn), period_ms, name, timer = std::move(timer)]() {
          if (RayConfig::instance().event_stats()) {
            DoRunFnPeriodicallyInstrumented(
                fn, boost::posix_time::milliseconds(period_ms), timer, name);
          } else {
            DoRunFnPeriodically(fn, boost::posix_time::milliseconds(period_ms), timer);
          }
        },
        "PeriodicalRunner.RunFnPeriodically");
  }
}

void PeriodicalRunner::DoRunFnPeriodically(
    const std::function<void()> &fn,
    boost::posix_time::milliseconds period,
    std::shared_ptr<boost::asio::deadline_timer> timer) {
  fn();
  absl::MutexLock lock(&mutex_);
  timer->expires_from_now(period);
  timer->async_wait([this, fn = std::move(fn), period, timer = std::move(timer)](
                        const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `timer` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g.
      // gcs_server)
      return;
    }
    RAY_CHECK(!error) << error.message();
    DoRunFnPeriodically(fn, period, timer);
  });
}

void PeriodicalRunner::DoRunFnPeriodicallyInstrumented(
    const std::function<void()> &fn,
    boost::posix_time::milliseconds period,
    std::shared_ptr<boost::asio::deadline_timer> timer,
    const std::string name) {
  fn();
  absl::MutexLock lock(&mutex_);
  timer->expires_from_now(period);
  // NOTE: We add the timer period to the enqueue time in order only measure the time in
  // which the handler was elgible to execute on the event loop but was queued by the
  // event loop.
  auto stats_handle = io_service_.stats().RecordStart(name, period.total_nanoseconds());
  timer->async_wait([this,
                     fn = std::move(fn),
                     period,
                     timer = std::move(timer),
                     stats_handle = std::move(stats_handle),
                     name](const boost::system::error_code &error) {
    io_service_.stats().RecordExecution(
        [this, fn = std::move(fn), error, period, timer = std::move(timer), name]() {
          if (error == boost::asio::error::operation_aborted) {
            // `operation_aborted` is set when `timer` is canceled or destroyed.
            // The Monitor lifetime may be short than the object who use it. (e.g.
            // gcs_server)
            return;
          }
          RAY_CHECK(!error) << error.message();
          DoRunFnPeriodicallyInstrumented(fn, period, timer, name);
        },
        std::move(stats_handle));
  });
}

}  // namespace ray
