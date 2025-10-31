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

#include <memory>
#include <string>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {

PeriodicalRunner::PeriodicalRunner(instrumented_io_context &io_service)
    : io_service_(io_service) {}

PeriodicalRunner::~PeriodicalRunner() {
  absl::MutexLock lock(&mutex_);
  for (const auto &timer : timers_) {
    timer->cancel();
  }
  timers_.clear();
}

void PeriodicalRunner::RunFnPeriodically(std::function<void()> fn,
                                         uint64_t period_ms,
                                         std::string name) {
  if (period_ms <= 0) {
    return;
  }
  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
  {
    absl::MutexLock lock(&mutex_);
    timers_.push_back(timer);
  }
  io_service_.post(
      [weak_self = weak_from_this(),
       fn = std::move(fn),
       period_ms,
       name = std::move(name),
       timer = std::move(timer)]() mutable {
        if (auto self = weak_self.lock(); self) {
          if (RayConfig::instance().event_stats()) {
            self->DoRunFnPeriodicallyInstrumented(
                std::move(fn),
                boost::posix_time::milliseconds(period_ms),
                std::move(timer),
                std::move(name));
          } else {
            self->DoRunFnPeriodically(std::move(fn),
                                      boost::posix_time::milliseconds(period_ms),
                                      std::move(timer));
          }
        }
      },
      "PeriodicalRunner.RunFnPeriodically");
}

void PeriodicalRunner::DoRunFnPeriodically(
    std::function<void()> fn,
    boost::posix_time::milliseconds period,
    std::shared_ptr<boost::asio::deadline_timer> timer) {
  fn();
  absl::MutexLock lock(&mutex_);
  timer->expires_from_now(period);

  timer->async_wait(
      [weak_self = weak_from_this(),
       fn = std::move(fn),
       period,
       timer = std::move(timer)](const boost::system::error_code &error) mutable {
        if (auto self = weak_self.lock(); self) {
          if (error == boost::asio::error::operation_aborted) {
            // `operation_aborted` is set when `timer` is canceled or destroyed.
            // The Monitor lifetime may be short than the object who use it. (e.g.
            // gcs_server)
            return;
          }
          RAY_CHECK(!error) << error.message();
          self->DoRunFnPeriodically(std::move(fn), period, std::move(timer));
        }
      });
}

void PeriodicalRunner::DoRunFnPeriodicallyInstrumented(
    std::function<void()> fn,
    boost::posix_time::milliseconds period,
    std::shared_ptr<boost::asio::deadline_timer> timer,
    std::string name) {
  fn();
  absl::MutexLock lock(&mutex_);
  timer->expires_from_now(period);
  // NOTE: We add the timer period to the enqueue time in order only measure the time in
  // which the handler was elgible to execute on the event loop but was queued by the
  // event loop.
  auto stats_handle =
      io_service_.stats().RecordStart(name, false, period.total_nanoseconds());
  timer->async_wait(
      [weak_self = weak_from_this(),
       fn = std::move(fn),
       period,
       timer = std::move(timer),
       stats_handle = std::move(stats_handle),
       name = std::move(name)](const boost::system::error_code &error) mutable {
        if (auto self = weak_self.lock(); self) {
          self->io_service_.stats().RecordExecution(
              [self,
               fn = std::move(fn),
               error,
               period,
               timer = std::move(timer),
               name = std::move(name)]() mutable {
                if (error == boost::asio::error::operation_aborted) {
                  // `operation_aborted` is set when `timer` is canceled or destroyed.
                  // The Monitor lifetime may be short than the object who use it. (e.g.
                  // gcs_server)
                  return;
                }
                RAY_CHECK(!error) << error.message();
                self->DoRunFnPeriodicallyInstrumented(
                    std::move(fn), period, std::move(timer), std::move(name));
              },
              std::move(stats_handle));
        }
      });
}

}  // namespace ray
