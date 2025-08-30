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

#include "ray/common/asio/instrumented_io_context.h"

#include <string>
#include <utility>

#include "ray/common/asio/asio_chaos.h"
#include "ray/common/asio/asio_util.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_defs.h"

namespace {

// Post a probe. Records the lag and schedule another probe.
// Requires: `interval_ms` > 0.
void LagProbeLoop(instrumented_io_context &io_context,
                  int64_t interval_ms,
                  const std::optional<std::string> &context_name) {
  auto begin = std::chrono::steady_clock::now();
  io_context.post(
      [&io_context, begin, interval_ms, context_name]() {
        auto end = std::chrono::steady_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
        ray::stats::STATS_io_context_event_loop_lag_ms.Record(
            duration.count(),
            {
                {"Name", context_name.value_or(GetThreadName())},
            });

        // Schedule the next probe. If `duration` is larger than `interval_ms`, we
        // should schedule the next probe immediately. Otherwise, we should wait
        // for `interval_ms - duration`.
        auto delay = interval_ms - duration.count();
        if (delay <= 0) {
          LagProbeLoop(io_context, interval_ms, context_name);
        } else {
          execute_after(
              io_context,
              [&io_context, interval_ms, context_name]() {
                LagProbeLoop(io_context, interval_ms, context_name);
              },
              std::chrono::milliseconds(delay));
        }
      },
      "event_loop_lag_probe");
}

void ScheduleLagProbe(instrumented_io_context &io_context,
                      const std::optional<std::string> &context_name) {
  auto interval =
      RayConfig::instance().io_context_event_loop_lag_collection_interval_ms();
  if (interval <= 0) {
    return;
  }
  RAY_LOG(DEBUG) << "Scheduling lag probe for the io_context on thread "
                 << context_name.value_or(GetThreadName()) << " every " << interval
                 << "ms";
  // At this time, the `io_context` may not be running yet, so we need to post the
  // first probe.
  io_context.post(
      [&io_context, interval, context_name]() {
        LagProbeLoop(io_context, interval, context_name);
      },
      "event_loop_lag_probe");
}
}  // namespace

instrumented_io_context::instrumented_io_context(
    const bool emit_metrics,
    const bool running_on_single_thread,
    const std::optional<std::string> context_name)
    : boost::asio::io_context(
          running_on_single_thread ? 1 : BOOST_ASIO_CONCURRENCY_HINT_DEFAULT),
      event_stats_(std::make_shared<EventTracker>()),
      emit_metrics_(emit_metrics),
      context_name_(context_name) {
  if (emit_metrics) {
    ScheduleLagProbe(*this, context_name_);
  }
}

void instrumented_io_context::post(std::function<void()> handler,
                                   std::string name,
                                   int64_t delay_us) {
  delay_us += ray::asio::testing::GetDelayUs(name);
  if (RayConfig::instance().event_stats()) {
    // References are only invalidated upon deletion of the corresponding item from the
    // table, which we won't do until this io_context is deleted. Provided that
    // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
    // handler stats it->second from multiple threads without acquiring a table-level
    // readers lock in the callback.
    auto stats_handle =
        event_stats_->RecordStart(std::move(name), emit_metrics_, 0, context_name_);
    handler = [handler = std::move(handler),
               stats_handle = std::move(stats_handle)]() mutable {
      EventTracker::RecordExecution(handler, std::move(stats_handle));
    };
  }

  if (delay_us == 0) {
    boost::asio::post(*this, std::move(handler));
  } else {
    execute_after(*this, std::move(handler), std::chrono::microseconds(delay_us));
  }
}

void instrumented_io_context::dispatch(std::function<void()> handler, std::string name) {
  if (!RayConfig::instance().event_stats()) {
    return boost::asio::post(*this, std::move(handler));
  }
  auto stats_handle =
      event_stats_->RecordStart(std::move(name), emit_metrics_, 0, context_name_);
  // References are only invalidated upon deletion of the corresponding item from the
  // table, which we won't do until this io_context is deleted. Provided that
  // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
  // handler stats it->second from multiple threads without acquiring a table-level
  // readers lock in the callback.
  boost::asio::dispatch(
      *this,
      [handler = std::move(handler), stats_handle = std::move(stats_handle)]() mutable {
        EventTracker::RecordExecution(handler, std::move(stats_handle));
      });
}
