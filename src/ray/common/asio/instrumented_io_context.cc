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

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <utility>

#include "ray/common/asio/asio_chaos.h"
#include "ray/common/asio/asio_util.h"

void instrumented_io_context::post(std::function<void()> handler,
                                   const std::string name,
                                   int64_t delay_us) {
  if (RayConfig::instance().event_stats()) {
    // References are only invalidated upon deletion of the corresponding item from the
    // table, which we won't do until this io_context is deleted. Provided that
    // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
    // handler stats it->second from multiple threads without acquiring a table-level
    // readers lock in the callback.
    const auto stats_handle = event_stats_->RecordStart(name);
    handler = [handler = std::move(handler), stats_handle = std::move(stats_handle)]() {
      EventTracker::RecordExecution(handler, std::move(stats_handle));
    };
  }
  delay_us += ray::asio::testing::get_delay_us(name);
  if (delay_us == 0) {
    boost::asio::io_context::post(std::move(handler));
  } else {
    RAY_LOG(DEBUG) << "Deferring " << name << " by " << delay_us << "us";
    execute_after(*this, std::move(handler), std::chrono::microseconds(delay_us));
  }
}

void instrumented_io_context::dispatch(std::function<void()> handler,
                                       const std::string name) {
  if (!RayConfig::instance().event_stats()) {
    return boost::asio::io_context::post(std::move(handler));
  }
  const auto stats_handle = event_stats_->RecordStart(name);
  // References are only invalidated upon deletion of the corresponding item from the
  // table, which we won't do until this io_context is deleted. Provided that
  // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
  // handler stats it->second from multiple threads without acquiring a table-level
  // readers lock in the callback.
  boost::asio::io_context::dispatch(
      [handler = std::move(handler), stats_handle = std::move(stats_handle)]() {
        EventTracker::RecordExecution(handler, std::move(stats_handle));
      });
}
