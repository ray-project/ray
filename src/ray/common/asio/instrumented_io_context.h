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
#include <memory>
#include <string>

#include "ray/common/event_stats.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

/// A proxy for boost::asio::io_context that collects statistics about posted handlers.
class instrumented_io_context : public boost::asio::io_context {
 public:
  /// Initializes the global stats struct after calling the base contructor.
  /// TODO(ekl) allow taking an externally defined event tracker.
  ///
  /// \param emit_metrics enables or disables metric emission on this io_context
  /// \param running_on_single_thread hints to the underlying io_context if locking should
  /// be enabled or not (that is, if running on multiple threads is true, then concurrency
  /// controls will engage)
  /// \param context_name optional name assigned to this io_context used for metric
  /// emission
  explicit instrumented_io_context(
      bool emit_metrics = false,
      bool running_on_single_thread = false,
      std::optional<std::string> context_name = std::nullopt);

  /// A proxy post function that collects count, queueing, and execution statistics for
  /// the given handler.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  /// \param delay_us Delay time before the handler will be executed.
  void post(std::function<void()> handler, std::string name, int64_t delay_us = 0);

  /// A proxy post function that collects count, queueing, and execution statistics for
  /// the given handler.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  void dispatch(std::function<void()> handler, std::string name);

  EventTracker &stats() const { return *event_stats_; };

 private:
  /// The event stats tracker to use to record asio handler stats to.
  std::shared_ptr<EventTracker> event_stats_;
  bool emit_metrics_;
  std::optional<std::string> context_name_;
};
