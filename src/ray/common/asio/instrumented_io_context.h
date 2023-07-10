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
#include <limits>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/event_stats.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

/// A proxy for boost::asio::io_context that collects statistics about posted handlers.
class instrumented_io_context : public boost::asio::io_context {
 public:
  /// Initializes the global stats struct after calling the base contructor.
  /// TODO(ekl) allow taking an externally defined event tracker.
  instrumented_io_context()
      : is_running_{false}, event_stats_(std::make_shared<EventTracker>()) {}

  /// Run the io_context if and only if no other thread is running it. Blocks
  /// other threads from running once started. Noop if there is a thread
  /// running it already. Only used in GcsClient::Connect, to be deprecated
  /// after the introduction of executors.
  void run_if_not_running(std::function<void()> prerun_fn) {
    absl::MutexLock l(&mu_);
    // Note: this doesn't set is_running_ because it blocks anything else from
    // running anyway.
    if (!is_running_) {
      prerun_fn();
      is_running_ = true;
      boost::asio::io_context::run();
    }
  }

  /// Assumes the mutex is held. Undefined behavior if not.
  void stop_without_lock() {
    is_running_ = false;
    boost::asio::io_context::stop();
  }

  void run() {
    {
      absl::MutexLock l(&mu_);
      is_running_ = true;
    }
    boost::asio::io_context::run();
  }

  void stop() {
    {
      absl::MutexLock l(&mu_);
      is_running_ = false;
    }
    boost::asio::io_context::stop();
  }

  /// A proxy post function that collects count, queueing, and execution statistics for
  /// the given handler.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  void post(std::function<void()> handler, const std::string name);

  /// A proxy post function where the operation start is manually recorded. For example,
  /// this is useful for tracking the number of active outbound RPC calls.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param handle The stats handle returned by RecordStart() previously.
  void post(std::function<void()> handler, std::shared_ptr<StatsHandle> handle);

  /// A proxy post function that collects count, queueing, and execution statistics for
  /// the given handler.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  void dispatch(std::function<void()> handler, const std::string name);

  EventTracker &stats() const { return *event_stats_; };

 private:
  absl::Mutex mu_;
  bool is_running_;
  /// The event stats tracker to use to record asio handler stats to.
  std::shared_ptr<EventTracker> event_stats_;
};
