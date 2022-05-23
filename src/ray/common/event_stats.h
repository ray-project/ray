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
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

/// Count, queueing, and execution statistics for a given event.
struct EventStats {
  // Counts.
  int64_t cum_count = 0;
  int64_t curr_count = 0;

  // Execution stats.
  int64_t cum_execution_time = 0;
  int64_t running_count = 0;
};

/// Count and queueing statistics over all events.
struct GlobalStats {
  // Queue stats.
  int64_t cum_queue_time = 0;
  int64_t min_queue_time = std::numeric_limits<int64_t>::max();
  int64_t max_queue_time = -1;
};

/// A mutex wrapper around a handler stats struct.
struct GuardedEventStats {
  // Stats for some handler.
  EventStats stats GUARDED_BY(mutex);

  // The mutex protecting the reading and writing of these stats.
  // This mutex should be acquired with a reader lock before reading, and should be
  // acquired with a writer lock before writing.
  mutable absl::Mutex mutex;
};

/// A mutex wrapper around a handler stats struct.
struct GuardedGlobalStats {
  // Stats over all handlers.
  GlobalStats stats GUARDED_BY(mutex);

  // The mutex protecting the reading and writing of these stats.
  // This mutex should be acquired with a reader lock before reading, and should be
  // acquired with a writer lock before writing.
  mutable absl::Mutex mutex;
};

/// An opaque stats handle, used to manually instrument event handlers.
struct StatsHandle {
  std::string event_name;
  int64_t start_time;
  std::shared_ptr<GuardedEventStats> handler_stats;
  std::shared_ptr<GuardedGlobalStats> global_stats;
  std::atomic<bool> execution_recorded;

  StatsHandle(std::string event_name_,
              int64_t start_time_,
              std::shared_ptr<GuardedEventStats> handler_stats_,
              std::shared_ptr<GuardedGlobalStats> global_stats_)
      : event_name(std::move(event_name_)),
        start_time(start_time_),
        handler_stats(std::move(handler_stats_)),
        global_stats(std::move(global_stats_)),
        execution_recorded(false) {}

  void ZeroAccumulatedQueuingDelay() { start_time = absl::GetCurrentTimeNanos(); }

  ~StatsHandle() {
    if (!execution_recorded) {
      // If handler execution was never recorded, we need to clean up some queueing
      // stats in order to prevent those stats from leaking.
      absl::MutexLock lock(&(handler_stats->mutex));
      handler_stats->stats.curr_count--;
    }
  }
};

class EventTracker {
 public:
  /// Initializes the global stats struct after calling the base constructor.
  EventTracker() : global_stats_(std::make_shared<GuardedGlobalStats>()) {}

  /// Sets the queueing start time, increments the current and cumulative counts and
  /// returns an opaque handle for these stats. This is used in conjunction with
  /// RecordExecution() to manually instrument an event.
  ///
  /// The returned opaque stats handle should be given to a subsequent RecordExecution()
  /// call.
  ///
  /// \param name A human-readable name to which collected stats will be associated.
  /// \param expected_queueing_delay_ns How much to pad the observed queueing start time,
  ///  in nanoseconds.
  /// \return An opaque stats handle, to be given to RecordExecution().
  std::shared_ptr<StatsHandle> RecordStart(const std::string &name,
                                           int64_t expected_queueing_delay_ns = 0);

  /// Records stats about the provided function's execution. This is used in conjunction
  /// with RecordStart() to manually instrument an event loop handler that doesn't call
  /// .post().
  ///
  /// \param fn The function to execute and instrument.
  /// \param handle An opaque stats handle returned by RecordStart().
  static void RecordExecution(const std::function<void()> &fn,
                              std::shared_ptr<StatsHandle> handle);

  /// Returns a snapshot view of the global count, queueing, and execution statistics
  /// across all handlers.
  ///
  /// \return A snapshot view of the global handler stats.
  GlobalStats get_global_stats() const;

  /// Returns a snapshot view of the count, queueing, and execution statistics for the
  /// provided event type.
  ///
  /// \param event_name The name of the event whose stats should be returned.
  /// \return A snapshot view of the event's stats.
  absl::optional<EventStats> get_event_stats(const std::string &event_name) const
      LOCKS_EXCLUDED(mutex_);

  /// Returns snapshot views of the count, queueing, and execution statistics for all
  /// events.
  ///
  /// \return A vector containing snapshot views of stats for all events.
  std::vector<std::pair<std::string, EventStats>> get_event_stats() const
      LOCKS_EXCLUDED(mutex_);

  /// Builds and returns a statistics summary string. Used by the DebugString() of
  /// objects that used this io_context wrapper, such as the raylet and the core worker.
  ///
  /// \return A stats summary string, suitable for inclusion in an object's
  /// DebugString().
  std::string StatsString() const LOCKS_EXCLUDED(mutex_);

 private:
  using EventStatsTable =
      absl::flat_hash_map<std::string, std::shared_ptr<GuardedEventStats>>;
  /// Get the mutex-guarded stats for this event if it exists, otherwise create the
  /// stats for this handler and return an iterator pointing to it.
  ///
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  std::shared_ptr<GuardedEventStats> GetOrCreate(const std::string &name);

  /// Global stats, across all handlers.
  std::shared_ptr<GuardedGlobalStats> global_stats_;

  /// Table of per-handler post stats.
  /// We use a std::shared_ptr value in order to ensure pointer stability.
  EventStatsTable post_handler_stats_ GUARDED_BY(mutex_);

  /// Protects access to the per-handler post stats table.
  mutable absl::Mutex mutex_;
};
