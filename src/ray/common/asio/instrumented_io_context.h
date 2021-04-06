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

/// Count, queueing, and execution statistics for an asio handler.
struct HandlerStats {
  // Counts.
  int64_t cum_count = 0;
  int64_t curr_count = 0;

  // Execution stats.
  int64_t cum_execution_time = 0;
};

/// Count and queueing statistics over all asio handlers.
struct GlobalStats {
  // Queue stats.
  int64_t cum_queue_time = 0;
  int64_t min_queue_time = std::numeric_limits<int64_t>::max();
  int64_t max_queue_time = -1;
};

/// A mutex wrapper around a handler stats struct.
struct GuardedHandlerStats {
  // Stats for some handler.
  HandlerStats stats;

  // The mutex protecting the reading and writing of these stats.
  // This mutex should be acquired with a reader lock before reading, and should be
  // acquired with a writer lock before writing.
  mutable absl::Mutex mutex;
};

/// A mutex wrapper around a handler stats struct.
struct GuardedGlobalStats {
  // Stats over all handlers.
  GlobalStats stats;

  // The mutex protecting the reading and writing of these stats.
  // This mutex should be acquired with a reader lock before reading, and should be
  // acquired with a writer lock before writing.
  mutable absl::Mutex mutex;
};

/// A proxy for boost::asio::io_context that collects statistics about posted handlers.
class instrumented_io_context : public boost::asio::io_context {
 public:
  /// Initializes the global stats struct after calling the base contructor.
  instrumented_io_context() : global_stats_(std::make_shared<GuardedGlobalStats>()) {}

  /// A proxy post function that collects count, queueing, and execution statistics for
  /// the given handler.
  ///
  /// \param handler The handler to be posted to the event loop.
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler. Defaults to UNKNOWN.
  void post(std::function<void()> handler, const std::string name = "UNKNOWN")
      LOCKS_EXCLUDED(mutex_);

  /// Increments the current and cumulative counts and returns the enqueueing time
  /// for this handler.
  /// The returned enqueueing time and handler stats pointer should be given to a
  /// subsequent RecordExecution() call. The handler stats is also returned in order to
  /// obviate the need for multiple handler stats table lookups, therefore reducing
  /// contention on the table lock.
  ///
  /// \param name A human-readable name to which collected stats will be associated.
  /// \return The time at which this handler was enqueued, in nanoseconds, and the
  ///  handler stats.
  std::pair<int64_t, std::shared_ptr<GuardedHandlerStats>> RecordStart(
      const std::string name);

  /// Increments the current and cumulative counts and returns the enqueueing time
  /// for this handler.
  /// The returned enqueueing time should be given to a subsequent RecordExecution() call.
  ///
  /// \param stats The handler stats.
  /// \return The time at which this handler was enqueued, in nanonseconds.
  static int64_t RecordStart(std::shared_ptr<GuardedHandlerStats> stats);

  /// Records stats about the provided function's execution. This version of the
  /// function exists in case there was no sensible RecordStart() call to be made.
  ///
  /// \param fn The function to execute and instrument.
  /// \param name The human-readable name for the handler.
  /// \return The stats for this handler.
  std::shared_ptr<GuardedHandlerStats> RecordExecution(std::function<void()> fn,
                                                       const std::string name);

  /// Records stats about the provided function's execution. The stats and enqueue_time
  /// arguments should be taken from a RecordStart("some_handler_name") call.
  ///
  /// \param fn The function to execute and instrument.
  /// \param stats The stats for the provided handler.
  /// \param global_stats The global stats for this event loop.
  /// \param enqueue_time The time at which this handler was enqueued for execution, in
  ///  nanoseconds.
  static void RecordExecution(std::function<void()> fn,
                              std::shared_ptr<GuardedHandlerStats> stats,
                              std::shared_ptr<GuardedGlobalStats> global_stats,
                              absl::optional<int64_t> enqueue_time = absl::nullopt);

  /// Returns a live guarded view of the global count, queueing, and execution statistics
  /// across all handlers.
  ///
  /// \return A live guarded view of the global handler stats.
  std::shared_ptr<GuardedGlobalStats> get_guarded_global_stats() const;

  /// Returns a live guarded view of the count, queueing, and execution statistics for the
  /// provided handler. If the handler stats doesn't yet exist, one is created.
  ///
  /// \param handler_name The name of the handler whose stats should be returned.
  /// \return A live guarded view of the handler's stats.
  std::shared_ptr<GuardedHandlerStats> get_guarded_handler_stats(
      const std::string &handler_name) LOCKS_EXCLUDED(mutex_);

  /// Returns a snapshot view of the global count, queueing, and execution statistics
  /// across all handlers.
  ///
  /// \return A snapshot view of the global handler stats.
  GlobalStats get_global_stats() const;

  /// Returns a snapshot view of the count, queueing, and execution statistics for the
  /// provided handler.
  ///
  /// \param handler_name The name of the handler whose stats should be returned.
  /// \return A snapshot view of the handler's stats.
  absl::optional<HandlerStats> get_handler_stats(const std::string &handler_name) const
      LOCKS_EXCLUDED(mutex_);

  /// Returns snapshot views of the count, queueing, and execution statistics for all
  /// handlers.
  ///
  /// \return A vector containing snapshot views of stats for all handlers.
  std::vector<std::pair<std::string, HandlerStats>> get_handler_stats() const
      LOCKS_EXCLUDED(mutex_);

  /// Builds and returns a statistics summary string. Used by the DebugString() of
  /// objects that used this io_context wrapper, such as the raylet and the core worker.
  ///
  /// \return A stats summary string, suitable for inclusion in an object's
  /// DebugString().
  std::string StatsString() const LOCKS_EXCLUDED(mutex_);

 private:
  using HandlerStatsTable =
      absl::flat_hash_map<std::string, std::shared_ptr<GuardedHandlerStats>>;
  /// Get an iterator to the stats for this handler if it exists, otherwise create the
  /// stats for this handler and return an iterator pointing to it.
  ///
  /// \param name A human-readable name for the handler, to be used for viewing stats
  /// for the provided handler.
  std::shared_ptr<GuardedHandlerStats> GetOrCreate(const std::string name);

  /// Global stats, across all handlers.
  std::shared_ptr<GuardedGlobalStats> global_stats_;

  /// Table of per-handler post stats.
  /// We use a std::shared_ptr value in order to ensure pointer stability.
  HandlerStatsTable post_handler_stats_ GUARDED_BY(mutex_);

  /// Protects access to the per-handler post stats table.
  mutable absl::Mutex mutex_;
};
