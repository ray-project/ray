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

#include "ray/common/event_stats.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <utility>

#include "ray/stats/metric.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/time.h"

namespace {

/// A helper for creating a snapshot view of the global stats.
/// This acquires a reader lock on the provided global stats, and creates a
/// lockless copy of the stats.
GlobalStats to_global_stats_view(std::shared_ptr<GuardedGlobalStats> stats) {
  absl::MutexLock lock(&(stats->mutex));
  return GlobalStats(stats->stats);
}

/// A helper for creating a snapshot view of the stats for a event.
/// This acquires a lock on the provided guarded event stats, and creates a
/// lockless copy of the stats.
EventStats to_event_stats_view(std::shared_ptr<GuardedEventStats> stats) {
  absl::MutexLock lock(&(stats->mutex));
  return EventStats(stats->stats);
}

/// Convert the duration in nanoseconds to a string of the format: X.YZms.
std::string to_ms_str(double duration_ns) {
  double duration_ms = duration_ns / std::pow(1000, 2);
  std::stringstream result;
  result << std::fixed << std::setprecision(2) << duration_ms << "ms";
  return result.str();
}

}  // namespace

std::shared_ptr<StatsHandle> EventTracker::RecordStart(
    std::string name,
    bool emit_metrics,
    const int64_t expected_queueing_delay_ns,
    const std::optional<std::string> &event_context_name) {
  auto stats = GetOrCreate(name);
  int64_t curr_count = 0;
  {
    absl::MutexLock lock(&(stats->mutex));
    ++stats->stats.cum_count;
    curr_count = ++stats->stats.curr_count;
  }

  if (emit_metrics) {
    ray::stats::STATS_operation_count.Record(1, event_context_name.value_or(name));
    ray::stats::STATS_operation_active_count.Record(curr_count,
                                                    event_context_name.value_or(name));
  }

  return std::make_shared<StatsHandle>(
      std::move(name),
      ray::current_time_ns() + expected_queueing_delay_ns,
      std::move(stats),
      global_stats_,
      emit_metrics,
      event_context_name);
}

void EventTracker::RecordEnd(std::shared_ptr<StatsHandle> handle) {
  RAY_CHECK(!handle->end_or_execution_recorded);
  absl::MutexLock lock(&(handle->handler_stats->mutex));
  const auto curr_count = --handle->handler_stats->stats.curr_count;
  const auto execution_time_ns = ray::current_time_ns() - handle->start_time;
  handle->handler_stats->stats.cum_execution_time += execution_time_ns;

  if (handle->emit_stats) {
    // Update event-specific stats.
    ray::stats::STATS_operation_run_time_ms.Record(
        execution_time_ns / 1000000, handle->context_name.value_or(handle->event_name));
    ray::stats::STATS_operation_active_count.Record(
        curr_count, handle->context_name.value_or(handle->event_name));
  }

  handle->end_or_execution_recorded = true;
}

void EventTracker::RecordExecution(const std::function<void()> &fn,
                                   std::shared_ptr<StatsHandle> handle) {
  RAY_CHECK(!handle->end_or_execution_recorded);
  int64_t start_execution = ray::current_time_ns();
  // Update running count
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    stats->stats.running_count++;
  }
  // Execute actual function.
  fn();
  int64_t end_execution = ray::current_time_ns();
  // Update execution time stats.
  const auto execution_time_ns = end_execution - start_execution;
  int64_t curr_count;
  const auto queue_time_ns = start_execution - handle->start_time;
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    // Event-specific execution stats.
    stats->stats.cum_execution_time += execution_time_ns;
    stats->stats.cum_queue_time += queue_time_ns;
    if (stats->stats.min_queue_time > queue_time_ns) {
      stats->stats.min_queue_time = queue_time_ns;
    }
    if (stats->stats.max_queue_time < queue_time_ns) {
      stats->stats.max_queue_time = queue_time_ns;
    }
    // Event-specific current count.
    curr_count = --stats->stats.curr_count;
    // Event-specific running count.
    stats->stats.running_count--;
  }

  if (handle->emit_stats) {
    // Update event-specific stats.
    ray::stats::STATS_operation_run_time_ms.Record(
        execution_time_ns / 1000000, handle->context_name.value_or(handle->event_name));
    ray::stats::STATS_operation_active_count.Record(
        curr_count, handle->context_name.value_or(handle->event_name));
    // Update global stats.
    ray::stats::STATS_operation_queue_time_ms.Record(
        queue_time_ns / 1000000, handle->context_name.value_or(handle->event_name));
  }

  {
    auto global_stats = handle->global_stats;
    absl::MutexLock lock(&(global_stats->mutex));
    // Global queue stats.
    global_stats->stats.cum_queue_time += queue_time_ns;
    if (global_stats->stats.min_queue_time > queue_time_ns) {
      global_stats->stats.min_queue_time = queue_time_ns;
    }
    if (global_stats->stats.max_queue_time < queue_time_ns) {
      global_stats->stats.max_queue_time = queue_time_ns;
    }
  }
  handle->end_or_execution_recorded = true;
}

std::shared_ptr<GuardedEventStats> EventTracker::GetOrCreate(const std::string &name) {
  // Get this event's stats.
  std::shared_ptr<GuardedEventStats> result;
  mutex_.ReaderLock();
  auto it = post_handler_stats_.find(name);
  if (it == post_handler_stats_.end()) {
    mutex_.ReaderUnlock();
    // Lock the table until we have added the entry. We use try_emplace and handle a
    // failed insertion in case the item was added before we acquire the writers lock;
    // this allows the common path, in which the handler already exists in the hash table,
    // to only require the readers lock.
    absl::WriterMutexLock lock(&mutex_);
    const auto pair =
        post_handler_stats_.try_emplace(name, std::make_shared<GuardedEventStats>());
    it = pair.first;
    result = it->second;
  } else {
    result = it->second;
    mutex_.ReaderUnlock();
  }
  return result;
}

GlobalStats EventTracker::get_global_stats() const {
  return to_global_stats_view(global_stats_);
}

// Testing only method
std::optional<EventStats> EventTracker::get_event_stats(
    const std::string &event_name) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto it = post_handler_stats_.find(event_name);
  if (it == post_handler_stats_.end()) {
    return {};
  }
  return to_event_stats_view(it->second);
}

// Logging only method
std::vector<std::pair<std::string, EventStats>> EventTracker::get_event_stats() const {
  // We lock the stats table while copying the table into a vector.
  absl::ReaderMutexLock lock(&mutex_);
  std::vector<std::pair<std::string, EventStats>> stats;
  stats.reserve(post_handler_stats_.size());
  std::transform(post_handler_stats_.begin(),
                 post_handler_stats_.end(),
                 std::back_inserter(stats),
                 [](const std::pair<std::string, std::shared_ptr<GuardedEventStats>> &p) {
                   return std::make_pair(p.first, to_event_stats_view(p.second));
                 });
  return stats;
}

std::string EventTracker::StatsString() const {
  if (!RayConfig::instance().event_stats()) {
    return "Stats collection disabled, turn on "
           "event_stats "
           "flag to enable event loop stats collection";
  }
  auto stats = get_event_stats();
  // Sort stats by cumulative count, outside of the table lock.
  sort(stats.begin(),
       stats.end(),
       [](const std::pair<std::string, EventStats> &a,
          const std::pair<std::string, EventStats> &b) {
         return a.second.cum_count > b.second.cum_count;
       });
  int64_t cum_count = 0;
  int64_t curr_count = 0;
  int64_t cum_execution_time = 0;
  std::stringstream event_stats_stream;
  for (const auto &entry : stats) {
    cum_count += entry.second.cum_count;
    curr_count += entry.second.curr_count;
    cum_execution_time += entry.second.cum_execution_time;
    event_stats_stream << "\n\t" << entry.first << " - " << entry.second.cum_count
                       << " total (" << entry.second.curr_count << " active";
    if (entry.second.running_count > 0) {
      event_stats_stream << ", " << entry.second.running_count << " running";
    }
    double cum_execution_time_d = static_cast<double>(entry.second.cum_execution_time);
    double cum_count_d = static_cast<double>(entry.second.cum_count);
    double cum_queue_time_d = static_cast<double>(entry.second.cum_queue_time);
    event_stats_stream
        << "), Execution time: mean = " << to_ms_str(cum_execution_time_d / cum_count_d)
        << ", total = " << to_ms_str(cum_execution_time_d)
        << ", Queueing time: mean = " << to_ms_str(cum_queue_time_d / cum_count_d)
        << ", max = " << to_ms_str(static_cast<double>(entry.second.max_queue_time))
        << ", min = " << to_ms_str(static_cast<double>(entry.second.min_queue_time))
        << ", total = " << to_ms_str(cum_queue_time_d);
  }
  const auto global_stats = get_global_stats();
  std::stringstream stats_stream;
  stats_stream << "\nGlobal stats: " << cum_count << " total (" << curr_count
               << " active)";
  stats_stream << "\nQueueing time: mean = "
               << to_ms_str(static_cast<double>(global_stats.cum_queue_time) /
                            static_cast<double>(cum_count))
               << ", max = "
               << to_ms_str(static_cast<double>(global_stats.max_queue_time))
               << ", min = "
               << to_ms_str(static_cast<double>(global_stats.min_queue_time))
               << ", total = "
               << to_ms_str(static_cast<double>(global_stats.cum_queue_time));
  stats_stream << "\nExecution time:  mean = "
               << to_ms_str(static_cast<double>(cum_execution_time) /
                            static_cast<double>(cum_count))
               << ", total = " << to_ms_str(static_cast<double>(cum_execution_time));
  stats_stream << "\nEvent stats:";
  stats_stream << event_stats_stream.rdbuf();
  return stats_stream.str();
}
