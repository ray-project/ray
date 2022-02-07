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

/// A helper for converting a duration into a human readable string, such as "5.346 ms".
std::string to_human_readable(double duration) {
  static const std::array<std::string, 4> to_unit{{"ns", "us", "ms", "s"}};
  size_t idx = std::min(to_unit.size() - 1,
                        static_cast<size_t>(std::log(duration) / std::log(1000)));
  double new_duration = duration / std::pow(1000, idx);
  std::stringstream result;
  result << std::fixed << std::setprecision(3) << new_duration << " " << to_unit[idx];
  return result.str();
}

/// A helper for converting a duration into a human readable string, such as "5.346 ms".
std::string to_human_readable(int64_t duration) {
  return to_human_readable(static_cast<double>(duration));
}

}  // namespace

std::shared_ptr<StatsHandle> EventTracker::RecordStart(
    const std::string &name, int64_t expected_queueing_delay_ns) {
  auto stats = GetOrCreate(name);
  int64_t curr_count = 0;
  {
    absl::MutexLock lock(&(stats->mutex));
    stats->stats.cum_count++;
    curr_count = ++stats->stats.curr_count;
  }
  ray::stats::STATS_operation_count.Record(curr_count, name);
  ray::stats::STATS_operation_active_count.Record(curr_count, name);
  return std::make_shared<StatsHandle>(
      name, absl::GetCurrentTimeNanos() + expected_queueing_delay_ns, stats,
      global_stats_);
}

void EventTracker::RecordExecution(const std::function<void()> &fn,
                                   std::shared_ptr<StatsHandle> handle) {
  int64_t start_execution = absl::GetCurrentTimeNanos();
  // Update running count
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    stats->stats.running_count++;
  }
  // Execute actual function.
  fn();
  int64_t end_execution = absl::GetCurrentTimeNanos();
  // Update execution time stats.
  const auto execution_time_ns = end_execution - start_execution;
  // Update event-specific stats.
  ray::stats::STATS_operation_run_time_ms.Record(execution_time_ns / 1000000,
                                                 handle->event_name);
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    // Event-specific execution stats.
    stats->stats.cum_execution_time += execution_time_ns;
    // Event-specific current count.
    stats->stats.curr_count--;
    ray::stats::STATS_operation_active_count.Record(stats->stats.curr_count,
                                                    handle->event_name);
    // Event-specific running count.
    stats->stats.running_count--;
  }
  // Update global stats.
  const auto queue_time_ns = start_execution - handle->start_time;
  ray::stats::STATS_operation_queue_time_ms.Record(queue_time_ns / 1000000,
                                                   handle->event_name);
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
  handle->execution_recorded = true;
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
    if (pair.second) {
      it = pair.first;
    } else {
      it = post_handler_stats_.find(name);
      // If try_emplace failed to insert the item, the item is guaranteed to exist in
      // the table.
      RAY_CHECK(it != post_handler_stats_.end());
    }
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

absl::optional<EventStats> EventTracker::get_event_stats(
    const std::string &event_name) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto it = post_handler_stats_.find(event_name);
  if (it == post_handler_stats_.end()) {
    return {};
  }
  return to_event_stats_view(it->second);
}

std::vector<std::pair<std::string, EventStats>> EventTracker::get_event_stats() const {
  // We lock the stats table while copying the table into a vector.
  absl::ReaderMutexLock lock(&mutex_);
  std::vector<std::pair<std::string, EventStats>> stats;
  stats.reserve(post_handler_stats_.size());
  std::transform(post_handler_stats_.begin(), post_handler_stats_.end(),
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
  sort(stats.begin(), stats.end(),
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
    event_stats_stream << "), CPU time: mean = "
                       << to_human_readable(entry.second.cum_execution_time /
                                            static_cast<double>(entry.second.cum_count))
                       << ", total = "
                       << to_human_readable(entry.second.cum_execution_time);
  }
  const auto global_stats = get_global_stats();
  std::stringstream stats_stream;
  stats_stream << "\nGlobal stats: " << cum_count << " total (" << curr_count
               << " active)";
  stats_stream << "\nQueueing time: mean = "
               << to_human_readable(global_stats.cum_queue_time /
                                    static_cast<double>(cum_count))
               << ", max = " << to_human_readable(global_stats.max_queue_time)
               << ", min = " << to_human_readable(global_stats.min_queue_time)
               << ", total = " << to_human_readable(global_stats.cum_queue_time);
  stats_stream << "\nExecution time:  mean = "
               << to_human_readable(cum_execution_time / static_cast<double>(cum_count))
               << ", total = " << to_human_readable(cum_execution_time);
  stats_stream << "\nEvent stats:";
  stats_stream << event_stats_stream.rdbuf();
  return stats_stream.str();
}
