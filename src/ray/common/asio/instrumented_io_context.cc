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

/// C++11 helper for generalized lambda capture.
template <typename T, typename F>
class capture_impl {
  T x;
  F f;

 public:
  capture_impl(T &&x, F &&f) : x{std::forward<T>(x)}, f{std::forward<F>(f)} {}

  template <typename... Ts>
  auto operator()(Ts &&... args) -> decltype(f(x, std::forward<Ts>(args)...)) {
    return f(x, std::forward<Ts>(args)...);
  }

  template <typename... Ts>
  auto operator()(Ts &&... args) const -> decltype(f(x, std::forward<Ts>(args)...)) {
    return f(x, std::forward<Ts>(args)...);
  }
};

template <typename T, typename F>
capture_impl<T, F> capture(T &&x, F &&f) {
  return capture_impl<T, F>(std::forward<T>(x), std::forward<F>(f));
}

void instrumented_io_context::post(std::function<void()> handler,
                                   const std::string &name) {
  if (!RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    return boost::asio::io_context::post(std::move(handler));
  }
  // Get this handler's stats.
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
        post_handler_stats_.try_emplace(name, std::make_shared<GuardedHandlerStats>());
    if (pair.second) {
      it = pair.first;
    } else {
      it = post_handler_stats_.find(name);
      // If try_emplace failed to insert the item, the item is guaranteed to exist in
      // the table.
      RAY_CHECK(it != post_handler_stats_.end());
    }
  } else {
    mutex_.ReaderUnlock();
  }
  {
    absl::MutexLock lock(&(it->second->mutex));
    it->second->stats.cum_count++;
    it->second->stats.curr_count++;
  }
  int64_t start = absl::GetCurrentTimeNanos();
  // References are only invalidated upon deletion of the corresponding item from the
  // table, which we won't do until this io_context is deleted. Provided that
  // GuardedHandlerStats synchronizes internal access, we can concurrently write to these
  // stats from multiple threads without acquiring a table-level readers lock in the
  // callback.
  auto &stats = it->second;
  // TODO(Clark): Use generalized lambda capture to move `handler` into the lambda once
  // we move to C++14.
  auto wrapped_handler = capture(
      std::move(handler), [this, name, start, stats](std::function<void()> handler) {
        int64_t start_execution = absl::GetCurrentTimeNanos();
        // Execute actual handler.
        handler();
        int64_t end_execution = absl::GetCurrentTimeNanos();
        // Update queue and execution time stats.
        const auto queue_time_ns = start_execution - start;
        const auto execution_time_ns = end_execution - start_execution;
        // Update handler-specific stats.
        {
          absl::MutexLock lock(&(stats->mutex));
          // Handler-specific execution stats.
          stats->stats.cum_execution_time += execution_time_ns;
          // Handler-specific current count.
          stats->stats.curr_count--;
        }
        // Update global stats.
        {
          absl::MutexLock lock(&(global_stats_.mutex));
          // Global queue stats.
          global_stats_.stats.cum_queue_time += queue_time_ns;
          if (global_stats_.stats.min_queue_time > queue_time_ns) {
            global_stats_.stats.min_queue_time = queue_time_ns;
          }
          if (global_stats_.stats.max_queue_time < queue_time_ns) {
            global_stats_.stats.max_queue_time = queue_time_ns;
          }
        }
      });
  boost::asio::io_context::post(std::move(wrapped_handler));
}

/// A helper for creating a snapshot view of the global stats.
/// This acquires a reader lock on the provided global stats, and creates a
/// lockless copy of the stats.
inline GlobalStats to_global_stats_view(const GuardedGlobalStats &stats) {
  absl::MutexLock lock(&(stats.mutex));
  return GlobalStats(stats.stats);
}

GlobalStats instrumented_io_context::get_global_stats() const {
  return to_global_stats_view(global_stats_);
}

/// A helper for creating a snapshot view of the stats for a handler.
/// This acquires a reader lock on the provided guarded handler stats, and creates a
/// lockless copy of the stats.
inline HandlerStats to_handler_stats_view(const GuardedHandlerStats &stats) {
  absl::MutexLock lock(&(stats.mutex));
  return HandlerStats(stats.stats);
}

absl::optional<HandlerStats> instrumented_io_context::get_handler_stats(
    const std::string &handler_name) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto it = post_handler_stats_.find(handler_name);
  if (it == post_handler_stats_.end()) {
    return {};
  }
  return to_handler_stats_view(*it->second);
}

std::vector<std::pair<std::string, HandlerStats>>
instrumented_io_context::get_handler_stats() const {
  // We lock the stats table while copying the table into a vector.
  absl::ReaderMutexLock lock(&mutex_);
  std::vector<std::pair<std::string, HandlerStats>> stats;
  stats.reserve(post_handler_stats_.size());
  std::transform(
      post_handler_stats_.begin(), post_handler_stats_.end(), std::back_inserter(stats),
      [](const std::pair<std::string, std::shared_ptr<GuardedHandlerStats>> &p) {
        return std::make_pair(p.first, to_handler_stats_view(*p.second));
      });
  return stats;
}

inline std::string to_human_readable(double duration) {
  static const std::array<std::string, 4> to_unit{{"ns", "us", "ms", "s"}};
  size_t idx = std::min(to_unit.size() - 1,
                        static_cast<size_t>(std::log(duration) / std::log(1000)));
  double new_duration = duration / std::pow(1000, idx);
  std::stringstream result;
  result << std::fixed << std::setprecision(3) << new_duration << " " << to_unit[idx];
  return result.str();
}

inline std::string to_human_readable(int64_t duration) {
  return to_human_readable(static_cast<double>(duration));
}

std::string instrumented_io_context::StatsString() const {
  if (!RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    return "Stats collection disabled, turn on asio_event_loop_stats_collection_enabled "
           "flag to enable event loop stats collection";
  }
  auto stats = get_handler_stats();
  // Sort stats by cumulative count, outside of the table lock.
  sort(stats.begin(), stats.end(),
       [](const std::pair<std::string, HandlerStats> &a,
          const std::pair<std::string, HandlerStats> &b) {
         return a.second.cum_count > b.second.cum_count;
       });
  int64_t cum_count = 0;
  int64_t curr_count = 0;
  int64_t cum_execution_time = 0;
  std::stringstream handler_stats_stream;
  for (const auto &entry : stats) {
    cum_count += entry.second.cum_count;
    curr_count += entry.second.curr_count;
    cum_execution_time += entry.second.cum_execution_time;
    handler_stats_stream << "\n\t" << entry.first << " - " << entry.second.cum_count
                         << " total (" << entry.second.curr_count
                         << " active), CPU time: mean = "
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
  stats_stream << "\nHandler stats:";
  stats_stream << handler_stats_stream.rdbuf();
  return stats_stream.str();
}
