// Copyright 2025 The Ray Authors.
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

#include <boost/circular_buffer.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/observability/ray_event_recorder_base.h"
#include "ray/observability/task_ray_event_interface.h"

namespace ray {
namespace observability {

// RayTaskEventRecorder is the task-event-specific RayEventRecorderInterface impl. Unlike
// the generic RayEventRecorder (a single FIFO drop-oldest buffer), it preserves the
// drop semantics of the legacy task event buffer (see core_worker/task_event_buffer):
//
//   - Status events (definition + lifecycle) live in a bounded ring. On overflow the
//     evicted event's task attempt is recorded as dropped, and later events for
//     an already-dropped attempt are dropped too. Dropped attempts are reported
//     via RayEventsData.task_events_metadata.dropped_task_attempts. For a task attempt,
//     either all events are sent out or none is sent out.
//   - Profile events are stored per attempt with a per-task cap and a global cap; on
//     overflow the newest profile event is dropped (counted as a dropped-event metric)
//     Dropped profile events are not reported. They are metric only.
//
// Events passed to AddEvents must implement TaskRayEventInterface.
//
// This class is thread safe.
class RayTaskEventRecorder : public RayEventRecorderBase {
 public:
  RayTaskEventRecorder(rpc::EventAggregatorClient &event_aggregator_client,
                       std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
                       size_t max_buffer_size,
                       std::string_view metric_source,
                       ray::observability::MetricInterface &dropped_events_counter,
                       const NodeID &node_id);

  // Add a vector of task events to the internal buffers. Each event must implement
  // TaskRayEventInterface.
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list) override;

 private:
  void ExportEvents() override;

  // Add a status (definition / lifecycle) event to the bounded ring. On overflow the
  // oldest event is evicted and its attempt added to dropped_task_attempts_unreported_;
  // an event whose attempt was already dropped is dropped.
  void AddStatusEvent(std::unique_ptr<RayEventInterface> event,
                      const TaskAttemptId &attempt) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Add a profile event to the per-attempt store, dropping the newest on a per-task or
  // global cap overflow.
  void AddProfileEvent(std::unique_ptr<RayEventInterface> event,
                       const TaskAttemptId &attempt)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Read the task attempt of an event; the event must implement TaskRayEventInterface.
  static TaskAttemptId GetTaskAttemptOrDie(
      const std::unique_ptr<RayEventInterface> &event);

  // Record `count` dropped events to the metric.
  void RecordDropped(size_t count) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Bounded ring of status events (definition + lifecycle). On overflow the oldest is
  // evicted and its attempt is recorded in dropped_task_attempts_unreported_.
  boost::circular_buffer<std::unique_ptr<RayEventInterface>> status_events_
      ABSL_GUARDED_BY(mutex_);
  // Task attempts whose status events were dropped and not yet reported.
  absl::flat_hash_set<TaskAttemptId> dropped_task_attempts_unreported_
      ABSL_GUARDED_BY(mutex_);
  // Profile events keyed by task attempt; capped per attempt and globally (drop-newest).
  absl::flat_hash_map<TaskAttemptId, std::vector<std::unique_ptr<RayEventInterface>>>
      profile_events_ ABSL_GUARDED_BY(mutex_);
  // Total profile events buffered across all attempts (for the global cap).
  size_t num_profile_events_buffered_ ABSL_GUARDED_BY(mutex_) = 0;
};

}  // namespace observability
}  // namespace ray
