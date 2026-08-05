// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_task_event_recorder.h"

#include <algorithm>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/observability/task_event_instrumentation.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace observability {

RayTaskEventRecorder::RayTaskEventRecorder(
    rpc::EventAggregatorClient &event_aggregator_client,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    size_t max_buffer_size,
    std::string_view metric_source,
    ray::observability::MetricInterface &dropped_events_counter,
    const NodeID &node_id)
    : RayEventRecorderBase(event_aggregator_client,
                           std::move(periodical_runner),
                           max_buffer_size,
                           metric_source,
                           dropped_events_counter,
                           node_id),
      status_events_(max_buffer_size) {}

TaskAttemptId RayTaskEventRecorder::GetTaskAttemptOrDie(
    const std::unique_ptr<RayEventInterface> &event) {
  auto *task_event = dynamic_cast<TaskRayEventInterface *>(event.get());
  RAY_CHECK(task_event != nullptr)
      << "RayTaskEventRecorder received an event that is not a TaskRayEventInterface.";
  return task_event->GetTaskAttempt();
}

void RayTaskEventRecorder::AddDroppedEvents(size_t count) {
  num_dropped_events_unreported_ += count;
  instr::n_dropped += static_cast<int64_t>(count);
}

bool RayTaskEventRecorder::Enabled() {
  return RayConfig::instance().enable_ray_event() &&
         RayConfig::instance().enable_ray_task_event_recorder();
}

void RayTaskEventRecorder::AddOneEvent(std::unique_ptr<RayEventInterface> event) {
  TaskAttemptId attempt = GetTaskAttemptOrDie(event);
  if (event->GetEventType() == rpc::events::RayEvent::TASK_PROFILE_EVENT) {
    AddProfileEvent(std::move(event), attempt);
  } else {
    AddStatusEvent(std::move(event), attempt);
  }
}

void RayTaskEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  absl::MutexLock lock(&mutex_);
  if (!enabled_ || !Enabled()) {
    return;
  }
  for (auto &event : data_list) {
    AddOneEvent(std::move(event));
  }
}

void RayTaskEventRecorder::AddEvent(std::unique_ptr<RayEventInterface> data) {
  const int64_t instr_t0 = instr::NowNanos();
  {
    absl::MutexLock lock(&mutex_);
    const int64_t instr_t1 = instr::NowNanos();
    instr::ns_lockwait += instr_t1 - instr_t0;
    if (!enabled_ || !Enabled()) {
      return;
    }
    AddOneEvent(std::move(data));
  }
  instr::n_events += 1;
  instr::ns_add += instr::NowNanos() - instr_t0;
}

void RayTaskEventRecorder::AddStatusEvent(std::unique_ptr<RayEventInterface> event,
                                          const TaskAttemptId &attempt) {
  if (dropped_task_attempts_unreported_.count(attempt) != 0u) {
    // This task attempt has already been dropped, so drop this event too.
    AddDroppedEvents(1);
    return;
  }
  if (status_events_.full()) {
    // The ring is full: pushing will evict the oldest event, so record its attempt as
    // dropped before it is overwritten.
    const TaskAttemptId &evicted = status_events_.front().attempt;
    RAY_LOG_EVERY_N(WARNING, 100000)
        << "[RayTaskEventRecorder] Dropping task status events for task: "
        << evicted.first
        << ", set a higher value for "
           "RAY_task_events_max_num_status_events_buffer_on_worker("
        << RayConfig::instance().task_events_max_num_status_events_buffer_on_worker()
        << ") to avoid this.";
    dropped_task_attempts_unreported_.insert(evicted);
    AddDroppedEvents(1);
  }
  status_events_.push_back({attempt, std::move(event)});
}

void RayTaskEventRecorder::AddProfileEvent(std::unique_ptr<RayEventInterface> event,
                                           const TaskAttemptId &attempt) {
  auto profile_events_itr = profile_events_.find(attempt);
  if (profile_events_itr == profile_events_.end()) {
    auto inserted = profile_events_.insert(
        {attempt, std::vector<std::unique_ptr<RayEventInterface>>()});
    RAY_CHECK(inserted.second);
    profile_events_itr = inserted.first;
  }

  auto max_num_profile_event_per_task =
      RayConfig::instance().task_events_max_num_profile_events_per_task();
  auto max_profile_events_stored =
      RayConfig::instance().task_events_max_num_profile_events_buffer_on_worker();

  // If we store too many per task or too many per kind of event, we drop the new event.
  if ((max_num_profile_event_per_task >= 0 &&
       profile_events_itr->second.size() >=
           static_cast<size_t>(max_num_profile_event_per_task)) ||
      num_profile_events_buffered_ >= max_profile_events_stored) {
    AddDroppedEvents(1);
    // Data loss. We are dropping the newly reported profile event.
    // This will likely happen on a driver task since the driver has a fixed placeholder
    // driver task id and it could generate large number of profile events when submitting
    // many tasks.
    RAY_LOG_EVERY_N(WARNING, 100000)
        << "[RayTaskEventRecorder] Dropping profiling events for task: " << attempt.first
        << ", set a higher value for RAY_task_events_max_num_profile_events_per_task("
        << max_num_profile_event_per_task
        << "), or RAY_task_events_max_num_profile_events_buffer_on_worker ("
        << max_profile_events_stored << ") to avoid this.";
    return;
  }

  num_profile_events_buffered_++;
  profile_events_itr->second.push_back(std::move(event));
}

void RayTaskEventRecorder::TakeEventsToSend(
    std::list<std::unique_ptr<RayEventInterface>> *events,
    absl::flat_hash_set<TaskAttemptId> *dropped_to_send) {
  const int64_t max_dropped_task_attempts =
      RayConfig::instance().task_events_dropped_task_attempt_batch_size();
  while ((max_dropped_task_attempts < 0 ||
          dropped_to_send->size() < static_cast<size_t>(max_dropped_task_attempts)) &&
         !dropped_task_attempts_unreported_.empty()) {
    auto itr = dropped_task_attempts_unreported_.begin();
    dropped_to_send->insert(*itr);
    dropped_task_attempts_unreported_.erase(itr);
  }

  const size_t batch_size = RayConfig::instance().task_events_send_batch_size();
  size_t num_skipped = 0;

  size_t num_status_events_to_take = std::min(batch_size, status_events_.size());
  for (size_t i = 0; i < num_status_events_to_take; i++) {
    BufferedStatusEvent buffered = std::move(status_events_.front());
    status_events_.pop_front();
    if (dropped_to_send->contains(buffered.attempt)) {
      num_skipped++;
      continue;
    }
    events->push_back(std::move(buffered.event));
  }

  size_t num_profile_events_taken = 0;
  while (!profile_events_.empty() && num_profile_events_taken < batch_size) {
    auto itr = profile_events_.begin();
    std::vector<std::unique_ptr<RayEventInterface>> &profile_vec = itr->second;
    size_t num_to_take =
        std::min(batch_size - num_profile_events_taken, profile_vec.size());
    if (dropped_to_send->contains(itr->first)) {
      num_skipped += num_to_take;
    } else {
      for (size_t i = 0; i < num_to_take; i++) {
        events->push_back(std::move(profile_vec[i]));
      }
    }
    profile_vec.erase(profile_vec.begin(), profile_vec.begin() + num_to_take);
    num_profile_events_taken += num_to_take;
    num_profile_events_buffered_ -= num_to_take;
    if (profile_vec.empty()) {
      profile_events_.erase(itr);
    }
  }

  AddDroppedEvents(num_skipped);
}

void RayTaskEventRecorder::ExportEvents() {
  const int64_t instr_export_t0 = instr::NowNanos();
  int64_t instr_locked_ns = 0;
  std::list<std::unique_ptr<RayEventInterface>> events;
  absl::flat_hash_set<TaskAttemptId> dropped_to_send;
  size_t num_dropped_to_report = 0;
  {
    absl::MutexLock lock(&mutex_);
    const int64_t instr_locked_t0 = instr::NowNanos();
    if (status_events_.empty() && profile_events_.empty() &&
        dropped_task_attempts_unreported_.empty() &&
        num_dropped_events_unreported_ == 0) {
      return;
    }
    // Skip if there's already an in-flight gRPC call to avoid overlapping requests.
    // (claude) Claiming the in-flight slot here, under the lock that guards the buffers,
    // is what keeps a single export in flight: grouping, serialization and the send below
    // run unlocked so they never block AddEvents on the task's call path.
    if (grpc_in_progress_.exchange(true)) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "Previous RayTaskEventRecorder export in progress: new events will be "
             "exported once previous export completes.";
      return;
    }
    TakeEventsToSend(&events, &dropped_to_send);
    num_dropped_to_report = num_dropped_events_unreported_;
    num_dropped_events_unreported_ = 0;
    instr_locked_ns = instr::NowNanos() - instr_locked_t0;
  }

  // (claude) Reported here rather than per dropped event: the tag map allocates, and the
  // drop path runs under mutex_ on the task's call path.
  if (num_dropped_to_report > 0) {
    dropped_events_counter_.Record(static_cast<double>(num_dropped_to_report),
                                   {{"Source", std::string(metric_source_)}});
  }

  rpc::events::AddEventsRequest request;
  rpc::events::RayEventsData *data = request.mutable_events_data();
  GroupAndSerializeEvents(std::move(events), data);

  rpc::events::TaskEventsMetadata *metadata = data->mutable_task_events_metadata();
  for (const auto &attempt : dropped_to_send) {
    rpc::TaskAttempt *rpc_attempt = metadata->add_dropped_task_attempts();
    rpc_attempt->set_task_id(attempt.first.Binary());
    rpc_attempt->set_attempt_number(attempt.second);
  }

  if (data->events_size() == 0 && metadata->dropped_task_attempts_size() == 0) {
    MarkGrpcDone();
    instr::n_exports += 1;
    instr::ns_export_locked += instr_locked_ns;
    instr::ns_export_total += instr::NowNanos() - instr_export_t0;
    instr::MaybeReport(10);
    return;
  }

  SendRequest(std::move(request));

  instr::n_exports += 1;
  instr::ns_export_locked += instr_locked_ns;
  instr::ns_export_total += instr::NowNanos() - instr_export_t0;
  instr::MaybeReport(10);
}

}  // namespace observability
}  // namespace ray
