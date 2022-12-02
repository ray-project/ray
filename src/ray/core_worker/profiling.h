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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/gcs/gcs_client/gcs_client.h"

namespace ray {
namespace core {

namespace worker {

class Profiler {
 public:
  Profiler(WorkerContext &worker_context,
           const std::string &node_ip_address,
           instrumented_io_context &io_service,
           const std::shared_ptr<gcs::GcsClient> &gcs_client);

  // Add an event to the queue to be flushed periodically.
  void AddEvent(const rpc::ProfileTableData::ProfileEvent &event) LOCKS_EXCLUDED(mutex_);

 private:
  // Flush all of the events that have been added since last flush to the GCS.
  void FlushEvents() LOCKS_EXCLUDED(mutex_);

  // Mutex guarding rpc_profile_data_.
  absl::Mutex mutex_;

  // ASIO IO service event loop. Must be started by the caller.
  instrumented_io_context &io_service_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  // RPC message containing profiling data. Holds the queue of profile events
  // until they are flushed.
  std::shared_ptr<rpc::ProfileTableData> rpc_profile_data_ GUARDED_BY(mutex_);

  /// Whether a profile flush is already in progress.
  bool profile_flush_active_ GUARDED_BY(mutex_) = false;

  // Client to the GCS used to push profile events to it.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
};

class ProfileEvent {
 public:
  ProfileEvent(const std::shared_ptr<Profiler> &profiler, const std::string &event_type);

  ProfileEvent(std::shared_ptr<TaskEventBuffer> task_event_buffer,
               const std::string &event_name,
               TaskID task_id,
               uint64_t attempt_number,
               const std::string &worker_type,
               const std::string &worker_id,
               const std::string &node_ip_address);

  // Set the end time for the event and add it to the profiler.
  ~ProfileEvent() {
    if (use_task_event_) {
      rpc::TaskEvents task_events;
      task_events.set_task_id(task_id_.Binary());
      task_events.set_attempt_number(attempt_number_);

      auto profile_events = task_events.mutable_profile_events();
      profile_events->set_component_type(component_type_);
      profile_events->set_component_id(component_id_);
      profile_events->set_node_ip_address(node_ip_address_);

      event_.set_end_time(absl::GetCurrentTimeNanos());
      auto event = profile_events->add_events();
      event->Swap(&event_);

      // Add task event to the task event buffer
      task_event_buffer_->AddTaskEvents(std::move(task_events));
      return;
    }
    rpc_event_.set_end_time(absl::GetCurrentTimeNanos() / 1e9);
    profiler_->AddEvent(rpc_event_);
  }

  // Set extra metadata for the event, which could change during the event.
  void SetExtraData(const std::string &extra_data) {
    if (use_task_event_) {
      event_.set_extra_data(extra_data);
    } else {
      rpc_event_.set_extra_data(extra_data);
    }
  }

 private:
  // shared_ptr to the profiler that this event will be added to when it is destructed.
  std::shared_ptr<Profiler> profiler_;

  // Underlying proto data structure that holds the event data.
  rpc::ProfileTableData::ProfileEvent rpc_event_;

  // TODO(rickyx): Refactor to use task events for profiling as default.
  // Flag if profiling should use the task event buffer
  bool use_task_event_ = false;

  // Shared pointer to the event buffer.
  std::shared_ptr<TaskEventBuffer> task_event_buffer_;

  // Underlying proto data structure that holds the event data.
  rpc::ProfileEventEntry event_;

  // Task ID
  TaskID task_id_;

  uint64_t attempt_number_;

  const std::string component_type_;
  const std::string component_id_;
  const std::string node_ip_address_;
};

}  // namespace worker

}  // namespace core
}  // namespace ray
