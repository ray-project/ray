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

#include <memory>
#include <string>

#include "ray/core_worker/context.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/observability/ray_event_recorder_interface.h"

namespace ray {
namespace core {

namespace worker {

/// A wrapper that wraps a TaskProfileEvent, will be wrapped by a Cython class.
///
/// This class's lifetime measures the duration of a target event. Upon destruction,
/// this class will be added to TaskEventBuffer or RayEventRecorder to be sent.
class ProfileEvent {
 public:
  /// Constructor for ProfileEvent.
  /// \param task_event_buffer Pointer to the TaskEventBuffer, can be nullptr.
  /// \param ray_event_recorder Pointer to the RayEventRecorder, can be nullptr.
  /// \param worker_context The worker context.
  /// \param node_ip_address The node IP address.
  /// \param event_type The event type name.
  /// \param session_name The session name (only used when ray_event_recorder is set).
  /// \param node_id The node ID (only used when ray_event_recorder is set).
  ProfileEvent(TaskEventBuffer *task_event_buffer,
               observability::RayEventRecorderInterface *ray_event_recorder,
               WorkerContext &worker_context,
               const std::string &node_ip_address,
               const std::string &event_type,
               const std::string &session_name,
               const NodeID &node_id);

  ProfileEvent(const ProfileEvent &) = delete;
  ProfileEvent &operator=(const ProfileEvent &) = delete;

  // Set the end time for the event and add it to the profiler.
  ~ProfileEvent();

  // Set extra metadata for the event, which could change during the event.
  void SetExtraData(const std::string &extra_data);

 private:
  // Pointer to the TaskEventBuffer (can be nullptr).
  TaskEventBuffer *task_event_buffer_;

  // Pointer to the RayEventRecorder (can be nullptr).
  observability::RayEventRecorderInterface *ray_event_recorder_;

  // The underlying event (for TaskEventBuffer path).
  std::unique_ptr<TaskProfileEvent> event_ = nullptr;

  // Profile event data (for RayEventRecorder path).
  TaskID task_id_;
  JobID job_id_;
  int32_t attempt_number_ = 0;
  std::string component_type_;
  std::string component_id_;
  std::string node_ip_address_;
  std::string event_name_;
  int64_t start_time_ = 0;
  std::string extra_data_;
  std::string session_name_;
  NodeID node_id_;

  // Whether to use RayEventRecorder path.
  bool use_ray_event_recorder_ = false;
};

}  // namespace worker

}  // namespace core
}  // namespace ray
