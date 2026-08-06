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

#include "ray/observability/task_event_populators.h"

#include <utility>

#include "ray/common/grpc_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

namespace {

rpc::events::TaskLifecycleEvent::TaskLogInfo TaskLogInfoToLifecycleEvent(
    const rpc::TaskLogInfo &src) {
  rpc::events::TaskLifecycleEvent::TaskLogInfo dest;
  if (src.has_stdout_file()) {
    dest.set_stdout_file(src.stdout_file());
  }
  if (src.has_stderr_file()) {
    dest.set_stderr_file(src.stderr_file());
  }
  if (src.has_stdout_start()) {
    dest.set_stdout_start(src.stdout_start());
  }
  if (src.has_stdout_end()) {
    dest.set_stdout_end(src.stdout_end());
  }
  if (src.has_stderr_start()) {
    dest.set_stderr_start(src.stderr_start());
  }
  if (src.has_stderr_end()) {
    dest.set_stderr_end(src.stderr_end());
  }
  return dest;
}

}  // namespace

void AppendTaskLifecycleUpdate(const TaskID &task_id,
                               const JobID &job_id,
                               int32_t task_attempt,
                               rpc::TaskStatus task_status,
                               int64_t timestamp,
                               const std::optional<TaskStateUpdate> &state_update,
                               rpc::events::TaskLifecycleEvent &lifecycle_event_data) {
  // Task identifier
  lifecycle_event_data.set_task_id(task_id.Binary());
  lifecycle_event_data.set_task_attempt(task_attempt);

  // Task state
  if (task_status != rpc::TaskStatus::NIL) {
    rpc::events::TaskLifecycleEvent::StateTransition state_transition;
    state_transition.set_state(task_status);
    *state_transition.mutable_timestamp() = AbslTimeNanosToProtoTimestamp(timestamp);
    *lifecycle_event_data.mutable_state_transitions()->Add() =
        std::move(state_transition);
  }

  lifecycle_event_data.set_job_id(job_id.Binary());

  // Task property updates
  if (!state_update.has_value()) {
    return;
  }

  if (state_update->error_info_.has_value()) {
    lifecycle_event_data.mutable_ray_error_info()->CopyFrom(*state_update->error_info_);
  }

  if (!state_update->actor_repr_name_.empty()) {
    lifecycle_event_data.set_actor_repr_name(state_update->actor_repr_name_);
  }

  if (state_update->node_id_.has_value()) {
    RAY_CHECK(task_status == rpc::TaskStatus::SUBMITTED_TO_WORKER)
            .WithField("TaskStatus", task_status)
        << "Node ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    lifecycle_event_data.set_node_id(state_update->node_id_->Binary());
  }

  if (state_update->worker_id_.has_value()) {
    RAY_CHECK(task_status == rpc::TaskStatus::SUBMITTED_TO_WORKER)
            .WithField("TaskStatus", task_status)
        << "Worker ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    lifecycle_event_data.set_worker_id(state_update->worker_id_->Binary());
  }

  if (state_update->pid_.has_value()) {
    lifecycle_event_data.set_worker_pid(state_update->pid_.value());
  }

  if (state_update->is_debugger_paused_.has_value()) {
    lifecycle_event_data.set_is_debugger_paused(
        state_update->is_debugger_paused_.value());
  }

  if (state_update->task_log_info_.has_value()) {
    *lifecycle_event_data.mutable_task_log_info() =
        TaskLogInfoToLifecycleEvent(state_update->task_log_info_.value());
  }
}

}  // namespace observability
}  // namespace ray
