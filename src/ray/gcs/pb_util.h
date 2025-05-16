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
#include <utility>

#include "absl/time/time.h"
#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/export_api/export_task_event.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

using ContextCase = rpc::ActorDeathCause::ContextCase;
// Forward declaration.
std::string GenErrorMessageFromDeathCause(const rpc::ActorDeathCause &death_cause);

/// Helper function to produce job table data (for newly created job or updated job).
///
/// \param job_id The ID of job that needs to be registered or updated.
/// \param is_dead Whether the driver of this job is dead.
/// \param timestamp The UNIX timestamp corresponding to this event.
/// \param driver_address Address of the driver that started this job.
/// \param driver_pid Process ID of the driver running this job.
/// \param entrypoint The entrypoint name of the job.
/// \param job_config The config of this job.
/// \return The job table data created by this method.
inline std::shared_ptr<ray::rpc::JobTableData> CreateJobTableData(
    const ray::JobID &job_id,
    bool is_dead,
    const ray::rpc::Address &driver_address,
    int64_t driver_pid,
    const std::string &entrypoint,
    const ray::rpc::JobConfig &job_config = {}) {
  auto job_info_ptr = std::make_shared<ray::rpc::JobTableData>();
  job_info_ptr->set_job_id(job_id.Binary());
  job_info_ptr->set_is_dead(is_dead);
  *job_info_ptr->mutable_driver_address() = driver_address;
  job_info_ptr->set_driver_ip_address(driver_address.ip_address());
  job_info_ptr->set_driver_pid(driver_pid);
  job_info_ptr->set_entrypoint(entrypoint);
  *job_info_ptr->mutable_config() = job_config;
  return job_info_ptr;
}

/// Helper function to produce error table data.
std::shared_ptr<ray::rpc::ErrorTableData> CreateErrorTableData(
    const std::string &error_type,
    const std::string &error_msg,
    absl::Time timestamp,
    const JobID &job_id = JobID::Nil());

/// Helper function to produce worker failure data.
inline std::shared_ptr<ray::rpc::WorkerTableData> CreateWorkerFailureData(
    const WorkerID &worker_id,
    const NodeID &node_id,
    const std::string &ip_address,
    int64_t timestamp,
    rpc::WorkerExitType disconnect_type,
    const std::string &disconnect_detail,
    int pid,
    const rpc::RayException *creation_task_exception = nullptr) {
  auto worker_failure_info_ptr = std::make_shared<ray::rpc::WorkerTableData>();
  // Only report the worker id + delta (new data upon worker failures).
  // GCS will merge the data with original worker data.
  worker_failure_info_ptr->mutable_worker_address()->set_worker_id(worker_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_raylet_id(node_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_ip_address(ip_address);
  worker_failure_info_ptr->set_timestamp(timestamp);
  worker_failure_info_ptr->set_exit_type(disconnect_type);
  worker_failure_info_ptr->set_exit_detail(disconnect_detail);
  worker_failure_info_ptr->set_end_time_ms(current_sys_time_ms());
  if (creation_task_exception != nullptr) {
    // this pointer will be freed by protobuf internal codes
    auto copied_data = new rpc::RayException(*creation_task_exception);
    worker_failure_info_ptr->set_allocated_creation_task_exception(copied_data);
  }
  return worker_failure_info_ptr;
}

/// Get actor creation task exception from ActorDeathCause.
/// Returns nullptr if actor isn't dead due to creation task failure.
inline const rpc::RayException *GetCreationTaskExceptionFromDeathCause(
    const rpc::ActorDeathCause *death_cause) {
  if (death_cause == nullptr ||
      death_cause->context_case() != ContextCase::kCreationTaskFailureContext) {
    return nullptr;
  }
  return &(death_cause->creation_task_failure_context());
}

inline const std::string &GetActorDeathCauseString(
    const rpc::ActorDeathCause &death_cause) {
  static absl::flat_hash_map<ContextCase, std::string> death_cause_string{
      {ContextCase::CONTEXT_NOT_SET, "CONTEXT_NOT_SET"},
      {ContextCase::kRuntimeEnvFailedContext, "RuntimeEnvFailedContext"},
      {ContextCase::kCreationTaskFailureContext, "CreationTaskFailureContext"},
      {ContextCase::kActorUnschedulableContext, "ActorUnschedulableContext"},
      {ContextCase::kActorDiedErrorContext, "ActorDiedErrorContext"},
      {ContextCase::kOomContext, "OOMContext"}};
  auto it = death_cause_string.find(death_cause.context_case());
  RAY_CHECK(it != death_cause_string.end())
      << "Given death cause case " << death_cause.context_case() << " doesn't exist.";
  return it->second;
}

/// Get the error information from the actor death cause.
///
/// \param[in] death_cause The rpc message that contains the actos death information.
/// \return RayErrorInfo that has propagated death cause.
inline rpc::RayErrorInfo GetErrorInfoFromActorDeathCause(
    const rpc::ActorDeathCause &death_cause) {
  rpc::RayErrorInfo error_info;
  switch (death_cause.context_case()) {
  case ContextCase::kActorDiedErrorContext:
  case ContextCase::kCreationTaskFailureContext:
    error_info.mutable_actor_died_error()->CopyFrom(death_cause);
    error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
    break;
  case ContextCase::kRuntimeEnvFailedContext:
    error_info.mutable_runtime_env_setup_failed_error()->CopyFrom(
        death_cause.runtime_env_failed_context());
    error_info.set_error_type(rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED);
    break;
  case ContextCase::kActorUnschedulableContext:
    error_info.set_error_type(rpc::ErrorType::ACTOR_UNSCHEDULABLE_ERROR);
    break;
  case ContextCase::kOomContext:
    error_info.mutable_actor_died_error()->CopyFrom(death_cause);
    error_info.set_error_type(rpc::ErrorType::OUT_OF_MEMORY);
    break;
  default:
    RAY_CHECK(death_cause.context_case() == ContextCase::CONTEXT_NOT_SET);
    error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
  }
  error_info.set_error_message(GenErrorMessageFromDeathCause(death_cause));
  return error_info;
}

/// Generate object error type from ActorDeathCause.
inline std::string GenErrorMessageFromDeathCause(
    const rpc::ActorDeathCause &death_cause) {
  if (death_cause.context_case() == ContextCase::kCreationTaskFailureContext) {
    return death_cause.creation_task_failure_context().formatted_exception_string();
  } else if (death_cause.context_case() == ContextCase::kRuntimeEnvFailedContext) {
    return death_cause.runtime_env_failed_context().error_message();
  } else if (death_cause.context_case() == ContextCase::kActorUnschedulableContext) {
    return death_cause.actor_unschedulable_context().error_message();
  } else if (death_cause.context_case() == ContextCase::kActorDiedErrorContext) {
    return death_cause.actor_died_error_context().error_message();
  } else if (death_cause.context_case() == ContextCase::kOomContext) {
    return death_cause.oom_context().error_message();
  } else {
    RAY_CHECK(death_cause.context_case() == ContextCase::CONTEXT_NOT_SET);
    return "Death cause not recorded.";
  }
}

inline bool IsActorRestartable(const rpc::ActorTableData &actor) {
  RAY_CHECK_EQ(actor.state(), rpc::ActorTableData::DEAD);
  return actor.death_cause().context_case() == ContextCase::kActorDiedErrorContext &&
         actor.death_cause().actor_died_error_context().reason() ==
             rpc::ActorDiedErrorContext::OUT_OF_SCOPE &&
         ((actor.max_restarts() == -1) ||
          (static_cast<int64_t>(actor.num_restarts()) < actor.max_restarts()));
}

inline std::string RayErrorInfoToString(const ray::rpc::RayErrorInfo &error_info) {
  std::stringstream ss;
  ss << "Error type " << error_info.error_type() << " exception string "
     << error_info.error_message();
  return ss.str();
}

/// Get the parent task id from the task event.
///
/// \param task_event Task event.
/// \return TaskID::Nil() if parent task id info not available, else the parent task id
/// for the task.
inline TaskID GetParentTaskId(const rpc::TaskEvents &task_event) {
  if (task_event.has_task_info()) {
    return TaskID::FromBinary(task_event.task_info().parent_task_id());
  }
  return TaskID::Nil();
}

inline void FillTaskInfo(rpc::TaskInfoEntry *task_info,
                         const TaskSpecification &task_spec) {
  rpc::TaskType type;
  if (task_spec.IsNormalTask()) {
    type = rpc::TaskType::NORMAL_TASK;
  } else if (task_spec.IsDriverTask()) {
    type = rpc::TaskType::DRIVER_TASK;
  } else if (task_spec.IsActorCreationTask()) {
    type = rpc::TaskType::ACTOR_CREATION_TASK;
    task_info->set_actor_id(task_spec.ActorCreationId().Binary());
  } else {
    RAY_CHECK(task_spec.IsActorTask());
    type = rpc::TaskType::ACTOR_TASK;
    task_info->set_actor_id(task_spec.ActorId().Binary());
  }
  task_info->set_type(type);
  task_info->set_name(task_spec.GetName());
  task_info->set_language(task_spec.GetLanguage());
  task_info->set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());
  // NOTE(rickyx): we will have scheduling states recorded in the events list.
  task_info->set_scheduling_state(rpc::TaskStatus::NIL);
  task_info->set_job_id(task_spec.JobId().Binary());

  task_info->set_task_id(task_spec.TaskId().Binary());
  // NOTE: we set the parent task id of a task to be submitter's task id, where
  // the submitter depends on the owner coreworker's:
  // - if the owner coreworker runs a normal task, the submitter's task id is the task id.
  // - if the owner coreworker runs an actor, the submitter's task id will be the actor's
  // creation task id.
  task_info->set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
  task_info->mutable_required_resources()->insert(resources_map.begin(),
                                                  resources_map.end());
  task_info->mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());
  const auto &pg_id = task_spec.PlacementGroupBundleId().first;
  if (!pg_id.IsNil()) {
    task_info->set_placement_group_id(pg_id.Binary());
  }
  if (task_spec.GetMessage().call_site().size() > 0) {
    task_info->set_call_site(task_spec.GetMessage().call_site());
  }
}

// Fill task_info for the export API with task specification from task_spec
inline void FillExportTaskInfo(rpc::ExportTaskEventData::TaskInfoEntry *task_info,
                               const TaskSpecification &task_spec) {
  rpc::TaskType type;
  if (task_spec.IsNormalTask()) {
    type = rpc::TaskType::NORMAL_TASK;
  } else if (task_spec.IsDriverTask()) {
    type = rpc::TaskType::DRIVER_TASK;
  } else if (task_spec.IsActorCreationTask()) {
    type = rpc::TaskType::ACTOR_CREATION_TASK;
    task_info->set_actor_id(task_spec.ActorCreationId().Binary());
  } else {
    RAY_CHECK(task_spec.IsActorTask());
    type = rpc::TaskType::ACTOR_TASK;
    task_info->set_actor_id(task_spec.ActorId().Binary());
  }
  task_info->set_type(type);
  task_info->set_language(task_spec.GetLanguage());
  task_info->set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());

  task_info->set_task_id(task_spec.TaskId().Binary());
  // NOTE: we set the parent task id of a task to be submitter's task id, where
  // the submitter depends on the owner coreworker's:
  // - if the owner coreworker runs a normal task, the submitter's task id is the task id.
  // - if the owner coreworker runs an actor, the submitter's task id will be the actor's
  // creation task id.
  task_info->set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
  task_info->mutable_required_resources()->insert(resources_map.begin(),
                                                  resources_map.end());
  task_info->mutable_labels()->insert(task_spec.GetLabels().begin(),
                                      task_spec.GetLabels().end());

  auto export_runtime_env_info = task_info->mutable_runtime_env_info();
  export_runtime_env_info->set_serialized_runtime_env(
      task_spec.RuntimeEnvInfo().serialized_runtime_env());
  auto export_runtime_env_uris = export_runtime_env_info->mutable_uris();
  export_runtime_env_uris->set_working_dir_uri(
      task_spec.RuntimeEnvInfo().uris().working_dir_uri());
  export_runtime_env_uris->mutable_py_modules_uris()->CopyFrom(
      task_spec.RuntimeEnvInfo().uris().py_modules_uris());
  auto export_runtime_env_config = export_runtime_env_info->mutable_runtime_env_config();
  export_runtime_env_config->set_setup_timeout_seconds(
      task_spec.RuntimeEnvInfo().runtime_env_config().setup_timeout_seconds());
  export_runtime_env_config->set_eager_install(
      task_spec.RuntimeEnvInfo().runtime_env_config().eager_install());
  export_runtime_env_config->mutable_log_files()->CopyFrom(
      task_spec.RuntimeEnvInfo().runtime_env_config().log_files());

  const auto &pg_id = task_spec.PlacementGroupBundleId().first;
  if (!pg_id.IsNil()) {
    task_info->set_placement_group_id(pg_id.Binary());
  }
}

/// Generate a RayErrorInfo from ErrorType
inline rpc::RayErrorInfo GetRayErrorInfo(const rpc::ErrorType &error_type,
                                         const std::string &error_msg = "") {
  rpc::RayErrorInfo error_info;
  error_info.set_error_type(error_type);
  error_info.set_error_message(error_msg);
  return error_info;
}

/// Get the worker id from the task event.
///
/// \param task_event Task event.
/// \return WorkerID::Nil() if worker id info not available, else the worker id.
inline WorkerID GetWorkerID(const rpc::TaskEvents &task_event) {
  if (task_event.has_state_updates() && task_event.state_updates().has_worker_id()) {
    return WorkerID::FromBinary(task_event.state_updates().worker_id());
  }
  return WorkerID::Nil();
}

/// Return if the task has already terminated (finished or failed)
///
/// \param task_event Task event.
/// \return True if the task has already terminated, false otherwise.
inline bool IsTaskTerminated(const rpc::TaskEvents &task_event) {
  if (!task_event.has_state_updates()) {
    return false;
  }

  const auto &state_updates = task_event.state_updates();
  return state_updates.state_ts_ns().contains(rpc::TaskStatus::FINISHED) ||
         state_updates.state_ts_ns().contains(rpc::TaskStatus::FAILED);
}

inline size_t NumProfileEvents(const rpc::TaskEvents &task_event) {
  if (!task_event.has_profile_events()) {
    return 0;
  }
  return static_cast<size_t>(task_event.profile_events().events_size());
}

inline TaskAttempt GetTaskAttempt(const rpc::TaskEvents &task_event) {
  return std::make_pair(TaskID::FromBinary(task_event.task_id()),
                        task_event.attempt_number());
}

inline bool IsActorTask(const rpc::TaskEvents &task_event) {
  if (!task_event.has_task_info()) {
    return false;
  }

  const auto &task_info = task_event.task_info();
  return task_info.type() == rpc::TaskType::ACTOR_TASK ||
         task_info.type() == rpc::TaskType::ACTOR_CREATION_TASK;
}

inline bool IsTaskFinished(const rpc::TaskEvents &task_event) {
  if (!task_event.has_state_updates()) {
    return false;
  }

  const auto &state_updates = task_event.state_updates();
  return state_updates.state_ts_ns().contains(rpc::TaskStatus::FINISHED);
}

/// Fill the rpc::TaskStateUpdate with the timestamps according to the status change.
///
/// \param task_status The task status.
/// \param timestamp The timestamp.
/// \param[out] state_updates The state updates with timestamp to be updated.
inline void FillTaskStatusUpdateTime(const ray::rpc::TaskStatus &task_status,
                                     int64_t timestamp,
                                     ray::rpc::TaskStateUpdate *state_updates) {
  if (task_status == rpc::TaskStatus::NIL) {
    // Not status change.
    return;
  }
  (*state_updates->mutable_state_ts_ns())[task_status] = timestamp;
}

/// Fill the rpc::ExportTaskEventData::TaskStateUpdate with the timestamps
/// according to the status change.
///
/// \param task_status The task status.
/// \param timestamp The timestamp.
/// \param[out] state_updates The state updates with timestamp to be updated.
inline void FillExportTaskStatusUpdateTime(
    const ray::rpc::TaskStatus &task_status,
    int64_t timestamp,
    rpc::ExportTaskEventData::TaskStateUpdate *state_updates) {
  if (task_status == rpc::TaskStatus::NIL) {
    // Not status change.
    return;
  }
  (*state_updates->mutable_state_ts_ns())[task_status] = timestamp;
}

/// Convert rpc::TaskLogInfo to rpc::ExportTaskEventData::TaskLogInfo
inline void TaskLogInfoToExport(const rpc::TaskLogInfo &src,
                                rpc::ExportTaskEventData::TaskLogInfo *dest) {
  dest->set_stdout_file(src.stdout_file());
  dest->set_stderr_file(src.stderr_file());
  dest->set_stdout_start(src.stdout_start());
  dest->set_stdout_end(src.stdout_end());
  dest->set_stderr_start(src.stderr_start());
  dest->set_stderr_end(src.stderr_end());
}

inline std::string FormatPlacementGroupLabelName(const std::string &pg_id) {
  return kPlacementGroupConstraintKeyPrefix + pg_id;
}

/// \brief Format placement group details.
///     Format:
///        <pg_id>:<strategy>:<state>
///
/// \param pg_data
/// \return
inline std::string FormatPlacementGroupDetails(
    const rpc::PlacementGroupTableData &pg_data) {
  return PlacementGroupID::FromBinary(pg_data.placement_group_id()).Hex() + ":" +
         rpc::PlacementStrategy_Name(pg_data.strategy()) + "|" +
         rpc::PlacementGroupTableData::PlacementGroupState_Name(pg_data.state());
}

/// Generate a placement constraint for placement group.
///
/// \param pg_id The ID of placement group.
/// \param strategy The placement strategy of placement group.
/// \return The placement constraint for placement group if it's not a strict
///   strategy, else absl::nullopt.
inline std::optional<rpc::autoscaler::PlacementConstraint>
GenPlacementConstraintForPlacementGroup(const std::string &pg_id,
                                        rpc::PlacementStrategy strategy) {
  rpc::autoscaler::PlacementConstraint pg_constraint;
  // We are embedding the PG id into the key for the same reasons as we do for
  // dynamic labels (a node will have multiple PGs thus having a common PG key
  // is not enough).
  const std::string name = FormatPlacementGroupLabelName(pg_id);
  switch (strategy) {
  case rpc::PlacementStrategy::STRICT_SPREAD: {
    pg_constraint.mutable_anti_affinity()->set_label_name(name);
    pg_constraint.mutable_anti_affinity()->set_label_value("");
    return pg_constraint;
  }
  case rpc::PlacementStrategy::STRICT_PACK: {
    pg_constraint.mutable_affinity()->set_label_name(name);
    pg_constraint.mutable_affinity()->set_label_value("");
    return pg_constraint;
  }
  case rpc::PlacementStrategy::SPREAD:
  case rpc::PlacementStrategy::PACK: {
    return absl::nullopt;
  }
  default: {
    RAY_LOG(ERROR) << "Encountered unexpected strategy type: " << strategy;
  }
  }
  return absl::nullopt;
}

}  // namespace gcs

}  // namespace ray
