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

#include <memory>
#include <string>
#include <utility>

#include "absl/time/time.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/export_task_event.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using ContextCase = rpc::ActorDeathCause::ContextCase;

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
std::shared_ptr<ray::rpc::JobTableData> CreateJobTableData(
    const ray::JobID &job_id,
    bool is_dead,
    const ray::rpc::Address &driver_address,
    int64_t driver_pid,
    const std::string &entrypoint,
    const ray::rpc::JobConfig &job_config = {});

/// Helper function to produce error table data.
rpc::ErrorTableData CreateErrorTableData(const std::string &error_type,
                                         const std::string &error_msg,
                                         absl::Time timestamp,
                                         const JobID &job_id = JobID::Nil());

/// Helper function to produce worker failure data.
std::shared_ptr<ray::rpc::WorkerTableData> CreateWorkerFailureData(
    const WorkerID &worker_id,
    const NodeID &node_id,
    const std::string &ip_address,
    int64_t timestamp,
    rpc::WorkerExitType disconnect_type,
    const std::string &disconnect_detail,
    int pid,
    const rpc::RayException *creation_task_exception = nullptr);

/// Get actor creation task exception from ActorDeathCause.
/// Returns nullptr if actor isn't dead due to creation task failure.
const rpc::RayException *GetCreationTaskExceptionFromDeathCause(
    const rpc::ActorDeathCause *death_cause);

const std::string &GetActorDeathCauseString(const rpc::ActorDeathCause &death_cause);

/// Get the error information from the actor death cause.
///
/// \param[in] death_cause The rpc message that contains the actos death information.
/// \return RayErrorInfo that has propagated death cause.
rpc::RayErrorInfo GetErrorInfoFromActorDeathCause(
    const rpc::ActorDeathCause &death_cause);

/// Generate object error type from ActorDeathCause.
std::string GenErrorMessageFromDeathCause(const rpc::ActorDeathCause &death_cause);

bool IsActorRestartable(const rpc::ActorTableData &actor);

std::string RayErrorInfoToString(const ray::rpc::RayErrorInfo &error_info);

/// Get the parent task id from the task event.
///
/// \param task_event Task event.
/// \return TaskID::Nil() if parent task id info not available, else the parent task id
/// for the task.
TaskID GetParentTaskId(const rpc::TaskEvents &task_event);

void FillTaskInfo(rpc::TaskInfoEntry *task_info, const TaskSpecification &task_spec);

// Fill task_info for the export API with task specification from task_spec
void FillExportTaskInfo(rpc::ExportTaskEventData::TaskInfoEntry *task_info,
                        const TaskSpecification &task_spec);

/// Generate a RayErrorInfo from ErrorType
rpc::RayErrorInfo GetRayErrorInfo(const rpc::ErrorType &error_type,
                                  const std::string &error_msg = "");

/// Get the worker id from the task event.
///
/// \param task_event Task event.
/// \return WorkerID::Nil() if worker id info not available, else the worker id.
WorkerID GetWorkerID(const rpc::TaskEvents &task_event);

/// Return if the task has already terminated (finished or failed)
///
/// \param task_event Task event.
/// \return True if the task has already terminated, false otherwise.
bool IsTaskTerminated(const rpc::TaskEvents &task_event);

size_t NumProfileEvents(const rpc::TaskEvents &task_event);

TaskAttempt GetTaskAttempt(const rpc::TaskEvents &task_event);

bool IsActorTask(const rpc::TaskEvents &task_event);

bool IsTaskFinished(const rpc::TaskEvents &task_event);

/// Fill the rpc::TaskStateUpdate with the timestamps according to the status change.
///
/// \param task_status The task status.
/// \param timestamp The timestamp.
/// \param[out] state_updates The state updates with timestamp to be updated.
void FillTaskStatusUpdateTime(const ray::rpc::TaskStatus &task_status,
                              int64_t timestamp,
                              ray::rpc::TaskStateUpdate *state_updates);

/// Fill the rpc::ExportTaskEventData::TaskStateUpdate with the timestamps
/// according to the status change.
///
/// \param task_status The task status.
/// \param timestamp The timestamp.
/// \param[out] state_updates The state updates with timestamp to be updated.
void FillExportTaskStatusUpdateTime(
    const ray::rpc::TaskStatus &task_status,
    int64_t timestamp,
    rpc::ExportTaskEventData::TaskStateUpdate *state_updates);

/// Convert rpc::TaskLogInfo to rpc::ExportTaskEventData::TaskLogInfo
void TaskLogInfoToExport(const rpc::TaskLogInfo &src,
                         rpc::ExportTaskEventData::TaskLogInfo *dest);

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
std::optional<rpc::autoscaler::PlacementConstraint>
GenPlacementConstraintForPlacementGroup(const std::string &pg_id,
                                        rpc::PlacementStrategy strategy);

}  // namespace gcs
}  // namespace ray
