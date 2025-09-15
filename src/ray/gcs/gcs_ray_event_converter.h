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

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest_prod.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsRayEventConverter converts RayEvents to TaskEvents.
class GcsRayEventConverter {
 public:
  GcsRayEventConverter() = default;
  ~GcsRayEventConverter() = default;

  /// Convert an AddEventsRequest to a list of AddTaskEventDataRequest objects,
  /// grouping entries by job id.
  ///
  /// \param request The AddEventsRequest to convert.
  /// \return A list of AddTaskEventDataRequest grouped by job id.
  std::vector<rpc::AddTaskEventDataRequest> ConvertToTaskEventDataRequests(
      rpc::events::AddEventsRequest &&request);

 private:
  /// Convert a TaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The TaskDefinitionEvent to convert.
  /// \return The output TaskEvents to populate.
  rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskDefinitionEvent &&event);

  /// Convert ProfileEvents to a TaskEvents.
  ///
  /// \param event TaskProfileEvents object to convert.
  /// \return The output TaskEvents to populate.
  rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskProfileEvents &&event);

  /// Convert a TaskExecutionEvent to a TaskEvents.
  ///
  /// \param event The TaskExecutionEvent to convert.
  /// \return The output TaskEvents to populate.
  rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskExecutionEvent &&event);

  /// Convert an ActorTaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The ActorTaskDefinitionEvent to convert.
  /// \return The output TaskEvents to populate.
  rpc::TaskEvents ConvertToTaskEvents(rpc::events::ActorTaskDefinitionEvent &&event);

  /// Populate the TaskInfoEntry with the given runtime env info, function descriptor,
  /// and required resources. This function is commonly used to convert the task
  /// and actor task definition events to TaskEvents.
  ///
  /// \param runtime_env_info The runtime env info.
  /// \param function_descriptor The function descriptor.
  /// \param required_resources The required resources.
  /// \param language The language of the task.
  /// \param task_info The output TaskInfoEntry to populate.
  void PopulateTaskRuntimeAndFunctionInfo(
      rpc::RuntimeEnvInfo &&runtime_env_info,
      rpc::FunctionDescriptor &&function_descriptor,
      ::google::protobuf::Map<std::string, double> &&required_resources,
      rpc::Language language,
      rpc::TaskInfoEntry *task_info);

  /// Add a task event to the appropriate job-grouped request.
  ///
  /// \param task_event The TaskEvents to add.
  /// \param requests_per_job_id The list of requests grouped by job id.
  /// \param job_id_to_index The map from job id to index in requests_per_job_id.
  void AddTaskEventToRequest(
      rpc::TaskEvents &&task_event,
      std::vector<rpc::AddTaskEventDataRequest> &requests_per_job_id,
      absl::flat_hash_map<std::string, size_t> &job_id_to_index);

  /// Add dropped task attempts to the appropriate job-grouped request.
  ///
  /// \param metadata The task events metadata containing dropped task attempts.
  /// \param requests_per_job_id The list of requests grouped by job id.
  /// \param job_id_to_index The map from job id to index in requests_per_job_id.
  void AddDroppedTaskAttemptsToRequest(
      rpc::events::TaskEventsMetadata &&metadata,
      std::vector<rpc::AddTaskEventDataRequest> &requests_per_job_id,
      absl::flat_hash_map<std::string, size_t> &job_id_to_index);

  FRIEND_TEST(GcsRayEventConverterTest, TestConvertTaskExecutionEvent);
  FRIEND_TEST(GcsRayEventConverterTest, TestConvertActorTaskDefinitionEvent);
};

}  // namespace gcs
}  // namespace ray
