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
  void ConvertToTaskEventDataRequests(rpc::events::AddEventsRequest &&request,
                                      std::vector<rpc::AddTaskEventDataRequest> &data);

 private:
  /// Convert a TaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The TaskDefinitionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertToTaskEvents(rpc::events::TaskDefinitionEvent &&event,
                           rpc::TaskEvents &task_event);

  /// Convert a TaskExecutionEvent to a TaskEvents.
  ///
  /// \param event The TaskExecutionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertToTaskEvents(rpc::events::TaskExecutionEvent &&event,
                           rpc::TaskEvents &task_event);

  /// Convert an ActorTaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The ActorTaskDefinitionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertToTaskEvents(rpc::events::ActorTaskDefinitionEvent &&event,
                           rpc::TaskEvents &task_event);

  /// Generate a TaskInfoEntry from the given runtime env info, function descriptor,
  /// and required resources. This function is commonly used to convert the task
  /// and actor task definition events to TaskEvents.
  ///
  /// \param runtime_env_info The runtime env info.
  /// \param function_descriptor The function descriptor.
  /// \param required_resources The required resources.
  /// \param language The language of the task.
  /// \param task_info The output TaskInfoEntry to populate.
  void GenerateTaskInfoEntry(
      rpc::RuntimeEnvInfo &&runtime_env_info,
      rpc::FunctionDescriptor &&function_descriptor,
      ::google::protobuf::Map<std::string, double> &&required_resources,
      rpc::Language language,
      rpc::TaskInfoEntry *task_info);

  FRIEND_TEST(GcsRayEventConverterTest, TestConvertTaskExecutionEvent);
  FRIEND_TEST(GcsRayEventConverterTest, TestConvertActorTaskDefinitionEvent);
};

}  // namespace gcs
}  // namespace ray
