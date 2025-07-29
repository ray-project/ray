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

#include "gtest/gtest_prod.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsOneEventConverter converts RayEvents to TaskEvents.
class GcsOneEventConverter {
 public:
  GcsOneEventConverter() = default;
  ~GcsOneEventConverter() = default;

  /// Convert an AddEventRequest to an AddTaskEventDataRequest.
  ///
  /// \param request The AddEventRequest to convert.
  /// \param data The output AddTaskEventDataRequest to populate.
  void ConvertToTaskEventData(const rpc::events::AddEventRequest &request,
                              rpc::AddTaskEventDataRequest &data);

 private:
  /// Convert a TaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The TaskDefinitionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertTaskDefinitionEventToTaskEvent(
      const rpc::events::TaskDefinitionEvent &event, rpc::TaskEvents &task_event);

  /// Convert a TaskExecutionEvent to a TaskEvents.
  ///
  /// \param event The TaskExecutionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertTaskExecutionEventToTaskEvent(const rpc::events::TaskExecutionEvent &event,
                                            rpc::TaskEvents &task_event);

  /// Convert an ActorTaskDefinitionEvent to a TaskEvents.
  ///
  /// \param event The ActorTaskDefinitionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertActorTaskDefinitionEventToTaskEvent(
      const rpc::events::ActorTaskDefinitionEvent &event, rpc::TaskEvents &task_event);

  /// Convert an ActorTaskExecutionEvent to a TaskEvents.
  ///
  /// \param event The ActorTaskExecutionEvent to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertActorTaskExecutionEventToTaskEvent(
      const rpc::events::ActorTaskExecutionEvent &event, rpc::TaskEvents &task_event);

  /// Convert ProfileEvents to a TaskEvents.
  ///
  /// \param event TaskProfileEvents object to convert.
  /// \param task_event The output TaskEvents to populate.
  void ConvertTaskProfileEventsToTaskEvent(const rpc::events::TaskProfileEvents &event,
                                           rpc::TaskEvents &task_event);

  /// Generate a TaskInfoEntry from the given runtime env info, function descriptor,
  /// and required resources. This function is commonly used to convert the task
  /// and actor task definition events to TaskEvents.
  ///
  /// \param runtime_env_info The runtime env info.
  /// \param function_descriptor The function descriptor.
  /// \param required_resources The required resources.
  /// \param task_info The output TaskInfoEntry to populate.
  void GenerateTaskInfoEntry(
      const rpc::RuntimeEnvInfo &runtime_env_info,
      const rpc::FunctionDescriptor &function_descriptor,
      const ::google::protobuf::Map<std::string, std::string> &required_resources,
      rpc::TaskInfoEntry *task_info);

  FRIEND_TEST(GcsOneEventConverterTest, TestConvertTaskExecutionEvent);
  FRIEND_TEST(GcsOneEventConverterTest, TestConvertActorTaskDefinitionEvent);
  FRIEND_TEST(GcsOneEventConverterTest,
              TestConvertActorTaskDefinitionEventWithInvalidResources);
  FRIEND_TEST(GcsOneEventConverterTest, TestConvertActorTaskExecutionEvent);
  FRIEND_TEST(GcsOneEventConverterTest, TestConvertTaskProfileEventsToTaskEvent);
};

}  // namespace gcs
}  // namespace ray
