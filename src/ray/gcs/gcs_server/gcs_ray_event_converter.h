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

  /// Convert an AddEventsRequest to an AddTaskEventDataRequest.
  ///
  /// \param request The AddEventsRequest to convert.
  /// \param data The output AddTaskEventDataRequest to populate.
  void ConvertToTaskEventDataRequest(rpc::events::AddEventsRequest &&request,
                                     rpc::AddTaskEventDataRequest &data);

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

  FRIEND_TEST(GcsRayEventConverterTest, TestConvertTaskExecutionEvent);
};

}  // namespace gcs
}  // namespace ray
