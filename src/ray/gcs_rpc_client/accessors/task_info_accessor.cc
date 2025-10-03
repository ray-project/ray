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

#include "ray/gcs_rpc_client/accessors/task_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

TaskInfoAccessor::TaskInfoAccessor(GcsClientContext *context) : context_(context) {}

void TaskInfoAccessor::AsyncAddTaskEventData(std::unique_ptr<rpc::TaskEventData> data_ptr,
                                             StatusCallback callback) {
  // Implementation extracted from accessor.cc
  rpc::AddTaskEventDataRequest request;
  request.mutable_data()->Swap(data_ptr.get());
  context_->GetGcsRpcClient().AddTaskEventData(
      std::move(request),
      [callback = std::move(callback)](const Status &status,
                                       rpc::AddTaskEventDataReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
}

void TaskInfoAccessor::AsyncGetTaskEvents(
    const MultiItemCallback<rpc::TaskEvents> &callback) {
  RAY_LOG(DEBUG) << "Getting task info.";
  rpc::GetTaskEventsRequest request;
  context_->GetGcsRpcClient().GetTaskEvents(
      std::move(request),
      [callback](const Status &status, rpc::GetTaskEventsReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_events_by_task())));
      });
}

}  // namespace gcs
}  // namespace ray
