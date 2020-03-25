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

#ifndef RAY_GCS_TASK_INFO_HANDLER_IMPL_H
#define RAY_GCS_TASK_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `TaskInfoHandler`.
class DefaultTaskInfoHandler : public rpc::TaskInfoHandler {
 public:
  explicit DefaultTaskInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleDeleteTasks(const DeleteTasksRequest &request, DeleteTasksReply *reply,
                         SendReplyCallback send_reply_callback) override;

  void HandleAddTaskLease(const AddTaskLeaseRequest &request, AddTaskLeaseReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleAttemptTaskReconstruction(const AttemptTaskReconstructionRequest &request,
                                       AttemptTaskReconstructionReply *reply,
                                       SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_TASK_INFO_HANDLER_IMPL_H
