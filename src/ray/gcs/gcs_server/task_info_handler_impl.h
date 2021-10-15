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

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `TaskInfoHandler`.
class DefaultTaskInfoHandler : public rpc::TaskInfoHandler {
 public:
  explicit DefaultTaskInfoHandler(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                                  std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub)
      : gcs_table_storage_(gcs_table_storage), gcs_pub_sub_(gcs_pub_sub) {}

  void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleAddTaskLease(const AddTaskLeaseRequest &request, AddTaskLeaseReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleGetTaskLease(const GetTaskLeaseRequest &request, GetTaskLeaseReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleAttemptTaskReconstruction(const AttemptTaskReconstructionRequest &request,
                                       AttemptTaskReconstructionReply *reply,
                                       SendReplyCallback send_reply_callback) override;

  std::string DebugString() const;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub_;

  // Debug info.
  enum CountType {
    ADD_TASK_REQUEST = 0,
    GET_TASK_REQUEST = 1,
    ADD_TASK_LEASE_REQUEST = 3,
    GET_TASK_LEASE_REQUEST = 4,
    ATTEMPT_TASK_RECONSTRUCTION_REQUEST = 5,
    CountType_MAX = 6,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace rpc
}  // namespace ray
