// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using AddTaskStateEventCallback =
    std::function<void(Status status, const TaskID &task_id)>;

/// GcsTaskManger is responsible for capturing task states change reported from other
/// components, i.e. raylets/workers through grpc handles. This class is not thread-safe.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  ///
  /// \param gcs_task_info_table GCS table external storage accessor.
  explicit GcsTaskManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
      : gcs_table_storage_(std::move(gcs_table_storage)){};

  /// TODO(tb)
  void HandleAddTaskStateEventData(rpc::AddTaskStateEventDataRequest request,
                                   rpc::AddTaskStateEventDataReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllTaskStateEvent(rpc::GetAllTaskStateEventRequest request,
                                  rpc::GetAllTaskStateEventReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

 private:
  void AddTaskStateEvents(rpc::TaskStateEventData &&data,
                          AddTaskStateEventCallback cb_on_done);

  void AddTaskStateEventForTask(const TaskID &task_id,
                                rpc::TaskStateEvents &&events_by_task,
                                AddTaskStateEventCallback cb_on_done);

 private:
  std::shared_ptr<GcsTableStorage> gcs_table_storage_;
};

}  // namespace gcs
}  // namespace ray
