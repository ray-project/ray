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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"  // absl::Mutex
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using AddTaskStateEventCallback =
    std::function<void(Status status, const TaskID &task_id)>;

/// GcsTaskManger is responsible for capturing task states change reported from other
/// components, i.e. raylets/workers through grpc handles.
/// This class is thread-safe.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  ///
  /// \param gcs_task_info_table GCS table external storage accessor.
  explicit GcsTaskManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
      : gcs_table_storage_(std::move(gcs_table_storage)){};

  void HandleAddTaskStateEventData(rpc::AddTaskStateEventDataRequest request,
                                   rpc::AddTaskStateEventDataReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllTaskStateEvent(rpc::GetAllTaskStateEventRequest request,
                                  rpc::GetAllTaskStateEventReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

 private:
  /// Add events for multiple tasks to the underlying GCS storage.
  ///
  /// \param data Task events data
  /// \param cb_on_done Callback when adding the events is done.
  void AddTaskStateEvents(rpc::TaskStateEventData &&data,
                          AddTaskStateEventCallback cb_on_done);

  /// Add events for a single task to the underlying GCS storage.
  ///
  /// \param task_id Task's id.
  /// \param events_by_task Events by a single task.
  /// \param cb_on_done Callback to be invoked when events have been added to GCS.
  void AddTaskStateEventForTask(const TaskID &task_id,
                                rpc::TaskStateEvents &&events_by_task,
                                AddTaskStateEventCallback cb_on_done);

 private:
  /// Underlying GCS storage
  std::shared_ptr<GcsTableStorage> gcs_table_storage_;

  /// Mutex guarding tasks_reported_
  absl::Mutex mutex_;

  /// Total set of tasks id reported to GCS, this might be larger than the actual tasks
  /// stored in GCS due to constrain on the size of GCS table We need a total set instead
  /// of a simple counter since events from a single task might arrive at separate gRPC
  /// calls. With a simple counter, we are not able to know the exact number of tasks we
  /// are dropping.
  absl::flat_hash_set<TaskID> tasks_reported_ GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray
