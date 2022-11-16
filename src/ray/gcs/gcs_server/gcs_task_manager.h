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

using AddTaskEventCallback = std::function<void(Status status, const TaskID &task_id)>;

/// GcsTaskManger is responsible for capturing task states change reported from other
/// components, i.e. raylets/workers through grpc handles.
/// This class is thread-safe.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  ///
  explicit GcsTaskManager(instrumented_io_context &io_service);

  void HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                              rpc::AddTaskEventDataReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllTaskEvents(rpc::GetAllTaskEventsRequest request,
                              rpc::GetAllTaskEventsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  // Stops the event loop and the thread of the task event handler.
  void Stop();

 private:
  /// Add events for a single task to the underlying GCS storage.
  ///
  /// \param task_id Task's id.
  /// \param events_by_task Events by a single task.
  Status AddTaskEventForTask(const TaskID &task_id, rpc::TaskEvents &&events_by_task);

 private:
  /// Mutex guarding tasks_reported_
  absl::Mutex mutex_;

  /// Total set of tasks id reported to GCS, this might be larger than the actual tasks
  /// stored in GCS due to constrain on the size of GCS table We need a total set instead
  /// of a simple counter since events from a single task might arrive at separate gRPC
  /// calls. With a simple counter, we are not able to know the exact number of tasks we
  /// are dropping.
  absl::flat_hash_set<TaskID> tasks_reported_ GUARDED_BY(mutex_);

  /// Current task events tracked. This map might contain less events than the actual task
  /// events reported to GCS due to truncation for capping memory usage.
  /// TODO(rickyx):  Refactor this to an abstraction
  absl::flat_hash_map<TaskID, rpc::TaskEvents> task_events_ GUARDED_BY(mutex_);

  /// Counter for tracking the size of task event.
  size_t num_bytes_task_events_ = 0;

  /// Its own separate IO service and thread.
  instrumented_io_context &io_service_;
  std::unique_ptr<std::thread> io_service_thread_;
};

}  // namespace gcs
}  // namespace ray
