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
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using AddTaskEventCallback = std::function<void(Status status, const TaskID &task_id)>;

using TaskAttempt = std::pair<TaskID, int32_t>;

/// GcsTaskManger is responsible for capturing task states change reported from other
/// components, i.e. raylets/workers through grpc handles.
/// When the maximal number of task events tracked specified by
/// `RAY_task_events_max_num_task_in_gcs`, older events (approximately by insertion order)
/// will be dropped.
/// This class has its own io_context and io_thread, that's separate from
/// other GCS services.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  ///
  GcsTaskManager()
      : task_event_storage_(std::make_unique<GcsTaskManagerStorage>(
            RayConfig::instance().task_events_max_num_task_in_gcs())),
        io_service_thread_(std::make_unique<std::thread>([this] {
          SetThreadName("task_events");
          // Keep io_service_ alive.
          boost::asio::io_service::work io_service_work_(io_service_);
          io_service_.run();
        })) {}

  void HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                              rpc::AddTaskEventDataReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                           rpc::GetTaskEventsReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  // Stops the event loop and the thread of the task event handler.
  void Stop();

  instrumented_io_context &GetIoContext() { return io_service_; }

  class GcsTaskManagerStorage {
   public:
    GcsTaskManagerStorage(size_t max_num_task_events)
        : max_num_task_events_(max_num_task_events) {}

    absl::optional<rpc::TaskEvents> AddOrReplaceTaskEvent(rpc::TaskEvents task_event);

    std::vector<rpc::TaskEvents> GetTaskEvents(
        absl::optional<JobID> job_id = absl::nullopt);

    const size_t max_num_task_events_ = 0;
    /// Current task events tracked. This map might contain less events than the
    /// actual task events reported to GCS due to truncation for capping memory usage.
    std::vector<rpc::TaskEvents> task_events_;
    /// A iterator into task_events_ that determines which element to be overwritten.
    size_t next_idx_to_overwrite_ = 0;
    /// Counter for tracking the size of task event. This assumes tasks events are never
    /// removed actively.
    uint64_t num_bytes_task_events_ = 0;

    absl::flat_hash_map<TaskAttempt, size_t> task_attempt_index_;
  };

 private:
  /// Add events for a single task to the underlying GCS storage.
  ///
  /// \param task_id Task's id.
  /// \param events_by_task Events by a single task.
  void AddTaskEventForTask(rpc::TaskEvents &&events_by_task);

  uint32_t total_num_task_events_reported_ = 0;

  /// Total number of task events dropped on the worker.
  uint32_t total_num_status_task_events_dropped_ = 0;
  uint32_t total_num_profile_task_events_dropped_ = 0;

  std::unique_ptr<GcsTaskManagerStorage> task_event_storage_;

  /// Its own separate IO service and thread.
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> io_service_thread_;

  FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
  FRIEND_TEST(GcsTaskManagerTest, TestAddTaskEventMerge);
  FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestGetTaskEvents);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestGetTaskEventsByJob);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
};

}  // namespace gcs
}  // namespace ray