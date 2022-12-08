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

/// Type alias for a single task attempt, i.e. <task id and attempt number>.
using TaskAttempt = std::pair<TaskID, int32_t>;

/// GcsTaskManger is responsible for capturing task states change reported by
/// TaskEventBuffer from other components.
///
/// When the maximal number of task events tracked specified by
/// `RAY_task_events_max_num_task_in_gcs` is exceeded, older events (approximately by
/// insertion order) will be dropped.
///
/// This class has its own io_context and io_thread, that's separate from other GCS
/// services. All handling of all rpc will be posted to the single thread it owns.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  GcsTaskManager()
      : task_event_storage_(std::make_unique<GcsTaskManagerStorage>(
            RayConfig::instance().task_events_max_num_task_in_gcs())),
        io_service_thread_(std::make_unique<std::thread>([this] {
          SetThreadName("task_events");
          // Keep io_service_ alive.
          boost::asio::io_service::work io_service_work_(io_service_);
          io_service_.run();
        })) {}

  /// Handles a AddTaskEventData request.
  ///
  /// \param request gRPC Request.
  /// \param reply gRPC Reply.
  /// \param send_reply_callback Callback to invoke when sending reply.
  void HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                              rpc::AddTaskEventDataReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  ///  Handles a GetTaskEvents request.
  ///
  /// \param request gRPC Request.
  /// \param reply gRPC Reply.
  /// \param send_reply_callback Callback to invoke when sending reply.
  void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                           rpc::GetTaskEventsReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  /// Stops the event loop and the thread of the task event handler.
  ///
  /// This function returns when the io thread is joined.
  void Stop();

  /// Returns the io_service.
  ///
  /// \return Reference to its io_service.
  instrumented_io_context &GetIoContext() { return io_service_; }

  /// A storage component that stores the task events.
  ///
  /// This is an in-memory storage component that supports adding and getting of task
  /// events.
  ///
  /// It merges events from a single task attempt (same task id and attempt number) into
  /// a single rpc::TaskEvents entry, as reported by multiple rpc calls from workers.
  ///
  /// When more than `RAY_task_events_max_num_task_in_gcs` task events are stored in the
  /// the storage, older task events will be replaced by new task events, where older
  /// task events are approximately task events that arrived in earlier rpc.
  class GcsTaskManagerStorage {
   public:
    /// Constructor
    ///
    /// \param max_num_task_events Max number of task events stored before replacing older
    /// ones.
    GcsTaskManagerStorage(size_t max_num_task_events)
        : max_num_task_events_(max_num_task_events) {}

    /// Add a new task event or replace an existing task event in the storage.
    ///
    /// If there are already `RAY_task_events_max_num_task_in_gcs` in the storage, the
    /// oldest task event will be replaced. Otherwise the `task_event` will be added.
    ///
    /// \param task_event Task event to be added to the storage.
    /// \return absl::nullptr if the `task_event` is added without replacement, else the
    /// replaced task event.
    absl::optional<rpc::TaskEvents> AddOrReplaceTaskEvent(rpc::TaskEvents task_event);

    /// Get task events.
    ///
    /// \param job_id Getting task events from this `job_id` only.
    /// \return A vector of task events.
    std::vector<rpc::TaskEvents> GetTaskEvents(
        absl::optional<JobID> job_id = absl::nullopt);

    /// Max number of task events allowed in the storage.
    const size_t max_num_task_events_ = 0;

    /// Current task events stored.
    std::vector<rpc::TaskEvents> task_events_;

    /// A iterator into task_events_ that determines which element to be overwritten.
    size_t next_idx_to_overwrite_ = 0;

    /// Index from task attempt to the index of the corresponding task event.
    absl::flat_hash_map<TaskAttempt, size_t> task_attempt_index_;

    /// Counter for tracking the size of task event. This assumes tasks events are never
    /// removed actively.
    uint64_t num_bytes_task_events_ = 0;
  };

 private:
  /// Add a profile event to the reply.
  ///
  /// \param reply rpc reply.
  /// \param task_event Task event from which the profile event will be made.
  void AddProfileEvent(rpc::GetTaskEventsReply *reply, rpc::TaskEvents &task_event);

  ///  Add a task status update event to the reply.
  ///
  /// \param reply rpc reply.
  /// \param task_event Task event from which the task status updates will be made.
  void AddStatusUpdateEvent(rpc::GetTaskEventsReply *reply, rpc::TaskEvents &task_event);

  /// Total number of task events reported.
  uint32_t total_num_task_events_reported_ = 0;

  /// Total number of status task events dropped on the worker.
  uint32_t total_num_status_task_events_dropped_ = 0;

  /// Total number of profile task events dropped on the worker.
  uint32_t total_num_profile_task_events_dropped_ = 0;

  // Pointer to the underlying task events storage.
  std::unique_ptr<GcsTaskManagerStorage> task_event_storage_;

  /// Its own separate IO service separated from the main service.
  instrumented_io_context io_service_;

  /// Its own IO thread from the main thread.
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