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

#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Type alias for a single task attempt, i.e. <task id and attempt number>.
using TaskAttempt = std::pair<TaskID, int32_t>;

/// Type alias for updating task event callback stored in the
/// GcsTaskManager::GcsTaskManagerStorage.
/// It takes a pointer to rpc::TaskEvents as argument, and returns if the update is
/// successful.
using UpdateTaskEventCallback = std::function<bool(rpc::TaskEvents *)>;

/// GcsTaskManger is responsible for capturing task states change reported by
/// TaskEventBuffer from other components.
///
/// When the maximal number of task events tracked specified by
/// `RAY_task_events_max_num_task_in_gcs` is exceeded, older events (approximately by
/// insertion order) will be dropped.
/// TODO(https://github.com/ray-project/ray/issues/31071): track per job.
///
/// This class has its own io_context and io_thread, that's separate from other GCS
/// services. All handling of all rpc should be posted to the single thread it owns.
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
                              rpc::SendReplyCallback send_reply_callback)
      LOCKS_EXCLUDED(mutex_) override;

  /// Handle GetTaskEvent request.
  ///
  /// \param request gRPC Request.
  /// \param reply gRPC Reply.
  /// \param send_reply_callback Callback to invoke when sending reply.
  void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                           rpc::GetTaskEventsReply *reply,
                           rpc::SendReplyCallback send_reply_callback)
      LOCKS_EXCLUDED(mutex_) override;

  /// Stops the event loop and the thread of the task event handler.
  ///
  /// After this is called, no more requests will be handled.
  /// This function returns when the io thread is joined.
  void Stop() LOCKS_EXCLUDED(mutex_);

  /// Returns the io_service.
  ///
  /// \return Reference to its io_service.
  instrumented_io_context &GetIoContext() { return io_service_; }

  /// Return string of debug state.
  ///
  /// \return Debug string
  std::string DebugString() LOCKS_EXCLUDED(mutex_);

  /// Record metrics.
  void RecordMetrics() LOCKS_EXCLUDED(mutex_);

  /// A storage component that stores the task events.
  ///
  /// This is an in-memory storage component that supports adding and getting of task
  /// events.
  ///
  /// This class is not thread-safe.
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
    absl::optional<rpc::TaskEvents> AddOrReplaceTaskEvent(rpc::TaskEvents &&task_event);

    /// Get task events from job.
    ///
    /// \param job_id Job ID to filter task events.
    /// \return task events of `job_id`.
    std::vector<rpc::TaskEvents> GetTaskEvents(JobID job_id) const;

    /// Get all task events.
    ///
    /// \return all task events stored.
    std::vector<rpc::TaskEvents> GetTaskEvents() const;

    /// Get task events from tasks corresponding to `task_ids`.
    ///
    /// \param task_ids Task ids of the tasks.
    /// \return task events from the `task_ids`.
    std::vector<rpc::TaskEvents> GetTaskEvents(
        const absl::flat_hash_set<TaskID> &task_ids) const;

    /// Get task events of task attempt.
    ///
    /// \param task_attempts Task attempts (task ids + attempt number).
    /// \return task events from the `task_attempts`.
    std::vector<rpc::TaskEvents> GetTaskEvents(
        const absl::flat_hash_set<TaskAttempt> &task_attempts) const;

   private:
    /// Mark the task tree containing this task attempt as failure if necessary.
    ///
    /// A task tree is represented by the `parent_task_id` field for each task.
    ///
    /// All non-terminated (not finished nor failed) tasks in the task tree with the task
    /// event as the root should be marked as failed with the root's failed timestamp if
    /// any of:
    ///     1. Its parent has failed: this happens when a task event is reported from a
    ///     worker later than the failure status reported from the parent worker.
    ///     2. The task itself fails.
    ///
    /// This is necessary since workers/drivers/raylet could crash unexpectedly, and
    /// terminal task status are not reported when that happens. Here we leverages on
    /// Ray's fault tolerance where a parent task failure will lead to all child task fail
    /// to mark the task tree with failure status recursively.
    ///
    /// \param task_id ID of the task event that's the root of the task tree.
    /// \param parent_task_id ID of the task's parent.
    void MarkTaskTreeFailedIfNeeded(const TaskID &task_id, const TaskID &parent_task_id);

    /// Get the task failed timestamp of a task.
    ///
    /// This finds the task event from the latest task attempt for the task, and returns
    /// the failure timestamp if the task fails.
    ///
    /// \param task_id The task id of the task.
    /// \return The failed timestamp of the task attempt if it fails. absl::nullopt if the
    /// latest task attempt could not be found due to data loss or the task attempt
    /// doesn't fail.
    absl::optional<int64_t> GetTaskFailedTime(const TaskID &task_id) const;

    /// Mark the task as failure with the failed timestamp.
    ///
    /// This also overwrites the finished state of the task if the task has finished by
    /// clearing the finished timestamp.
    ///
    /// \param task_id The task to mark as failed.
    /// \param failed_ts The failure timestamp that's the same from parent's failure
    /// timestamp.
    void MarkTaskFailed(const TaskID &task_id, int64_t failed_ts);

    /// Get the latest task attempt for the task.
    ///
    /// If there is no such task or data loss due to task events dropped at the worker,
    /// i.e. missing task attempts for a task with retries, absl::nullopt will be
    /// returned.
    ///
    /// \param task_id The task's task id.
    /// \return The latest task attempt of the task, abls::nullopt if no task attempt
    /// could be found or there's data loss.
    absl::optional<TaskAttempt> GetLatestTaskAttempt(const TaskID &task_id) const;

    /// Get the number of task events stored.
    size_t GetTaskEventsCount() const { return task_events_.size(); }

    /// Get the total number of bytes of task events stored.
    uint64_t GetTaskEventsBytes() const { return num_bytes_task_events_; }

    /// Max number of task events allowed in the storage.
    const size_t max_num_task_events_ = 0;

    /// A iterator into task_events_ that determines which element to be overwritten.
    size_t next_idx_to_overwrite_ = 0;

    /// TODO(rickyx): Refactor this into LRI(least recently inserted) buffer:
    /// https://github.com/ray-project/ray/issues/31158
    /// Current task events stored.
    std::vector<rpc::TaskEvents> task_events_;

    /// Index from task attempt to the corresponding task attempt in the buffer
    /// `task_events_`.
    absl::flat_hash_map<TaskAttempt, size_t> task_attempt_index_;

    /// Secondary index from task id to task attempts.
    absl::flat_hash_map<TaskID, absl::flat_hash_set<TaskAttempt>>
        task_to_task_attempt_index_;

    /// Secondary index from job id to task attempts of the job.
    absl::flat_hash_map<JobID, absl::flat_hash_set<TaskAttempt>>
        job_to_task_attempt_index_;

    /// Secondary index from parent task id to a set of children task ids.
    absl::flat_hash_map<TaskID, absl::flat_hash_set<TaskID>>
        parent_to_children_task_index_;

    /// Counter for tracking the size of task event. This assumes tasks events are never
    /// removed actively.
    uint64_t num_bytes_task_events_ = 0;

    friend class GcsTaskManager;
    FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
    FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
  };

 private:
  /// Mutex guarding all fields that will be accessed by main_io as well.
  absl::Mutex mutex_;

  /// Total number of task events reported.
  uint32_t total_num_task_events_reported_ GUARDED_BY(mutex_) = 0;

  /// Total number of status task events dropped on the worker.
  uint32_t total_num_status_task_events_dropped_ GUARDED_BY(mutex_) = 0;

  /// Total number of profile task events dropped on the worker.
  uint32_t total_num_profile_task_events_dropped_ GUARDED_BY(mutex_) = 0;

  // Pointer to the underlying task events storage.
  std::unique_ptr<GcsTaskManagerStorage> task_event_storage_ GUARDED_BY(mutex_);

  /// Its own separate IO service separated from the main service.
  instrumented_io_context io_service_;

  /// Its own IO thread from the main thread.
  std::unique_ptr<std::thread> io_service_thread_;

  FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
  FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
};

}  // namespace gcs
}  // namespace ray
