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
#include "ray/gcs/gcs_client/usage_stats_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Type alias for a single task attempt, i.e. <task id and attempt number>.
using TaskAttempt = std::pair<TaskID, int32_t>;

enum GcsTaskManagerCounter {
  kTotalNumTaskEventsReported,
  kTotalNumStatusTaskEventsDropped,
  kTotalNumProfileTaskEventsDropped,
  kNumTaskEventsBytesStored,
  kNumTaskEventsStored,
  kTotalNumActorCreationTask,
  kTotalNumActorTask,
  kTotalNumNormalTask,
  kTotalNumDriverTask,
};

const absl::flat_hash_map<rpc::TaskType, GcsTaskManagerCounter> kTaskTypeToCounterType = {
    {rpc::TaskType::NORMAL_TASK, kTotalNumNormalTask},
    {rpc::TaskType::ACTOR_CREATION_TASK, kTotalNumActorCreationTask},
    {rpc::TaskType::ACTOR_TASK, kTotalNumActorTask},
    {rpc::TaskType::DRIVER_TASK, kTotalNumDriverTask},
};

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
      : stats_counter_(),
        task_event_storage_(std::make_unique<GcsTaskManagerStorage>(
            RayConfig::instance().task_events_max_num_task_in_gcs(), stats_counter_)),
        io_service_thread_(std::make_unique<std::thread>([this] {
          SetThreadName("task_events");
          // Keep io_service_ alive.
          boost::asio::io_service::work io_service_work_(io_service_);
          io_service_.run();
        })),
        timer_(io_service_) {}

  /// Handles a AddTaskEventData request.
  ///
  /// \param request gRPC Request.
  /// \param reply gRPC Reply.
  /// \param send_reply_callback Callback to invoke when sending reply.
  void HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                              rpc::AddTaskEventDataReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Handle GetTaskEvent request.
  ///
  /// \param request gRPC Request.
  /// \param reply gRPC Reply.
  /// \param send_reply_callback Callback to invoke when sending reply.
  void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                           rpc::GetTaskEventsReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  /// Stops the event loop and the thread of the task event handler.
  ///
  /// After this is called, no more requests will be handled.
  /// This function returns when the io thread is joined.
  void Stop();

  /// Handler to be called when a job finishes. This marks all non-terminated tasks
  /// of the job as failed.
  ///
  /// \param job_id Job Id
  /// \param job_finish_time_ms Job finish time in ms.
  void OnJobFinished(const JobID &job_id, int64_t job_finish_time_ms);

  /// Returns the io_service.
  ///
  /// \return Reference to its io_service.
  instrumented_io_context &GetIoContext() { return io_service_; }

  /// Return string of debug state.
  ///
  /// \return Debug string
  std::string DebugString();

  /// Record metrics.
  void RecordMetrics() LOCKS_EXCLUDED(mutex_);

  /// Set telemetry client.
  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) LOCKS_EXCLUDED(mutex_);

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
    GcsTaskManagerStorage(size_t max_num_task_events,
                          CounterMapThreadSafe<GcsTaskManagerCounter> &stats_counter)
        : max_num_task_events_(max_num_task_events), stats_counter_(stats_counter) {}

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
    /// This retrieves copies of all task events ordered from the least recently inserted
    /// to the most recently inserted task events.
    ///
    /// \return all task events stored sorted with insertion order.
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

    ///  Mark tasks from a job as failed.
    ///
    /// \param job_id Job ID
    /// \param job_finish_time_ns job finished time in nanoseconds, which will be the task
    /// failed time.
    void MarkTasksFailed(const JobID &job_id, int64_t job_finish_time_ns);

   private:
    /// Mark the task tree containing this task attempt as failure if necessary.
    ///
    /// A task tree is represented by the `parent_task_id` field for each task.
    ///
    /// All non-terminated (not finished nor failed) tasks in the task tree with the task
    /// event as the root should be marked as failed with the root's failed timestamp if
    /// any of:
    ///     1. The task itself fails.
    ///     2. Its parent failed, and neither itself nor its parent is detached actor
    ///     task.
    ///     3. Its parent failed, and both itself and its parent are detached actor task.
    /// TODO(rickyx): Detached actors tasks are now currently marked as failed if parent
    /// dies.
    ///
    /// NOTE: Since there is delay in task events reporting, a task's state might not be
    /// reported before a worker is killed, therefore, some tasks might have actually
    /// run/been submitted but it's states not being reported to GCS. This is hard to
    /// resolve unless we add synchronized reporting before submitting a task.
    ///
    /// \param task_id ID of the task event that's the root of the task tree.
    /// \param parent_task_id ID of the task's parent.
    void MarkTaskTreeFailedIfNeeded(const TaskID &task_id, const TaskID &parent_task_id);

    /// Get a reference to the TaskEvent stored in the buffer.
    ///
    /// \param task_attempt The task attempt.
    /// \return Reference to the task events stored in the buffer.
    rpc::TaskEvents &GetTaskEvent(const TaskAttempt &task_attempt);

    /// Get a const reference to the TaskEvent stored in the buffer.
    ///
    /// \param task_attempt The task attempt.
    /// \return Reference to the task events stored in the buffer.
    const rpc::TaskEvents &GetTaskEvent(const TaskAttempt &task_attempt) const;

    /// Get the timestamp of a task status update.
    ///
    /// \param task_id The task id of the task.
    /// \param task_status The status update.
    /// \return The failed timestamp of the task attempt if it fails. absl::nullopt if the
    /// latest task attempt could not be found due to data loss or the task attempt
    /// doesn't fail.
    absl::optional<int64_t> GetTaskStatusUpdateTime(
        const TaskID &task_id, const rpc::TaskStatus &task_status) const;

    ///  Return if task has terminated.
    ///
    /// \param task_id Task id
    /// \return True if the task has finished or failed timestamp sets, false otherwise.
    bool IsTaskTerminated(const TaskID &task_id) const;

    /// Mark the task as failure with the failed timestamp.
    ///
    /// This also overwrites the finished state of the task if the task has finished by
    /// clearing the finished timestamp.
    ///
    /// \param task_id The task to mark as failed.
    /// \param failed_ts The failure timestamp that's the same from parent's failure
    /// timestamp.
    void MarkTaskFailed(const TaskID &task_id, int64_t failed_ts);

    ///  Mark a task attempt as failed.
    ///
    /// \param task_attempt Task attempt.
    /// \param failed_ts The failure timestamp.
    void MarkTaskAttemptFailed(const TaskAttempt &task_attempt, int64_t failed_ts);

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

    /// Reference to the counter map owned by the GcsTaskManager.
    CounterMapThreadSafe<GcsTaskManagerCounter> &stats_counter_;

    friend class GcsTaskManager;
    FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
    FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
  };

 private:
  /// Test only
  size_t GetTotalNumStatusTaskEventsDropped() {
    return stats_counter_.Get(kTotalNumStatusTaskEventsDropped);
  }

  /// Test only
  size_t GetTotalNumProfileTaskEventsDropped() {
    return stats_counter_.Get(kTotalNumProfileTaskEventsDropped);
  }

  /// Test only
  size_t GetTotalNumTaskEventsReported() {
    return stats_counter_.Get(kTotalNumTaskEventsReported);
  }

  /// Test only
  size_t GetNumTaskEventsStored() { return stats_counter_.Get(kNumTaskEventsStored); }

  // Mutex guarding the usage stats client
  absl::Mutex mutex_;

  UsageStatsClient *usage_stats_client_ GUARDED_BY(mutex_) = nullptr;

  /// Counter map for GcsTaskManager stats.
  CounterMapThreadSafe<GcsTaskManagerCounter> stats_counter_;

  // Pointer to the underlying task events storage. This is only accessed from
  // the io_service_thread_. Access to it is *not* thread safe.
  std::unique_ptr<GcsTaskManagerStorage> task_event_storage_;

  /// Its own separate IO service separated from the main service.
  instrumented_io_context io_service_;

  /// Its own IO thread from the main thread.
  std::unique_ptr<std::thread> io_service_thread_;

  /// Timer for delay functions.
  boost::asio::deadline_timer timer_;

  FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
  FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
  FRIEND_TEST(GcsTaskManagerTest, TestJobFinishesFailAllRunningTasks);
};

}  // namespace gcs
}  // namespace ray
