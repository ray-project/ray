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
#include "ray/gcs/pb_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

enum GcsTaskManagerCounter {
  kTotalNumTaskEventsReported,
  kTotalNumTaskAttemptsDropped,
  kTotalNumProfileTaskEventsDropped,
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

class TaskEventsGcPolicyInterface {
 public:
  virtual ~TaskEventsGcPolicyInterface() = default;
  /// Return the max priority of the task events under this policy.
  /// A numerically higher priority means the task events will be evicted later.
  virtual size_t MaxPriority() const = 0;

  /// Return the priority of the task events.
  virtual size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const = 0;
};

class FinishedTaskActorTaskGcPolicy : public TaskEventsGcPolicyInterface {
 public:
  size_t MaxPriority() const { return 3; }

  size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const {
    if (IsTaskFinished(task_events)) {
      return 0;
    }

    if (IsActorTask(task_events)) {
      return 1;
    }

    return 2;
  }
};

/// GcsTaskManger is responsible for capturing task states change reported by
/// TaskEventBuffer from other components.
///
/// When the maximal number of task events tracked specified by
/// `RAY_task_events_max_num_task_in_gcs` is exceeded, older events (approximately by
/// insertion order) will be dropped.
///
/// This class has its own io_context and io_thread, that's separate from other GCS
/// services. All handling of all rpc should be posted to the single thread it owns.
class GcsTaskManager : public rpc::TaskInfoHandler {
 public:
  /// Create a GcsTaskManager.
  GcsTaskManager()
      : stats_counter_(),
        task_event_storage_(std::make_unique<GcsTaskManagerStorage>(
            RayConfig::instance().task_events_max_num_task_in_gcs(),
            stats_counter_,
            std::make_unique<FinishedTaskActorTaskGcPolicy>())),
        io_service_thread_(std::make_unique<std::thread>([this] {
          SetThreadName("task_events");
          // Keep io_service_ alive.
          boost::asio::io_service::work io_service_work_(io_service_);
          io_service_.run();
        })),
        periodical_runner_(io_service_) {
    periodical_runner_.RunFnPeriodically([this] { task_event_storage_->GcJobSummary(); },
                                         5 * 1000,
                                         "GcsTaskManager.GcJobSummary");
  }

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

  /// Handler to be called when a worker is dead. This marks all non-terminated tasks
  /// of the worker as failed.
  ///
  /// \param worker_id Worker Id
  /// \param worker_failure_data Worker failure data.
  void OnWorkerDead(const WorkerID &worker_id,
                    const std::shared_ptr<rpc::WorkerTableData> &worker_failure_data);

  /// Returns the io_service.
  ///
  /// \return Reference to its io_service.
  instrumented_io_context &GetIoContext() { return io_service_; }

  /// Return string of debug state.
  ///
  /// \return Debug string
  std::string DebugString();

  /// Record metrics.
  void RecordMetrics() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Set telemetry client.
  void SetUsageStatsClient(UsageStatsClient *usage_stats_client)
      ABSL_LOCKS_EXCLUDED(mutex_);

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
  /// the storage, tasks with lower gc priority as specified by
  /// `TaskEventGcPolicyInterface` will be evicted first. When new events from the
  /// already evicted task attempts are reported to GCS, those events will also be
  /// dropped.
  class GcsTaskManagerStorage {
    class TaskEventLocator;
    class JobTaskSummary;

   public:
    /// Constructor
    ///
    /// \param max_num_task_events Max number of task events stored before replacing older
    /// ones.
    GcsTaskManagerStorage(size_t max_num_task_events,
                          CounterMapThreadSafe<GcsTaskManagerCounter> &stats_counter,
                          std::unique_ptr<TaskEventsGcPolicyInterface> gc_policy)
        : max_num_task_events_(max_num_task_events),
          stats_counter_(stats_counter),
          gc_policy_(std::move(gc_policy)),
          task_events_list_(gc_policy_->MaxPriority(), std::list<rpc::TaskEvents>()) {}

    /// Add a new task event or replace an existing task event in the storage.
    ///
    /// If there are already `RAY_task_events_max_num_task_in_gcs` in the storage, the
    /// oldest task event will be replaced. Otherwise the `task_event` will be added.
    ///
    /// \param task_event Task event to be added to the storage.
    /// replaced task event.
    void AddOrReplaceTaskEvent(rpc::TaskEvents &&task_event);

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

    /// Get task events of task locators.
    ///
    /// \param task_attempts Task attempts (task ids + attempt number).
    /// \return task events from the `task_attempts`.
    std::vector<rpc::TaskEvents> GetTaskEvents(
        const absl::flat_hash_set<std::shared_ptr<TaskEventLocator>> &task_locators)
        const;

    ///  Mark tasks from a job as failed as job ends with a delay.
    ///
    /// \param job_id Job ID
    /// \param job_finish_time_ns job finished time in nanoseconds, which will be the task
    /// failed time.
    void MarkTasksFailedOnJobEnds(const JobID &job_id, int64_t job_finish_time_ns);

    /// Mark tasks from a worker as failed as worker dies.
    ///
    /// \param worker_id Worker ID
    /// \param worker_failure_data Worker failure data.
    void MarkTasksFailedOnWorkerDead(const WorkerID &worker_id,
                                     const rpc::WorkerTableData &worker_failure_data);

    /// Get the job task summary given a job id.
    ///
    /// Caller should make sure the job id exists by calling HasJob() first.
    ///
    /// \param job_id Job ID.
    const JobTaskSummary &GetJobTaskSummary(const JobID &job_id) const {
      auto it = job_task_summary_.find(job_id);
      RAY_CHECK(it != job_task_summary_.end());
      return it->second;
    }

    void UpdateJobSummaryOnJobDone(const JobID &job_id) {
      auto it = job_task_summary_.find(job_id);
      if (it == job_task_summary_.end()) {
        return;
      }
      it->second.OnJobEnds();
    }

    void GcJobSummary() {
      for (auto &job_summary : job_task_summary_) {
        job_summary.second.GcOldDroppedTaskAttempts(job_summary.first);
      }
    }

    /// Return if a job exists in the storage.
    bool HasJob(const JobID &job_id) const {
      auto it = job_task_summary_.find(job_id);
      return it != job_task_summary_.end();
    }

    /// Return total number of profile events dropped from all jobs.
    size_t NumProfileEventsDropped() const {
      size_t num_profile_events_dropped = 0;
      for (const auto &job_summary : job_task_summary_) {
        num_profile_events_dropped += job_summary.second.NumProfileEventsDropped();
      }
      return num_profile_events_dropped;
    }

    /// Return total number of task attempts dropped from all jobs.
    size_t NumTaskAttemptsDropped() const {
      size_t num_task_attempts_dropped = 0;
      for (const auto &job_summary : job_task_summary_) {
        num_task_attempts_dropped += job_summary.second.NumTaskAttemptsDropped();
      }
      return num_task_attempts_dropped;
    }

   private:
    /// A helper class to locate a task event in the storage.
    ///
    /// Task events of each task attempt is stored in multiple lists in the storage. Each
    /// list has a different GC priority, i.e. if the storage is full (in terms of task
    /// attempts tracked), it will evict task events from the list with the lowest GC
    /// priority. The GC priority and the number of task lists is specified by the
    /// `TaskEventsGcPolicyInterface`.
    ///
    /// Each locator contains the iterator to the list and the index of the list.
    /// - When a task event is added to the storage, a locator is created and added to the
    /// indices.
    /// - When a task event is removed from the storage, the locator is removed from the
    /// indices.
    /// - When a task event is updated, it might move between different lists, and the
    /// locator will be updated accordingly.
    class TaskEventLocator {
     public:
      TaskEventLocator(std::list<rpc::TaskEvents>::iterator iter, size_t task_list_index)
          : iter_(iter), task_list_index_(task_list_index) {}

      rpc::TaskEvents &GetTaskEventsMutable() const { return *iter_; }

      size_t GetCurrentListIndex() const { return task_list_index_; }

      std::list<rpc::TaskEvents>::iterator GetCurrentListIterator() const {
        return iter_;
      }

      void SetCurrentList(size_t cur_list_index,
                          std::list<rpc::TaskEvents>::iterator cur_list_iter) {
        iter_ = cur_list_iter;
        task_list_index_ = cur_list_index;
      }

     private:
      /// Iterator to the task list.
      std::list<rpc::TaskEvents>::iterator iter_;
      /// Index of the task list.
      size_t task_list_index_;
    };

    /// A helper class to summarize the stats of a job.
    /// TODO: we could probably do source side summary here per job.
    ///
    /// This class contains stats of:
    /// - Number of task attempts dropped, it's used to determine if task events should be
    /// dropped if data from the task attempt is being already dropped.
    /// - Number of profile events dropped.
    class JobTaskSummary {
     public:
      /// Record a task attempt as dropped.
      ///
      /// \param task_attempt Task attempt.
      void RecordTaskAttemptDropped(const TaskAttempt &task_attempt) {
        dropped_task_attempts_.insert(task_attempt);
        num_task_attempts_dropped_tracked_ = dropped_task_attempts_.size();
      }

      /// Record a number of profile event as dropped.
      void RecordProfileEventsDropped(int32_t cnt) { num_profile_events_dropped_ += cnt; }

      /// Return if a task attempt should be dropped.
      ///
      /// A task attempt should be dropped if some task events from the attempt are
      /// already dropped.
      bool ShouldDropTaskAttempt(const TaskAttempt &task_attempt) const {
        return dropped_task_attempts_.count(task_attempt) > 0;
      }

      size_t NumProfileEventsDropped() const { return num_profile_events_dropped_; }

      size_t NumTaskAttemptsDropped() const {
        return num_task_attempts_dropped_tracked_ + num_dropped_task_attempts_evicted_;
      }

      /// GC the currently tracked dropped task attempts.
      void GcOldDroppedTaskAttempts(const JobID &job_id);

      /// Callback when job is finished.
      ///
      /// When a job is finished, there will be no more task events from the job. So we
      /// can clear the cached dropped task attempts.
      void OnJobEnds() { dropped_task_attempts_.clear(); }

     private:
      int64_t num_profile_events_dropped_ = 0;

      int64_t num_task_attempts_dropped_tracked_ = 0;

      int64_t num_dropped_task_attempts_evicted_ = 0;

      // A set of task attempts that are already being dropped.
      absl::flat_hash_set<TaskAttempt> dropped_task_attempts_;

      FRIEND_TEST(GcsTaskManagerTest, TestMultipleJobsDataLoss);
      FRIEND_TEST(GcsTaskManagerDroppedTaskAttemptsLimit, TestDroppedTaskAttemptsLimit);
    };

    ///  Mark a task attempt as failed if needed.
    ///
    ///  We only mark a task attempt as failed if it's not already terminated(finished or
    ///  failed).
    ///
    /// \param task_attempt Task attempt.
    /// \param failed_ts The failure timestamp.
    /// \param error_info The error info.
    void MarkTaskAttemptFailedIfNeeded(const std::shared_ptr<TaskEventLocator> &locator,
                                       int64_t failed_ts,
                                       const rpc::RayErrorInfo &error_info);

    /// Update or init a task event locator for the task events.
    ///
    /// \param events_by_task Task events.
    /// \return The task event locator.
    std::shared_ptr<TaskEventLocator> UpdateOrInitTaskEventLocator(
        rpc::TaskEvents &&events_by_task);

    /// Update an existing task attempt given the locator and the task events.
    ///
    /// \param loc The task event locator.
    /// \param task_events The task events updates for the task attempt.
    void UpdateExistingTaskAttempt(const std::shared_ptr<TaskEventLocator> &loc,
                                   const rpc::TaskEvents &task_events);

    /// Add a new task event given the task events to the storage, and
    /// returns a locator to the task event.
    ///
    /// \param events_by_task Task events.
    /// \return The task event locator.
    std::shared_ptr<TaskEventLocator> AddNewTaskEvent(rpc::TaskEvents &&events_by_task);

    /// Add the locator to indices.
    ///
    /// \param loc The task event locator.
    void UpdateIndex(const std::shared_ptr<TaskEventLocator> &loc);

    /// Remove the locator from indices.
    ///
    /// \param loc The locator
    /// \return The task event locator.
    void RemoveFromIndex(const std::shared_ptr<TaskEventLocator> &loc);

    /// Record data loss from a worker.
    /// \param data
    void RecordDataLossFromWorker(const rpc::TaskEventData &data);

    /// Evict task events from the storage when there are too many task events.
    void EvictTaskEvent();

    /// Remove information of a task attempt from the storage.
    void RemoveTaskAttempt(std::shared_ptr<TaskEventLocator> loc);

    /// Test only functions.
    std::shared_ptr<TaskEventLocator> GetTaskEventLocator(
        const TaskAttempt &task_attempt) const {
      return primary_index_.at(task_attempt);
    }

    /// Max number of task events allowed in the storage.
    const size_t max_num_task_events_ = 0;

    /// Reference to the counter map owned by the GcsTaskManager.
    CounterMapThreadSafe<GcsTaskManagerCounter> &stats_counter_;

    // Primary index from task attempt to the locator.
    absl::flat_hash_map<TaskAttempt, std::shared_ptr<TaskEventLocator>> primary_index_;

    // Secondary indices for retrieval.
    absl::flat_hash_map<TaskID, absl::flat_hash_set<std::shared_ptr<TaskEventLocator>>>
        task_index_;
    absl::flat_hash_map<JobID, absl::flat_hash_set<std::shared_ptr<TaskEventLocator>>>
        job_index_;
    absl::flat_hash_map<WorkerID, absl::flat_hash_set<std::shared_ptr<TaskEventLocator>>>
        worker_index_;

    // A summary for per job stats.
    absl::flat_hash_map<JobID, JobTaskSummary> job_task_summary_;

    /// GC policy.
    std::unique_ptr<TaskEventsGcPolicyInterface> gc_policy_;

    /// Task events lists.
    std::vector<std::list<rpc::TaskEvents>> task_events_list_;

    friend class GcsTaskManager;
    FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
    FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
    FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
    FRIEND_TEST(GcsTaskManagerTest, TestMarkTaskAttemptFailedIfNeeded);
    FRIEND_TEST(GcsTaskManagerTest, TestMultipleJobsDataLoss);
    FRIEND_TEST(GcsTaskManagerDroppedTaskAttemptsLimit, TestDroppedTaskAttemptsLimit);
  };

 private:
  /// Record data loss from worker.
  ///
  /// TODO(rickyx): This will be updated to record task attempt loss properly.
  ///
  /// \param data The task event data.
  void RecordDataLossFromWorker(const rpc::TaskEventData &data);

  /// Test only
  size_t GetTotalNumTaskAttemptsDropped() {
    return stats_counter_.Get(kTotalNumTaskAttemptsDropped);
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

  UsageStatsClient *usage_stats_client_ ABSL_GUARDED_BY(mutex_) = nullptr;

  /// Counter map for GcsTaskManager stats.
  CounterMapThreadSafe<GcsTaskManagerCounter> stats_counter_;

  // Pointer to the underlying task events storage. This is only accessed from
  // the io_service_thread_. Access to it is *not* thread safe.
  std::unique_ptr<GcsTaskManagerStorage> task_event_storage_;

  /// Its own separate IO service separated from the main service.
  instrumented_io_context io_service_;

  /// Its own IO thread from the main thread.
  std::unique_ptr<std::thread> io_service_thread_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  FRIEND_TEST(GcsTaskManagerTest, TestHandleAddTaskEventBasic);
  FRIEND_TEST(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents);
  FRIEND_TEST(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak);
  FRIEND_TEST(GcsTaskManagerTest, TestJobFinishesFailAllRunningTasks);
  FRIEND_TEST(GcsTaskManagerTest, TestMarkTaskAttemptFailedIfNeeded);
  FRIEND_TEST(GcsTaskManagerTest, TestTaskDataLossWorker);
  FRIEND_TEST(GcsTaskManagerTest, TestMultipleJobsDataLoss);
  FRIEND_TEST(GcsTaskManagerDroppedTaskAttemptsLimit, TestDroppedTaskAttemptsLimit);
  FRIEND_TEST(GcsTaskManagerProfileEventsLimitTest, TestProfileEventsNoLeak);
};

}  // namespace gcs
}  // namespace ray
