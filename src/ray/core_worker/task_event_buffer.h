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

#include <boost/circular_buffer.hpp>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

namespace worker {

using TaskAttempt = std::pair<TaskID, int32_t>;

/// A  wrapper class that will be converted to rpc::TaskEvents
///
/// This will be created by CoreWorker and stored in TaskEventBuffer, and
/// when it is being flushed periodically to GCS, it will be converted to
/// rpc::TaskEvents.
/// This is an optimization so that converting to protobuf (which is costly)
/// will not happen in the critical path of task execution/submission.
class TaskEvent {
 public:
  /// Constructor for Profile events
  explicit TaskEvent(TaskID task_id, JobID job_id, int32_t attempt_number);

  virtual ~TaskEvent() = default;

  /// Convert itself a rpc::TaskEvents or drop it if there is data loss.
  ///
  /// NOTE: this method will modify internal states by moving fields to the
  /// rpc::TaskEvents.
  /// \param[out] rpc_task_events The rpc task event to be filled.
  /// \return True if data is dropped, false otherwise.
  virtual bool ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) = 0;

  virtual JobID GetJobId() const { return job_id_; }

  /// If it is a profile event.
  virtual bool IsProfileEvent() const = 0;

  virtual TaskAttempt GetTaskAttempt() const {
    return std::make_pair(task_id_, attempt_number_);
  }

 protected:
  /// Task Id.
  const TaskID task_id_ = TaskID::Nil();
  /// Job id.
  const JobID job_id_ = JobID::Nil();
  /// Attempt number
  const int32_t attempt_number_ = -1;
};

/// TaskStatusEvent is generated when a task changes its status.
class TaskStatusEvent : public TaskEvent {
 public:
  /// A class that contain data that will be converted to rpc::TaskStateUpdate
  struct TaskStateUpdate {
    TaskStateUpdate() {}

    TaskStateUpdate(const absl::optional<const rpc::RayErrorInfo> &error_info)
        : error_info_(error_info) {}

    TaskStateUpdate(const NodeID &node_id, const WorkerID &worker_id)
        : node_id_(node_id), worker_id_(worker_id) {}

    TaskStateUpdate(const rpc::TaskLogInfo &task_log_info)
        : task_log_info_(task_log_info) {}

    TaskStateUpdate(const std::string &actor_repr_name, uint32_t pid)
        : actor_repr_name_(actor_repr_name), pid_(pid) {}

    TaskStateUpdate(uint32_t pid) : pid_(pid) {}

   private:
    friend class TaskStatusEvent;

    /// Node id if it's a SUBMITTED_TO_WORKER status change.
    const absl::optional<NodeID> node_id_ = absl::nullopt;
    /// Worker id if it's a SUBMITTED_TO_WORKER status change.
    const absl::optional<WorkerID> worker_id_ = absl::nullopt;
    /// Task error info.
    const absl::optional<rpc::RayErrorInfo> error_info_ = absl::nullopt;
    /// Task log info.
    const absl::optional<rpc::TaskLogInfo> task_log_info_ = absl::nullopt;
    /// Actor task repr name.
    const std::string actor_repr_name_ = "";
    /// Worker's pid if it's a RUNNING status change.
    const absl::optional<uint32_t> pid_ = absl::nullopt;
  };

  explicit TaskStatusEvent(
      TaskID task_id,
      JobID job_id,
      int32_t attempt_number,
      const rpc::TaskStatus &task_status,
      int64_t timestamp,
      const std::shared_ptr<const TaskSpecification> &task_spec = nullptr,
      absl::optional<const TaskStateUpdate> state_update = absl::nullopt);

  bool ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) override;

  bool IsProfileEvent() const override { return false; }

 private:
  /// The task status change if it's a status change event.
  const rpc::TaskStatus task_status_ = rpc::TaskStatus::NIL;
  /// The time when the task status change happens.
  const int64_t timestamp_ = -1;
  /// Pointer to the task spec.
  const std::shared_ptr<const TaskSpecification> task_spec_ = nullptr;
  /// Optional task state update
  const absl::optional<const TaskStateUpdate> state_update_ = absl::nullopt;
};

/// TaskProfileEvent is generated when `RAY_enable_timeline` is on.
class TaskProfileEvent : public TaskEvent {
 public:
  explicit TaskProfileEvent(TaskID task_id,
                            JobID job_id,
                            int32_t attempt_number,
                            const std::string &component_type,
                            const std::string &component_id,
                            const std::string &node_ip_address,
                            const std::string &event_name,
                            int64_t start_time);

  bool ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) override;

  bool IsProfileEvent() const override { return true; }

  void SetEndTime(int64_t end_time) { end_time_ = end_time; }

  void SetExtraData(const std::string &extra_data) { extra_data_ = extra_data; }

 private:
  /// The below fields mirror rpc::ProfileEvent
  const std::string component_type_;
  const std::string component_id_;
  const std::string node_ip_address_;
  const std::string event_name_;
  const int64_t start_time_;
  int64_t end_time_;
  std::string extra_data_;
};

/// @brief An enum class defining counters to be used in TaskEventBufferImpl.
enum TaskEventBufferCounter {
  /// Number of task events stored in the buffer.
  kNumTaskEventsStored,
  /// Number of dropped task attempt stored in the buffer.
  kNumTaskAttemptsDroppedStored,
  /// Total number of task events dropped on the worker due to network issue.
  kTotalNumTaskEventsDropped,
  /// Number of profile events dropped since the last report.
  kNumTaskProfileEventDroppedSinceLastFlush,
  /// Total number of task events reported to GCS.
  kTotalNumTaskEventsReported,
  /// Total bytes of task events reported to GCS.
  kTotalTaskEventsBytesReported,
};

/// An interface for a buffer that stores task status changes and profiling events,
/// and reporting these events to the GCS periodically.
///
/// Dropping of task events
/// ========================
/// Task events from task attempts will be lost in the below cases for now:
///   1. If any of the gRPC call failed, the task events will be dropped and warnings
///   logged. This is probably fine since this usually indicated a much worse issue.
///
///   2. More than `RAY_task_events_max_buffer_size` tasks have been stored
///   in the buffer, oldest events in the buffer will be dropped. In this case, the task
///   attempts info will also be included in subsequent flush to GCS.
///
/// For profiling events:
///   - If the number of profiling events for a task attempt exceeds the limit specified
///   by `RAY_task_events_max_num_profile_events_for_task`, any new profiling events will
///   be dropped. Dropping of profile events will not result in the entire task attempt
///   being dropped.
///
/// For task status events:
///   - If any task status change event is dropped, the entire task attempt will be
///   dropped. The dropped task attempt info will be sent to GCS, and GCS will then drop
///   all new and existing events from the task attempt.
///
/// No overloading of GCS
/// =====================
/// If GCS failed to respond quickly enough to the previous report, reporting of events to
/// GCS will be delayed until GCS replies the gRPC in future intervals.
class TaskEventBuffer {
 public:
  virtual ~TaskEventBuffer() = default;

  /// Add a task event to be reported.
  ///
  /// \param task_events Task events.
  virtual void AddTaskEvent(std::unique_ptr<TaskEvent> task_event) = 0;

  /// Flush all task events stored in the buffer to GCS.
  ///
  /// This function will be called periodically configured by
  /// `RAY_task_events_report_interval_ms`, and send task events stored in a buffer to
  /// GCS. If GCS has not responded to a previous flush, it will defer the flushing to
  /// the next interval (if not forced.)
  ///
  /// Before flushing to GCS, events from a single task attempt will also be coalesced
  /// into one rpc::TaskEvents as an optimization.
  ///
  /// \param forced When set to true, buffered events will be sent to GCS even if GCS has
  ///       not responded to the previous flush. A forced flush will be called before
  ///       CoreWorker disconnects to ensure all task events in the buffer are sent.
  virtual void FlushEvents(bool forced) = 0;

  /// Start the TaskEventBuffer.
  ///
  /// Connects the GCS client, starts its io_thread, and sets up periodical runner for
  /// flushing events to GCS.
  /// When it returns non ok status, the TaskEventBuffer will be disabled, and call to
  /// Enabled() will return false.
  ///
  /// \param auto_flush Test only flag to disable periodical flushing events if false.
  /// \return Status code. When the status is not ok, events will not be recorded nor
  /// reported.
  virtual Status Start(bool auto_flush = true) = 0;

  /// Stop the TaskEventBuffer and it's underlying IO, disconnecting GCS clients.
  virtual void Stop() = 0;

  /// Return true if recording and reporting of task events is enabled.
  ///
  /// The TaskEventBuffer will be disabled if Start() returns not ok.
  virtual bool Enabled() const = 0;

  /// Return a string that describes the task event buffer stats.
  virtual const std::string DebugString() = 0;
};

/// Implementation of TaskEventBuffer.
///
/// The buffer has its own io_context and io_thread, that's isolated from other
/// components.
///
/// This class is thread-safe.
class TaskEventBufferImpl : public TaskEventBuffer {
 public:
  /// Constructor
  ///
  /// \param gcs_client GCS client
  /// \param job_id Corresponding Job ID
  TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client, const JobID &job_id);

  ~TaskEventBufferImpl() override;

  void AddTaskEvent(std::unique_ptr<TaskEvent> task_event)
      LOCKS_EXCLUDED(mutex_) override;

  void FlushEvents(bool forced) LOCKS_EXCLUDED(mutex_) override;

  Status Start(bool auto_flush = true) LOCKS_EXCLUDED(mutex_) override;

  void Stop() LOCKS_EXCLUDED(mutex_) override;

  bool Enabled() const override;

  const std::string DebugString() override;

 private:
  /// Test only functions.
  size_t GetNumTaskEventsStored() {
    return stats_counter_.Get(TaskEventBufferCounter::kNumTaskEventsStored);
  }

  /// Test only functions.
  size_t GetNumTaskEventsDropped() {
    return stats_counter_.Get(TaskEventBufferCounter::kTotalNumTaskEventsDropped);
  }

  /// Test only function.
  size_t GetNumTaskEventsReported() {
    return stats_counter_.Get(TaskEventBufferCounter::kTotalNumTaskEventsReported);
  }

  /// Test only function.
  size_t GetNumProfileTaskEventsDroppedSinceLastFlush() {
    return stats_counter_.Get(
        TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
  }

  /// Test only functions.
  gcs::GcsClient *GetGcsClient() {
    absl::MutexLock lock(&mutex_);
    return gcs_client_.get();
  }

  /// Test only functions.
  const JobID &GetJobId() const { return job_id_; }

  /// Mutex guarding task_events_data_.
  absl::Mutex mutex_;

  /// Job id.
  const JobID job_id_;

  /// IO service event loop owned by TaskEventBuffer.
  instrumented_io_context io_service_;

  /// Work guard to prevent the io_context from exiting when no work.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;

  /// Dedicated io thread for running the periodical runner and the GCS client.
  std::thread io_thread_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// Client to the GCS used to push profile events to it.
  std::unique_ptr<gcs::GcsClient> gcs_client_ GUARDED_BY(mutex_);

  /// True if the TaskEventBuffer is enabled.
  std::atomic<bool> enabled_ = false;

  /// Circular buffered task events.
  boost::circular_buffer<std::unique_ptr<TaskEvent>> buffer_ GUARDED_BY(mutex_);

  /// Stats counter map.
  CounterMapThreadSafe<TaskEventBufferCounter> stats_counter_;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  std::atomic<bool> grpc_in_progress_ = false;

  /// Task attempts dropped on this worker that are to be reported to GCS. Reported
  /// data loss will be removed.
  absl::flat_hash_set<TaskAttempt> task_attempts_dropped_ GUARDED_BY(mutex_);

  FRIEND_TEST(TaskEventBufferTestManualStart, TestGcsClientFail);
  FRIEND_TEST(TaskEventBufferTestBatchSend, TestBatchedSend);
  FRIEND_TEST(TaskEventBufferTest, TestAddEvent);
  FRIEND_TEST(TaskEventBufferTest, TestFlushEvents);
  FRIEND_TEST(TaskEventBufferTest, TestFailedFlush);
  FRIEND_TEST(TaskEventBufferTest, TestBackPressure);
  FRIEND_TEST(TaskEventBufferTest, TestForcedFlush);
  FRIEND_TEST(TaskEventBufferTest, TestBufferSizeLimit);
  FRIEND_TEST(TaskEventBufferTestLimitProfileEvents, TestLimitProfileEventsPerTask);
};

}  // namespace worker

}  // namespace core
}  // namespace ray
