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
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/counter_map.h"
#include "ray/util/event.h"
#include "src/ray/protobuf/export_task_event.pb.h"
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

  /// Convert itself a rpc::TaskEvents
  ///
  /// NOTE: this method will modify internal states by moving fields to the
  /// rpc::TaskEvents.
  /// \param[out] rpc_task_events The rpc task event to be filled.
  virtual void ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) = 0;

  /// Convert itself a rpc::ExportTaskEventData
  ///
  /// \param[out] rpc_task_export_event_data The rpc export task event data to be filled.
  virtual void ToRpcTaskExportEvents(
      std::shared_ptr<rpc::ExportTaskEventData> rpc_task_export_event_data) = 0;

  /// If it is a profile event.
  virtual bool IsProfileEvent() const = 0;

  virtual TaskAttempt GetTaskAttempt() const {
    return std::make_pair(task_id_, attempt_number_);
  }

 protected:
  /// Task Id.
  TaskID task_id_ = TaskID::Nil();
  /// Job id.
  JobID job_id_ = JobID::Nil();
  /// Attempt number
  int32_t attempt_number_ = -1;
};

/// TaskStatusEvent is generated when a task changes its status.
class TaskStatusEvent : public TaskEvent {
 public:
  /// A class that contain data that will be converted to rpc::TaskStateUpdate
  struct TaskStateUpdate {
    TaskStateUpdate() = default;

    explicit TaskStateUpdate(const std::optional<const rpc::RayErrorInfo> &error_info)
        : error_info_(error_info) {}

    TaskStateUpdate(const NodeID &node_id, const WorkerID &worker_id)
        : node_id_(node_id), worker_id_(worker_id) {}

    explicit TaskStateUpdate(rpc::TaskLogInfo task_log_info)
        : task_log_info_(std::move(task_log_info)) {}

    TaskStateUpdate(std::string actor_repr_name, uint32_t pid)
        : actor_repr_name_(std::move(actor_repr_name)), pid_(pid) {}

    explicit TaskStateUpdate(uint32_t pid) : pid_(pid) {}

    explicit TaskStateUpdate(bool is_debugger_paused)
        : is_debugger_paused_(is_debugger_paused) {}

   private:
    friend class TaskStatusEvent;

    /// Node id if it's a SUBMITTED_TO_WORKER status change.
    std::optional<NodeID> node_id_ = std::nullopt;
    /// Worker id if it's a SUBMITTED_TO_WORKER status change.
    std::optional<WorkerID> worker_id_ = std::nullopt;
    /// Task error info.
    std::optional<rpc::RayErrorInfo> error_info_ = std::nullopt;
    /// Task log info.
    std::optional<rpc::TaskLogInfo> task_log_info_ = std::nullopt;
    /// Actor task repr name.
    std::string actor_repr_name_;
    /// Worker's pid if it's a RUNNING status change.
    std::optional<uint32_t> pid_ = std::nullopt;
    /// If the task is paused by the debugger.
    std::optional<bool> is_debugger_paused_ = std::nullopt;
  };

  explicit TaskStatusEvent(
      TaskID task_id,
      JobID job_id,
      int32_t attempt_number,
      const rpc::TaskStatus &task_status,
      int64_t timestamp,
      const std::shared_ptr<const TaskSpecification> &task_spec = nullptr,
      std::optional<const TaskStateUpdate> state_update = std::nullopt);

  void ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) override;

  void ToRpcTaskExportEvents(
      std::shared_ptr<rpc::ExportTaskEventData> rpc_task_export_event_data) override;

  bool IsProfileEvent() const override { return false; }

 private:
  /// The task status change if it's a status change event.
  rpc::TaskStatus task_status_ = rpc::TaskStatus::NIL;
  /// The time when the task status change happens.
  int64_t timestamp_ = -1;
  /// Pointer to the task spec.
  std::shared_ptr<const TaskSpecification> task_spec_ = nullptr;
  /// Optional task state update
  std::optional<const TaskStateUpdate> state_update_ = std::nullopt;
};

/// TaskProfileEvent is generated when `RAY_enable_timeline` is on.
class TaskProfileEvent : public TaskEvent {
 public:
  TaskProfileEvent(TaskID task_id,
                   JobID job_id,
                   int32_t attempt_number,
                   std::string component_type,
                   std::string component_id,
                   std::string node_ip_address,
                   std::string event_name,
                   int64_t start_time);

  void ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) override;

  void ToRpcTaskExportEvents(
      std::shared_ptr<rpc::ExportTaskEventData> rpc_task_export_event_data) override;

  bool IsProfileEvent() const override { return true; }

  void SetEndTime(int64_t end_time) { end_time_ = end_time; }

  void SetExtraData(const std::string &extra_data) { extra_data_ = extra_data; }

 private:
  /// The below fields mirror rpc::ProfileEvent
  std::string component_type_;
  std::string component_id_;
  std::string node_ip_address_;
  std::string event_name_;
  int64_t start_time_{};
  int64_t end_time_{};
  std::string extra_data_;
};

/// @brief An enum class defining counters to be used in TaskEventBufferImpl.
enum TaskEventBufferCounter {
  kNumTaskProfileEventDroppedSinceLastFlush,
  kNumTaskStatusEventDroppedSinceLastFlush,
  kNumTaskProfileEventsStored,
  kNumTaskStatusEventsStored,
  kNumDroppedTaskAttemptsStored,
  kNumTaskStatusEventsForExportAPIStored,
  kTotalNumTaskProfileEventDropped,
  kTotalNumTaskStatusEventDropped,
  kTotalNumTaskAttemptsReported,
  kTotalNumLostTaskAttemptsReported,
  kTotalTaskEventsBytesReported,
  kTotalNumFailedToReport,
};

/// An interface for a buffer that stores task status changes and profiling events,
/// and reporting these events to the GCS periodically.
///
/// Dropping of task events
/// ========================
/// Task events will be lost in the below cases for now:
///   1. If any of the gRPC call failed, the task events will be dropped and warnings
///   logged. This is probably fine since this usually indicated a much worse issue.
///
///   2. More than `RAY_task_events_max_num_status_events_buffer_on_worker` tasks have
///   been stored in the buffer, any new task events will be dropped. In this case, the
///   number of dropped task events will also be included in the next flush to surface
///   this.
///
/// No overloading of GCS
/// =====================
/// If GCS failed to respond quickly enough to the previous report, reporting of events to
/// GCS will be delayed until GCS replies the gRPC in future intervals.
class TaskEventBuffer {
 public:
  virtual ~TaskEventBuffer() = default;

  /// Update task status change for the task attempt in TaskEventBuffer if needed.
  ///
  /// It skips the reporting when:
  ///   1. when the enable_task_events for the task is false in TaskSpec.
  ///   2. when the task event reporting is disabled on the worker (through ray config,
  ///   i.e., RAY_task_events_report_interval_ms=0).
  ///
  /// \param attempt_number Attempt number for the task attempt.
  /// \param spec corresponding TaskSpecification of the task
  /// \param status the changed status.
  /// \param state_update optional task state updates.
  /// \return true if the event is recorded, false otherwise.
  bool RecordTaskStatusEventIfNeeded(
      const TaskID &task_id,
      const JobID &job_id,
      int32_t attempt_number,
      const TaskSpecification &spec,
      rpc::TaskStatus status,
      bool include_task_info = false,
      std::optional<const TaskStatusEvent::TaskStateUpdate> state_update = absl::nullopt);

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
  virtual std::string DebugString() = 0;
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
  explicit TaskEventBufferImpl(std::shared_ptr<gcs::GcsClient> gcs_client);

  TaskEventBufferImpl(const TaskEventBufferImpl &) = delete;
  TaskEventBufferImpl &operator=(const TaskEventBufferImpl &) = delete;

  ~TaskEventBufferImpl() override;

  void AddTaskEvent(std::unique_ptr<TaskEvent> task_event)
      ABSL_LOCKS_EXCLUDED(mutex_) override;

  void FlushEvents(bool forced) ABSL_LOCKS_EXCLUDED(mutex_) override;

  Status Start(bool auto_flush = true) ABSL_LOCKS_EXCLUDED(mutex_) override;

  void Stop() ABSL_LOCKS_EXCLUDED(mutex_) override;

  bool Enabled() const override;

  std::string DebugString() override;

 private:
  /// Add a task status event to be reported.
  ///
  /// \param status_event Task status event.
  void AddTaskStatusEvent(std::unique_ptr<TaskEvent> status_event)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Add a task profile event to be reported.
  ///
  /// \param profile_event Task profile event.
  void AddTaskProfileEvent(std::unique_ptr<TaskEvent> profile_event)
      ABSL_LOCKS_EXCLUDED(profile_mutex_);

  /// Get data related to task status events to be send to GCS.
  ///
  /// \param[out] status_events_to_send Task status events to be sent.
  /// \param[out] status_events_to_write_for_export Task status events that will
  ///              be written to the Export API. This includes both status events
  ///              that are sent to GCS, and as many dropped status events that
  ///              fit in the buffer.
  /// \param[out] dropped_task_attempts_to_send Task attempts that were dropped due to
  ///             status events being dropped.
  void GetTaskStatusEventsToSend(
      std::vector<std::shared_ptr<TaskEvent>> *status_events_to_send,
      std::vector<std::shared_ptr<TaskEvent>> *status_events_to_write_for_export,
      absl::flat_hash_set<TaskAttempt> *dropped_task_attempts_to_send)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get data related to task profile events to be send to GCS.
  ///
  /// \param[out] profile_events_to_send Task profile events to be sent.
  void GetTaskProfileEventsToSend(
      std::vector<std::shared_ptr<TaskEvent>> *profile_events_to_send)
      ABSL_LOCKS_EXCLUDED(profile_mutex_);

  /// Get the task events to GCS.
  ///
  /// \param status_events_to_send Task status events to be sent.
  /// \param profile_events_to_send Task profile events to be sent.
  /// \param dropped_task_attempts_to_send Task attempts that were dropped due to
  ///        status events being dropped.
  /// \return A unique_ptr to rpc::TaskEvents to be sent to GCS.
  std::unique_ptr<rpc::TaskEventData> CreateDataToSend(
      const std::vector<std::shared_ptr<TaskEvent>> &status_events_to_send,
      const std::vector<std::shared_ptr<TaskEvent>> &profile_events_to_send,
      const absl::flat_hash_set<TaskAttempt> &dropped_task_attempts_to_send);

  /// Write task events for the Export API.
  ///
  /// \param status_events_to_write_for_export Task status events that will
  ///              be written to the Export API. This includes both status events
  ///              that are sent to GCS, and as many dropped status events that
  ///              fit in the buffer.
  /// \param profile_events_to_send Task profile events to be written.
  void WriteExportData(
      const std::vector<std::shared_ptr<TaskEvent>> &status_events_to_write_for_export,
      const std::vector<std::shared_ptr<TaskEvent>> &profile_events_to_send);

  // Verify if export events should be written for EXPORT_TASK source types
  bool IsExportAPIEnabledTask() const {
    return IsExportAPIEnabledSourceType(
        "EXPORT_TASK",
        ::RayConfig::instance().enable_export_api_write(),
        ::RayConfig::instance().enable_export_api_write_config());
  }

  /// Reset the counters during flushing data to GCS.
  void ResetCountersForFlush();

  /// Test only functions.
  size_t GetNumTaskEventsStored() {
    return stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventsStored) +
           stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventsStored);
  }

  /// Test only functions.
  size_t GetTotalNumStatusTaskEventsDropped() {
    return stats_counter_.Get(TaskEventBufferCounter::kTotalNumTaskStatusEventDropped);
  }

  /// Test only functions.
  size_t GetNumStatusTaskEventsDroppedSinceLastFlush() {
    return stats_counter_.Get(
        TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
  }

  /// Test only functions.
  size_t GetTotalNumProfileTaskEventsDropped() {
    return stats_counter_.Get(TaskEventBufferCounter::kTotalNumTaskProfileEventDropped);
  }

  /// Test only functions.
  size_t GetNumProfileTaskEventsDroppedSinceLastFlush() {
    return stats_counter_.Get(
        TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
  }

  /// Test only functions.
  size_t GetNumFailedToReport() {
    return stats_counter_.Get(TaskEventBufferCounter::kTotalNumFailedToReport);
  }

  /// Test only functions.
  gcs::GcsClient *GetGcsClient() {
    absl::MutexLock lock(&mutex_);
    return gcs_client_.get();
  }

  /// Mutex guarding task_events_data_.
  absl::Mutex mutex_;

  absl::Mutex profile_mutex_;

  /// IO service event loop owned by TaskEventBuffer.
  instrumented_io_context io_service_{/*enable_lag_probe=*/false,
                                      /*running_on_single_thread=*/true};

  /// Work guard to prevent the io_context from exiting when no work.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;

  /// Dedicated io thread for running the periodical runner and the GCS client.
  std::thread io_thread_;

  /// The runner to run function periodically.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;

  /// Client to the GCS used to push profile events to it.
  std::shared_ptr<gcs::GcsClient> gcs_client_ ABSL_GUARDED_BY(mutex_);

  /// True if the TaskEventBuffer is enabled.
  std::atomic<bool> enabled_ = false;

  /// Circular buffered task status events.
  boost::circular_buffer<std::shared_ptr<TaskEvent>> status_events_
      ABSL_GUARDED_BY(mutex_);

  /// Status events that will be written for the export API. This could
  /// contain events that were dropped from being sent to GCS. A circular
  /// buffer is used to limit memory.
  boost::circular_buffer<std::shared_ptr<TaskEvent>> status_events_for_export_;

  /// Buffered task attempts that were dropped due to status events being dropped.
  /// This will be sent to GCS to surface the dropped task attempts.
  absl::flat_hash_set<TaskAttempt> dropped_task_attempts_unreported_
      ABSL_GUARDED_BY(mutex_);

  /// Buffered task profile events. A FIFO queue to be sent to GCS.
  absl::flat_hash_map<TaskAttempt, std::vector<std::shared_ptr<TaskEvent>>>
      profile_events_ ABSL_GUARDED_BY(profile_mutex_);

  /// Stats counter map.
  CounterMapThreadSafe<TaskEventBufferCounter> stats_counter_;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  std::atomic<bool> grpc_in_progress_ = false;

  /// If true, task events are exported for Export API
  bool export_event_write_enabled_ = false;

  FRIEND_TEST(TaskEventBufferTestManualStart, TestGcsClientFail);
  FRIEND_TEST(TaskEventBufferTestBatchSend, TestBatchedSend);
  FRIEND_TEST(TaskEventBufferTest, TestAddEvent);
  FRIEND_TEST(TaskEventBufferTest, TestFlushEvents);
  FRIEND_TEST(TaskEventBufferTest, TestFailedFlush);
  FRIEND_TEST(TaskEventBufferTest, TestBackPressure);
  FRIEND_TEST(TaskEventBufferTest, TestForcedFlush);
  FRIEND_TEST(TaskEventBufferTestLimitBuffer, TestBufferSizeLimitStatusEvents);
  FRIEND_TEST(TaskEventBufferTestLimitProfileEvents, TestBufferSizeLimitProfileEvents);
  FRIEND_TEST(TaskEventBufferTestLimitProfileEvents, TestLimitProfileEventsPerTask);
  FRIEND_TEST(TaskEventTestWriteExport, TestWriteTaskExportEvents);
};

}  // namespace worker

}  // namespace core
}  // namespace ray
