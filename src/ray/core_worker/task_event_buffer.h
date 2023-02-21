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
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

namespace worker {

class TaskEventBufferImpl;
class TaskEventThreadBuffer;

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

  /// Convert itself a rpc::TaskEvents or drop itself due to data limit.
  ///
  /// NOTE: this method will modify internal states by moving fields to the
  /// rpc::TaskEvents.
  /// \param[out] rpc_task_events The rpc task event to be filled.
  /// \return If it's dropped due to data limit.
  virtual bool ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) = 0;

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
  explicit TaskStatusEvent(
      TaskID task_id,
      JobID job_id,
      int32_t attempt_number,
      const rpc::TaskStatus &task_status,
      int64_t timestamp,
      const std::shared_ptr<const TaskSpecification> &task_spec = nullptr,
      absl::optional<NodeID> node_id = absl::nullopt,
      absl::optional<WorkerID> worker_id = absl::nullopt);

  bool ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) override;

  bool IsProfileEvent() const override { return false; }

 private:
  friend class TaskEventBufferImpl;
  /// The task status change if it's a status change event.
  const rpc::TaskStatus task_status_ = rpc::TaskStatus::NIL;
  /// The time when the task status change happens.
  const int64_t timestamp_ = -1;
  /// Pointer to the task spec.
  const std::shared_ptr<const TaskSpecification> task_spec_ = nullptr;
  /// Node id if it's a SUBMITTED_TO_WORKER status change.
  const absl::optional<NodeID> node_id_ = absl::nullopt;
  /// Worker id if it's a SUBMITTED_TO_WORKER status change.
  const absl::optional<WorkerID> worker_id_ = absl::nullopt;
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
  friend class TaskEventBufferImpl;
  /// The below fields mirror rpc::ProfileEvent
  const std::string component_type_;
  const std::string component_id_;
  const std::string node_ip_address_;
  const std::string event_name_;
  const int64_t start_time_;
  int64_t end_time_;
  std::string extra_data_;
};

/// @brief A per thread buffer that stores TaskEvent.
///
/// This class essentially contains vectors for the different categories of TaskEvent.
/// Each thread of CoreWorker will populate the buffers while submitting/executing tasks.
///
/// This class is **not** thread safe, access should be guarded by sync primitives.
class TaskEventThreadBuffer {
 public:
  TaskEventThreadBuffer()
      : status_events_(std::make_unique<std::vector<TaskStatusEvent>>()),
        profile_events_(std::make_unique<std::vector<TaskProfileEvent>>()) {}

  /// Add a TaskStatusEvent to the local buffer.
  void AddTaskStatusEvent(TaskStatusEvent e) { status_events_->push_back(std::move(e)); }

  /// Add a TaskProfileEvent to the local buffer.
  void AddTaskProfileEvent(TaskProfileEvent e) {
    profile_events_->push_back(std::move(e));
  }

  /// Swap the local TaskStatusEvent buffer.
  ///
  /// \return A pointer to the buffer with events stored.
  std::unique_ptr<std::vector<TaskStatusEvent>> ResetStatusEventBuffer() {
    auto new_buffer = std::make_unique<std::vector<TaskStatusEvent>>();
    new_buffer->reserve(kInitialTaskEventThreadBufferSize);
    new_buffer.swap(status_events_);
    return new_buffer;
  }

  /// Swap the local TaskProfileEvent buffer.
  ///
  /// \return A pointer to the buffer with events stored.
  std::unique_ptr<std::vector<TaskProfileEvent>> ResetProfileEventBuffer() {
    auto new_buffer = std::make_unique<std::vector<TaskProfileEvent>>();
    new_buffer->reserve(kInitialTaskEventThreadBufferSize);
    new_buffer.swap(profile_events_);
    return new_buffer;
  }

 private:
  /// Number of TaskEvents reserved for each buffer for optimization.
  static constexpr size_t kInitialTaskEventThreadBufferSize = 1024;

  /// Buffer storing TaskStatusEvents.
  std::unique_ptr<std::vector<TaskStatusEvent>> status_events_;

  /// Buffer storing TaskProfileEvents.
  std::unique_ptr<std::vector<TaskProfileEvent>> profile_events_;
};

/// @brief An enum class defining counters to be used in TaskEventBufferImpl.
enum TaskEventBufferCounter {
  kNumTaskProfileEventDroppedSinceLastFlush,
  kNumTaskStatusEventDroppedSinceLastFlush,
  kNumTaskEventsStored,
  /// Below stats are updated every flush.
  kTotalNumTaskProfileEventDropped,
  kTotalNumTaskStatusEventDropped,
  kTotalTaskEventsReported,
  kTotalTaskEventsBytesReported,
};

/// An interface for a buffer that stores task status changes and profiling events,
/// and reporting these events to the GCS periodically.
///
/// Adding of task events
/// ========================
/// Task events are generated when executing/submitting tasks on CoreWorker from multiple
/// threads. These task events (TaskEvent) will first be added to a thread-local buffer,
/// and be taken out every `RAY_task_events_report_interval_ms` into a circular buffer by
/// the flushing thread. If the buffer is full (more than
/// `RAY_task_events_worker_buffer_size`) are in the buffer, old task events will be
/// dropped in a FIFO fashion.
///
/// Dropping of task events
/// TODO(rickyx): use a per-task-attempt GC policy.
/// ========================
/// Task events will be lost in the below cases for now:
///   1. If any of the gRPC call failed, the task events will be dropped and warnings
///   logged. This is probably fine since this usually indicated a much worse issue.
///
///   2. More than `RAY_task_events_worker_buffer_size` tasks have been stored
///   in the buffer, any new task events will be dropped. In this case, the number of
///   dropped task events will also be included in the next flush to surface this.
///
///   3. More than `RAY_task_events_max_num_profile_events_for_task` for a task of an
///   attempt has been recorded in the core worker buffer, subsequent TaskProfileEvent
///   will be dropped before it's flushed.
///
/// No overloading of GCS
/// =====================
/// The config `task_events_send_batch_size` controls how many rpc::TaskEvents are
/// reported to GCS each flush. This limits the data for each flush. If GCS failed to
/// respond quickly enough to the previous flush, reporting of events to GCS will be
/// delayed until GCS replies the gRPC in future intervals.
class TaskEventBuffer {
 public:
  virtual ~TaskEventBuffer() = default;

  /// Add a task status event to be reported.
  ///
  /// \param event Task status event.
  virtual void AddTaskStatusEvent(TaskStatusEvent event) = 0;

  /// Add a task profile event to be reported.
  ///
  /// \param event Task profile event.
  virtual void AddTaskProfileEvent(TaskProfileEvent event) = 0;

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
  TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client);

  void AddTaskStatusEvent(TaskStatusEvent e) override;

  void AddTaskProfileEvent(TaskProfileEvent e) override;

  void FlushEvents(bool forced) LOCKS_EXCLUDED(mutex_) override;

  Status Start(bool auto_flush = true) LOCKS_EXCLUDED(mutex_) override;

  void Stop() LOCKS_EXCLUDED(mutex_) override;

  bool Enabled() const override;

  const std::string DebugString() LOCKS_EXCLUDED(mutex_) override;

 private:
  /// @brief Get or initialize the thread buffer.
  /// @return Reference to the thread buffer.
  TaskEventThreadBuffer &GetOrCreateThreadBuffer() SHARED_LOCKS_REQUIRED(buf_map_mutex_);

  /// @brief Gather all the task events stored in the threaded buffer into the circular
  /// buffer `buffer_`.
  void GatherThreadBuffer() LOCKS_EXCLUDED(buf_map_mutex_, mutex_);

  /// Test only functions.
  size_t GetNumTaskEventsStored() {
    GatherThreadBuffer();
    return stats_counter_.Get(TaskEventBufferCounter::kNumTaskEventsStored);
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
  gcs::GcsClient *GetGcsClient() {
    absl::MutexLock lock(&mutex_);
    return gcs_client_.get();
  }

  /// Mutex guarding buffer_ and other internal fields.
  absl::Mutex mutex_;

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
  boost::circular_buffer<std::unique_ptr<rpc::TaskEvents>> buffer_ GUARDED_BY(mutex_);

  /// Mutex guarding `all_thd_buffer_`.
  absl::Mutex buf_map_mutex_;

  /// Per thread buffer.
  absl::flat_hash_map<std::thread::id, TaskEventThreadBuffer> all_thd_buffer_;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  std::atomic<bool> grpc_in_progress_ = false;

  CounterMapThreadSafe<TaskEventBufferCounter> stats_counter_;

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
