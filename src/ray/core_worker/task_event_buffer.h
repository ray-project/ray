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
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

namespace worker {

/// An interface for a buffer that stores task status changes and profiling events,
/// and reporting these events to the GCS periodically.
///
/// Dropping of task events
/// ========================
/// Task events will be lost in the below cases for now:
///   1. If any of the gRPC call failed, the task events will be dropped and warnings
///   logged. This is probably fine since this usually indicated a much worse issue.
///
///   2. More than `RAY_task_events_max_num_task_events_in_buffer` tasks have been stored
///   in the buffer, any new task events will be dropped. In this case, the number of
///   dropped task events will also be included in the next flush to surface this.
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
  virtual void AddTaskEvent(rpc::TaskEvents task_events) = 0;

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

  void AddTaskEvent(rpc::TaskEvents task_events) LOCKS_EXCLUDED(mutex_) override;

  void FlushEvents(bool forced) LOCKS_EXCLUDED(mutex_) override;

  Status Start(bool auto_flush = true) LOCKS_EXCLUDED(mutex_) override;

  void Stop() LOCKS_EXCLUDED(mutex_) override;

  bool Enabled() const override;

  const std::string DebugString() LOCKS_EXCLUDED(mutex_) override;

 private:
  /// Test only functions.
  std::vector<rpc::TaskEvents> GetAllTaskEvents() LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    std::vector<rpc::TaskEvents> copy(buffer_);
    return copy;
  }

  /// Test only functions.
  size_t GetNumStatusTaskEventsDropped() LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return num_status_task_events_dropped_;
  }

  /// Test only functions.
  size_t GetNumProfileTaskEventsDropped() LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return num_profile_task_events_dropped_;
  }

  /// Test only functions.
  gcs::GcsClient *GetGcsClient() {
    absl::MutexLock lock(&mutex_);
    return gcs_client_.get();
  }

  /// Mutex guarding task_events_data_.
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

  /// Buffered task events.
  std::vector<rpc::TaskEvents> buffer_ GUARDED_BY(mutex_);

  /// A iterator into buffer_ that determines which element to be overwritten.
  size_t next_idx_to_overwrite_ GUARDED_BY(mutex_) = 0;

  /// Number of profile task events dropped since the last report flush.
  size_t num_profile_task_events_dropped_ GUARDED_BY(mutex_) = 0;

  /// Number of status task events dropped since the last report flush.
  size_t num_status_task_events_dropped_ GUARDED_BY(mutex_) = 0;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  bool grpc_in_progress_ GUARDED_BY(mutex_) = false;

  /// A buffer to store task events to be sent to GCS in batches.
  std::vector<rpc::TaskEvents> send_buffer_ GUARDED_BY(mutex_);

  /// Debug stats: total number of bytes of task events sent so far to GCS.
  uint64_t total_events_bytes_ GUARDED_BY(mutex_) = 0;

  /// Debug stats: total number of task events sent so far to GCS.
  uint64_t total_num_events_ GUARDED_BY(mutex_) = 0;

  FRIEND_TEST(TaskEventBufferTestManualStart, TestGcsClientFail);
  FRIEND_TEST(TaskEventBufferTestBatchSend, TestBatchedSend);
  FRIEND_TEST(TaskEventBufferTest, TestAddEvent);
  FRIEND_TEST(TaskEventBufferTest, TestFlushEvents);
  FRIEND_TEST(TaskEventBufferTest, TestFailedFlush);
  FRIEND_TEST(TaskEventBufferTest, TestBackPressure);
  FRIEND_TEST(TaskEventBufferTest, TestForcedFlush);
  FRIEND_TEST(TaskEventBufferTest, TestBufferSizeLimit);
};

}  // namespace worker

}  // namespace core
}  // namespace ray
