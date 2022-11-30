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

#include <memory>  // std::shared_ptr
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"               // absl::Mutex
#include "absl/types/optional.h"                      // std::unique_ptr
#include "ray/common/asio/instrumented_io_context.h"  // instrumented_io_context
#include "ray/common/asio/periodical_runner.h"        // PeriodicRunner
#include "ray/common/id.h"                            // TaskID
#include "ray/common/task/task_spec.h"                // TaskSpecification
#include "ray/gcs/gcs_client/gcs_client.h"            // Gcs::GcsClient
#include "src/ray/protobuf/gcs.pb.h"                  // rpc::TaskEventData

namespace ray {
namespace core {

namespace worker {

class TaskEventBuffer {
 public:
  virtual ~TaskEventBuffer() = default;

  /// Add a task event with optional task metadata info.
  ///
  /// \param task_id Task ID of the task.
  /// \param task_status Status of the task event.
  /// \param task_info Immutable TaskInfoEntry of metadata for the task.
  /// \param task_state_update Mutable task states updated for task execution.
  virtual void AddTaskStatusEvent(
      TaskID task_id,
      rpc::TaskStatus task_status,
      std::unique_ptr<rpc::TaskInfoEntry> task_info,
      std::unique_ptr<rpc::TaskStateEntry> task_state_update) = 0;

  /// Add a profile event.
  ///
  /// \param task_id Task id of the task.
  /// \param event The profile event.
  /// \param component_type Type of the component, e.g. worker.
  /// \param component_id Id of the component, e.g. worker id.
  /// \param node_ip_address Node IP address of this node.
  virtual void AddProfileEvent(TaskID task_id,
                               rpc::ProfileEventEntry event,
                               const std::string &component_type,
                               const std::string &component_id,
                               const std::string &node_ip_address) = 0;

  /// Flushing task events to GCS.
  ///
  /// This function will be called periodically configured by
  /// `RAY_task_events_report_interval_ms`, and send task events stored in a buffer to
  /// GCS. If GCS has not responded to a previous flush, it will defer the flushing to
  /// the next interval (if not forced.)
  ///
  /// \param force When set to true, buffered events will be sent to GCS even if GCS has
  ///       not responded to the previous flush.
  virtual void FlushEvents(bool forced) = 0;

  /// Stop the TaskEventBuffer and it's underlying IO, disconnecting GCS clients.
  virtual void Stop() = 0;

  /// Get all task events stored in the buffer.
  virtual rpc::TaskEventData GetAllTaskEvents() = 0;
};

/// An in-memory buffer for storing task state events, and flushing them periodically to
/// GCS. Task state events will be recorded by other core components, i.e. core worker
/// and raylet.
/// The buffer has its own io_context and io_thread, that's isolated from other
/// components.
///
/// If any of the gRPC call failed, the task events will be silently dropped. This
/// is probably fine since this usually indicated a much worse issue.
/// If GCS failed to respond quickly enough on the next flush, no gRPC will be made and
/// reporting of events to GCS will be delayed until GCS replies the gRPC.
///
/// TODO(rickyx): The buffer could currently grow unbounded in memory if GCS is
/// overloaded/unavailable.
///
///
/// This class is thread-safe.
class TaskEventBufferImpl : public TaskEventBuffer {
 public:
  /// Constructor
  ///
  /// \param gcs_client GCS client
  /// \param manual_flush Test only flag to disable periodical flushing events.
  TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client,
                      bool manual_flush = false);

  void AddTaskStatusEvent(TaskID task_id,
                          rpc::TaskStatus task_status,
                          std::unique_ptr<rpc::TaskInfoEntry> task_info,
                          std::unique_ptr<rpc::TaskStateEntry> task_state_update)
      LOCKS_EXCLUDED(mutex_) override;

  void AddProfileEvent(TaskID task_id,
                       rpc::ProfileEventEntry event,
                       const std::string &component_type,
                       const std::string &component_id,
                       const std::string &node_ip_address)
      LOCKS_EXCLUDED(mutex_) override;

  /// Flush all of the events that have been added since last flush to the GCS.
  /// If previous flush's gRPC hasn't been replied and `forced` is false, the flush will
  /// be skipped until the next invocation.
  ///
  /// \param forced True if it should be flushed regardless of previous gRPC event's
  /// state.
  void FlushEvents(bool forced) LOCKS_EXCLUDED(mutex_) override;

  /// Stop the TaskEventBuffer and it's underlying IO, disconnecting GCS clients.
  void Stop() LOCKS_EXCLUDED(mutex_) override;

  rpc::TaskEventData GetAllTaskEvents() LOCKS_EXCLUDED(mutex_) override {
    absl::MutexLock lock(&mutex_);
    rpc::TaskEventData copy(task_events_data_);
    return copy;
  }

 private:
  /// Mutex guarding task_events_data_.
  absl::Mutex mutex_;

  /// IO service event loop owned by TaskEventBuffer.
  instrumented_io_context io_service_;

  /// Work guard to prevent the io_context from exiting when no work.
  // boost::asio::io_service::work io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;

  /// Dedicated io thread for running the periodical runner and the GCS client.
  std::thread io_thread_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// Client to the GCS used to push profile events to it.
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  /// Current task event data to be pushed to GCS.
  rpc::TaskEventData task_events_data_ GUARDED_BY(mutex_);

  /// Flag to toggle event recording on/off.
  bool recording_on_ = false;

  /// True if there's a pending gRPC call. It's a simple way to prevent overloading
  /// GCS with too many calls. There is no point sending more events if GCS could not
  /// process them quick enough.
  /// TODO(rickyx): When there are so many workers, we might even want to proxy those to
  /// the agent/raylet to further prevent overloading GCS.
  std::atomic<bool> grpc_in_progress_ = false;

  /// Stats tracking for debugging and monitoring.
  size_t total_events_bytes_ = 0;
  size_t total_num_events_ = 0;

  FRIEND_TEST(TaskEventBufferTest, TestAddEvent);
  FRIEND_TEST(TaskEventBufferTest, TestFlushEvents);
  FRIEND_TEST(TaskEventBufferTest, TestBackPressure);
  FRIEND_TEST(TaskEventBufferTest, TestForcedFlush);
};

}  // namespace worker

}  // namespace core
}  // namespace ray