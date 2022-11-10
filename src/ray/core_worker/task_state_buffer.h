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
#include "absl/container/flat_hash_map.h"             // absl::flat_hash_map
#include "absl/synchronization/mutex.h"               // absl::Mutex
#include "ray/common/asio/instrumented_io_context.h"  // instrumented_io_context
#include "ray/common/asio/periodical_runner.h"        // PeriodicRunner
#include "ray/common/id.h"                            // TaskID
#include "ray/common/task/task_spec.h"                // TaskSpecification
#include "ray/gcs/gcs_client/gcs_client.h"            // Gcs::GcsClient
#include "src/ray/protobuf/gcs.pb.h"                  // rpc::TaskStateEventData

namespace ray {
namespace core {

namespace worker {

using TaskIdEventMap = absl::flat_hash_map<TaskID, rpc::TaskStateEvents>;
/// An in-memory buffer for storing task state changes, and flushing them periodically to
/// GCS. Task state events will be recorded by other core components, i.e. core worker
/// and raylet.
/// This class is thread-safe.
/// TODO: The buffer could be configured with parameters for frequency of flushing, and
/// and maximal number of entries pending. Extra entries should evict old events when
/// necessary.
class TaskStateBuffer {
 public:
  /// Constructor
  ///
  /// \param io_service IO service to run the periodic flushing routines.
  /// \param gcs_client GCS client
  TaskStateBuffer(instrumented_io_context &io_service,
                  const std::shared_ptr<gcs::GcsClient> &gcs_client);

  /// Add a task event with optional task metadata info delta.
  ///
  /// \param task_id Task ID of the task.
  /// \param task_info_update Changed TaskInfoEntry delta to be updated for the task.
  /// \param task_status Current task status to be recorded.
  void AddTaskEvent(TaskID task_id,
                    rpc::TaskInfoEntry &&task_info_update,
                    rpc::TaskStatus task_status) LOCKS_EXCLUDED(mutex_);

 private:
  TaskIdEventMap::iterator GetOrInitTaskEvents(TaskID task_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Flush all of the events that have been added since last flush to the GCS.
  void FlushEvents() LOCKS_EXCLUDED(mutex_);

  std::unique_ptr<rpc::TaskStateEventData> GetAndResetBuffer()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Mutex guarding rpc_profile_data_.
  absl::Mutex mutex_;

  /// ASIO IO service event loop. Must be started by the caller.
  instrumented_io_context &io_service_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// Client to the GCS used to push profile events to it.
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Current buffer storing task state events, mapped from task name to a list of events.
  TaskIdEventMap task_events_map_ GUARDED_BY(mutex_);

  /// Metadata for the buffer

  /// TODO(rickyx): init these fields properly based on callers.
  /// Component type of the component where this buffer is maintained.
  const std::string component_type_;

  /// Binary representation of component id of the component where this buffer is
  /// maintained.
  const std::string component_id_;

  /// Node ID.
  const NodeID node_id_;
};

}  // namespace worker

}  // namespace core
}  // namespace ray