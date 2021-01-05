// Copyright 2017 The Ray Authors.
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

#include <boost/asio.hpp>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/id.h"
#include "ray/gcs/gcs_client.h"
#include "ray/object_manager/object_directory.h"

namespace ray {

namespace raylet {

using rpc::TaskReconstructionData;

class ReconstructionPolicyInterface {
 public:
  virtual void ListenAndMaybeReconstruct(const ObjectID &object_id,
                                         const rpc::Address &owner_address) = 0;
  virtual void Cancel(const ObjectID &object_id) = 0;
  virtual ~ReconstructionPolicyInterface(){};
};

class ReconstructionPolicy : public ReconstructionPolicyInterface {
 public:
  /// Create the reconstruction policy.
  ///
  /// \param io_service The event loop to attach reconstruction timers to.
  /// \param reconstruction_handler The handler to call if a task needs to be
  /// re-executed.
  /// \param initial_reconstruction_timeout_ms The initial timeout within which
  /// a task lease notification must be received. Otherwise, reconstruction
  /// will be triggered.
  /// \param node_id The node ID to use when requesting notifications from
  /// the GCS.
  /// \param gcs_client The Client of GCS.
  /// lease notifications from.
  ReconstructionPolicy(
      boost::asio::io_service &io_service,
      std::function<void(const TaskID &, const ObjectID &)> reconstruction_handler,
      int64_t initial_reconstruction_timeout_ms, const NodeID &node_id,
      std::shared_ptr<gcs::GcsClient> gcs_client,
      std::shared_ptr<ObjectDirectoryInterface> object_directory);

  /// Listen for task lease notifications about an object that may require
  /// reconstruction. If no notifications are received within the initial
  /// timeout, then the registered task reconstruction handler will be called
  /// for the task that created the object.
  ///
  /// \param object_id The object to check for reconstruction.
  void ListenAndMaybeReconstruct(const ObjectID &object_id,
                                 const rpc::Address &owner_address);

  /// Cancel listening for an object. Notifications for the object will be
  /// ignored. This does not cancel a reconstruction attempt that is already in
  /// progress.
  ///
  /// \param object_id The object to cancel.
  void Cancel(const ObjectID &object_id);

  /// Handle a notification for a task lease. This handler should be called to
  /// indicate that a task is currently being executed, so any objects that it
  /// creates should not be reconstructed.
  ///
  /// \param task_id The task ID of the task being executed.
  /// \param lease_timeout_ms After this timeout, the task's lease is
  /// guaranteed to be expired. If a second notification is not received within
  /// this timeout, then objects that the task creates may be reconstructed.
  void HandleTaskLeaseNotification(const TaskID &task_id, int64_t lease_timeout_ms);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 private:
  struct ReconstructionTask {
    ReconstructionTask(boost::asio::io_service &io_service)
        : expires_at(INT64_MAX),
          subscribed(false),
          reconstruction_attempt(0),
          reconstruction_timer(new boost::asio::deadline_timer(io_service)) {}

    // The objects created by this task that we are listening for notifications for.
    std::unordered_set<ObjectID> created_objects;
    // Owner addresses of created objects.
    std::unordered_map<ObjectID, rpc::Address> owner_addresses;
    // The time at which the timer for this task expires, according to this
    // node's steady clock.
    int64_t expires_at;
    // Whether we are subscribed to lease notifications for this task.
    bool subscribed;
    // The number of times we've attempted reconstructing this task so far.
    int reconstruction_attempt;
    // The task's reconstruction timer. If this expires before a lease
    // notification is received, then the task will be reconstructed.
    std::unique_ptr<boost::asio::deadline_timer> reconstruction_timer;
  };

  /// Set the reconstruction timer for a task. If no task lease notifications
  /// are received within the timeout, then reconstruction will be triggered.
  /// If the timer was previously set, this method will cancel it and reset the
  /// timer to the new timeout.
  void SetTaskTimeout(std::unordered_map<TaskID, ReconstructionTask>::iterator task_it,
                      int64_t timeout_ms);

  /// Handle task lease notification from GCS.
  void OnTaskLeaseNotification(const TaskID &task_id,
                               const boost::optional<rpc::TaskLeaseData> &task_lease);

  /// Attempt to re-execute a task to reconstruct the required object.
  ///
  /// \param task_id The task to attempt to re-execute.
  /// \param required_object_id The object created by the task that requires
  /// reconstruction.
  /// \param reconstruction_attempt What number attempt this is at
  /// reconstructing the task. This is used to suppress duplicate
  /// reconstructions of the same task (e.g., if a task creates two objects
  /// that both require reconstruction).
  void AttemptReconstruction(const TaskID &task_id, const ObjectID &required_object_id,
                             int reconstruction_attempt);

  /// Handle expiration of a task lease.
  void HandleTaskLeaseExpired(const TaskID &task_id);

  /// Handle the response for an attempt at adding an entry to the task
  /// reconstruction log.
  void HandleReconstructionLogAppend(const TaskID &task_id, const ObjectID &object_id,
                                     bool success);

  /// The event loop.
  boost::asio::io_service &io_service_;
  /// The handler to call for tasks that require reconstruction.
  const std::function<void(const TaskID &, const ObjectID &)> reconstruction_handler_;
  /// The initial timeout within which a task lease notification must be
  /// received. Otherwise, reconstruction will be triggered.
  const int64_t initial_reconstruction_timeout_ms_;
  /// The node ID to use when requesting notifications from the GCS.
  const NodeID node_id_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// The object directory used to lookup object locations.
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  /// The tasks that we are currently subscribed to in the GCS.
  std::unordered_map<TaskID, ReconstructionTask> listening_tasks_;
};

}  // namespace raylet

}  // namespace ray
