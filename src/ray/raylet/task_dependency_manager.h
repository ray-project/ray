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

// clang-format off
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/reconstruction_policy.h"
// clang-format on

namespace ray {

namespace raylet {

using rpc::TaskLeaseData;

class ReconstructionPolicy;

/// \class TaskDependencyManager
///
/// Responsible for managing object dependencies for tasks.  The caller can
/// subscribe to object dependencies for a task. The task manager will
/// determine which object dependencies are remote. These are the objects that
/// are neither in the local object store, nor will they be created by a
/// locally queued task. The task manager will request that these objects be
/// made available locally, either by object transfer from a remote node or
/// reconstruction. The task manager will also cancel these objects if they are
/// no longer needed by any task.
class TaskDependencyManager {
 public:
  /// Create a task dependency manager.
  TaskDependencyManager(ObjectManagerInterface &object_manager,
                        ReconstructionPolicyInterface &reconstruction_policy,
                        boost::asio::io_service &io_service, const ClientID &client_id,
                        int64_t initial_lease_period_ms,
                        std::shared_ptr<gcs::GcsClient> gcs_client);

  /// Check whether an object is locally available.
  ///
  /// \param object_id The object to check for.
  /// \return Whether the object is local.
  bool CheckObjectLocal(const ObjectID &object_id) const;

  /// Subscribe to object depedencies required by the task and check whether
  /// all dependencies are fulfilled. This should be called for task arguments and
  /// `ray.get` calls during task execution.
  ///
  /// The TaskDependencyManager will track the task's dependencies
  /// until UnsubscribeGetDependencies is called on the same task ID. If any
  /// dependencies are remote, then they will be requested. When the last
  /// remote dependency later appears locally via a call to HandleObjectLocal,
  /// the subscribed task will be returned by the HandleObjectLocal call,
  /// signifying that it is ready to run. This method may be called multiple
  /// times per task.
  ///
  /// \param task_id The ID of the task whose dependencies to subscribe to.
  /// \param required_objects The objects required by the task.
  /// \return Whether all of the given dependencies for the given task are
  /// local.
  bool SubscribeGetDependencies(
      const TaskID &task_id, const std::vector<rpc::ObjectReference> &required_objects);

  /// Subscribe to object depedencies required by the worker. This should be called for
  /// ray.wait calls during task execution.
  ///
  /// The TaskDependencyManager will track all remote dependencies until the
  /// dependencies are local, or until UnsubscribeWaitDependencies is called
  /// with the same worker ID, whichever occurs first. Remote dependencies will
  /// be requested.  This method may be called multiple times per worker on the
  /// same objects.
  ///
  /// \param worker_id The ID of the worker that called `ray.wait`.
  /// \param required_objects The objects required by the worker.
  /// \return Void.
  void SubscribeWaitDependencies(
      const WorkerID &worker_id,
      const std::vector<rpc::ObjectReference> &required_objects);

  /// Unsubscribe from the object dependencies required by this task through the task
  /// arguments or `ray.get`. If the objects were remote and are no longer required by any
  /// subscribed task, then they will be canceled.
  ///
  /// \param task_id The ID of the task whose dependencies we should unsubscribe from.
  /// \return Whether the task was subscribed before.
  bool UnsubscribeGetDependencies(const TaskID &task_id);

  /// Unsubscribe from the object dependencies required by this worker through `ray.wait`.
  /// If the objects were remote and are no longer required by any subscribed task, then
  /// they will be canceled.
  ///
  /// \param worker_id The ID of the worker whose dependencies we should unsubscribe from.
  /// \return The objects that the worker was waiting on.
  void UnsubscribeWaitDependencies(const WorkerID &worker_id);

  /// Mark that the given task is pending execution. Any objects that it creates
  /// are now considered to be pending creation. If there are any subscribed
  /// tasks that depend on these objects, then the objects will be canceled.
  ///
  /// \param task The task that is pending execution.
  void TaskPending(const Task &task);

  /// Mark that the given task is no longer pending execution. Any objects that
  /// it creates that are not already local are now considered to be remote. If
  /// there are any subscribed tasks that depend on these objects, then the
  /// objects will be requested.
  ///
  /// \param task_id The ID of the task to cancel.
  void TaskCanceled(const TaskID &task_id);

  /// Handle an object becoming locally available. If there are any subscribed
  /// tasks that depend on this object, then the object will be canceled.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that now
  /// have all of their dependencies fulfilled, once this object was made
  /// local.
  std::vector<TaskID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available. If there are any
  /// subscribed tasks that depend on this object, then the object will be
  /// requested.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that
  /// previously had all of their dependencies fulfilled, but are now missing
  /// this object dependency.
  std::vector<TaskID> HandleObjectMissing(const ray::ObjectID &object_id);

  /// Get a list of all Tasks currently marked as pending object dependencies in the task
  /// dependency manager.
  ///
  /// \return Return a vector of TaskIDs for tasks registered as pending.
  std::vector<TaskID> GetPendingTasks() const;

  /// Remove all of the tasks specified. These tasks will no longer be
  /// considered pending and the objects they depend on will no longer be
  /// required.
  ///
  /// \param task_ids The collection of task IDs. For a given task in this set,
  /// all tasks that depend on the task must also be included in the set.
  void RemoveTasksAndRelatedObjects(const std::unordered_set<TaskID> &task_ids);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

  /// Get the address of the owner of this object. An address will only be
  /// returned if the caller previously specified that this object is required
  /// on this node, through a call to SubscribeGetDependencies or
  /// SubscribeWaitDependencies.
  ///
  /// \param[in] object_id The object whose owner to get.
  /// \param[out] owner_address The address of the object's owner, if
  /// available.
  /// \return True if we have owner information for the object.
  bool GetOwnerAddress(const ObjectID &object_id, rpc::Address *owner_address) const;

 private:
  struct ObjectDependencies {
    ObjectDependencies(const rpc::ObjectReference &ref)
        : owner_address(ref.owner_address()) {}
    /// The tasks that depend on this object, either because the object is a task argument
    /// or because the task called `ray.get` on the object.
    std::unordered_set<TaskID> dependent_tasks;
    /// The workers that depend on this object because they called `ray.wait` on the
    /// object.
    std::unordered_set<WorkerID> dependent_workers;
    /// The address of the worker that owns this object.
    rpc::Address owner_address;

    bool Empty() const { return dependent_tasks.empty() && dependent_workers.empty(); }
  };

  /// A struct to represent the object dependencies of a task.
  struct TaskDependencies {
    /// The objects that the task depends on. These are either the arguments to
    /// the task or objects that the task calls `ray.get` on. These must be
    /// local before the task is ready to execute. Objects are removed from
    /// this set once UnsubscribeGetDependencies is called.
    std::unordered_set<ObjectID> get_dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    int64_t num_missing_get_dependencies;
  };

  /// The objects that the worker is fetching. These are objects that a task that executed
  /// or is executing on the worker called `ray.wait` on that are not yet local. An object
  /// will be automatically removed from this set once it becomes local.
  using WorkerDependencies = std::unordered_set<ObjectID>;

  struct PendingTask {
    PendingTask(int64_t initial_lease_period_ms, boost::asio::io_service &io_service)
        : lease_period(initial_lease_period_ms),
          expires_at(INT64_MAX),
          lease_timer(new boost::asio::deadline_timer(io_service)) {}

    /// The timeout within which the lease should be renewed.
    int64_t lease_period;
    /// The time at which the current lease will expire, according to this
    /// node's steady clock.
    int64_t expires_at;
    /// A timer used to determine when to next renew the lease.
    std::unique_ptr<boost::asio::deadline_timer> lease_timer;
  };

  /// Check whether the given object needs to be made available through object
  /// transfer or reconstruction. These are objects for which: (1) there is a
  /// subscribed task dependent on it, (2) the object is not local, and (3) the
  /// task that creates the object is not pending execution locally.
  bool CheckObjectRequired(const ObjectID &object_id) const;
  /// If the given object is required, then request that the object be made
  /// available through object transfer or reconstruction.
  void HandleRemoteDependencyRequired(const ObjectID &object_id);
  /// If the given object is no longer required, then cancel any in-progress
  /// operations to make the object available through object transfer or
  /// reconstruction.
  void HandleRemoteDependencyCanceled(const ObjectID &object_id);
  /// Acquire the task lease in the GCS for the given task. This is used to
  /// indicate to other nodes that the task is currently pending on this node.
  /// The task lease has an expiration time. If we do not renew the lease
  /// before that time, then other nodes may choose to execute the task.
  void AcquireTaskLease(const TaskID &task_id);

  /// The object manager, used to fetch required objects from remote nodes.
  ObjectManagerInterface &object_manager_;
  /// The reconstruction policy, used to reconstruct required objects that no
  /// longer exist on any live nodes.
  ReconstructionPolicyInterface &reconstruction_policy_;
  /// The event loop, used to set timers for renewing task leases. The task
  /// leases are used to indicate which tasks are pending execution on this
  /// node and must be periodically renewed.
  boost::asio::io_service &io_service_;
  /// This node's GCS client ID, used in the task lease information.
  const ClientID client_id_;
  /// For a given task, the expiration period of the initial task lease that is
  /// added to the GCS. The lease expiration period is doubled every time the
  /// lease is renewed.
  const int64_t initial_lease_period_ms_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// A mapping from task ID of each subscribed task to its list of object
  /// dependencies, either task arguments or objects passed into `ray.get`.
  std::unordered_map<ray::TaskID, TaskDependencies> task_dependencies_;
  /// A mapping from worker ID to each object that the worker called `ray.wait` on.
  std::unordered_map<ray::WorkerID, WorkerDependencies> worker_dependencies_;
  /// All tasks whose outputs are required by a subscribed task. This is a
  /// mapping from task ID to information about the objects that the task
  /// creates, either by return value or by `ray.put`. For each object, we
  /// store the IDs of the subscribed tasks that are dependent on the object.
  std::unordered_map<ray::TaskID, std::unordered_map<ObjectID, ObjectDependencies>>
      required_tasks_;
  /// Objects that are required by a subscribed task, are not local, and are
  /// not created by a pending task. For these objects, there are pending
  /// operations to make the object available.
  std::unordered_set<ray::ObjectID> required_objects_;
  /// The set of locally available objects.
  std::unordered_set<ray::ObjectID> local_objects_;
  /// The set of tasks that are pending execution. Any objects created by these
  /// tasks that are not already local are pending creation.
  std::unordered_map<ray::TaskID, PendingTask> pending_tasks_;
};

}  // namespace raylet

}  // namespace ray
