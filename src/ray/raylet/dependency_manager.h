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
#include "ray/common/common_protocol.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/reconstruction_policy.h"
// clang-format on

namespace ray {

namespace raylet {

using rpc::TaskLeaseData;

class ReconstructionPolicy;

/// \class DependencyManager
///
/// Responsible for managing object dependencies for tasks.  The caller can
/// subscribe to object dependencies for a task. The task manager will
/// determine which object dependencies are remote. These are the objects that
/// are neither in the local object store, nor will they be created by a
/// locally queued task. The task manager will request that these objects be
/// made available locally, either by object transfer from a remote node or
/// reconstruction. The task manager will also cancel these objects if they are
/// no longer needed by any task.
class DependencyManager {
 public:
  /// Create a task dependency manager.
  DependencyManager(ObjectManagerInterface2 &object_manager,
                    ReconstructionPolicyInterface &reconstruction_policy)
      : object_manager_(object_manager), reconstruction_policy_(reconstruction_policy) {}

  /// Check whether an object is locally available.
  ///
  /// \param object_id The object to check for.
  /// \return Whether the object is local.
  bool CheckObjectLocal(const ObjectID &object_id) const;

  /// Update the `ray.wait` request. This will start a Pull request for any new
  /// objects that we're not already fetching.
  void AddOrUpdateWaitRequest(const WorkerID &worker_id,
                              const std::vector<rpc::ObjectReference> &required_objects);

  void CancelWaitRequest(const WorkerID &worker_id);

  /// Update the `ray.get` request. If any new objects are added, cancel the
  /// old Pull request and add a new one.
  void AddOrUpdateGetRequest(const WorkerID &worker_id,
                             const std::vector<rpc::ObjectReference> &required_objects);

  void CancelGetRequest(const WorkerID &worker_id);

  /// Request dependencies for a queued task.
  bool AddTaskDependencies(const TaskID &task_id,
                           const std::vector<rpc::ObjectReference> &required_objects);

  void CancelTaskDependencies(const TaskID &task_id);

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
    /// The workers that depend on this object because they called `ray.get` on the
    /// object.
    std::unordered_set<WorkerID> dependent_get_requests;
    /// The workers that depend on this object because they called `ray.wait` on the
    /// object.
    std::unordered_set<WorkerID> dependent_wait_requests;
    /// If this object is required by at least one worker that called `ray.wait`, this is
    /// the pull request ID.
    uint64_t wait_request_id = 0;
    /// The address of the worker that owns this object.
    rpc::Address owner_address;

    bool Empty() const {
      return dependent_tasks.empty() && dependent_get_requests.empty() &&
             dependent_wait_requests.empty();
    }
  };

  /// A struct to represent the object dependencies of a task.
  struct TaskDependencies {
    TaskDependencies(const std::vector<rpc::ObjectReference> &deps)
        : num_missing_get_dependencies(deps.size()) {
      const auto dep_ids = ObjectRefsToIds(deps);
      dependencies.insert(dep_ids.begin(), dep_ids.end());
    }
    /// The objects that the task depends on. These are the arguments to the
    /// task. These must all be simultaneously local before the task is ready
    /// to execute. Objects are removed from this set once
    /// UnsubscribeGetDependencies is called.
    absl::flat_hash_set<ObjectID> dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    size_t num_missing_get_dependencies;
    /// Used to identify the pull request for the dependencies to the object
    /// manager.
    uint64_t pull_request_id = 0;
  };

  void RemoveObjectIfNotNeeded(
      absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator required_object_it);

  absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator GetOrInsertRequiredObject(
      const ObjectID &object_id, const rpc::ObjectReference &ref);

  /// The object manager, used to fetch required objects from remote nodes.
  ObjectManagerInterface2 &object_manager_;
  /// The reconstruction policy, used to reconstruct required objects that no
  /// longer exist on any live nodes.
  ReconstructionPolicyInterface &reconstruction_policy_;

  /// Task ID -> request ID.
  absl::flat_hash_map<TaskID, TaskDependencies> queued_task_requests_;

  /// Worker ID -> required objects.
  absl::flat_hash_map<WorkerID, std::pair<absl::flat_hash_set<ObjectID>, uint64_t>>
      get_requests_;

  /// Worker ID -> waiting objects.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ObjectID>> wait_requests_;

  /// Deduplicated pool of objects required by all queued tasks and workers.
  /// Objects are removed from this set once there are no more tasks or workers
  /// that require it.
  absl::flat_hash_map<ObjectID, ObjectDependencies> required_objects_;

  /// The set of locally available objects.
  std::unordered_set<ray::ObjectID> local_objects_;
};

}  // namespace raylet

}  // namespace ray
