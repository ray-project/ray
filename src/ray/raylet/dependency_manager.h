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
#include "ray/util/counter_map.h"
// clang-format on

namespace ray {

namespace raylet {

/// Used for unit-testing the ClusterTaskManager, which requests dependencies
/// for queued tasks.
class TaskDependencyManagerInterface {
 public:
  virtual bool RequestTaskDependencies(
      const TaskID &task_id,
      const std::vector<rpc::ObjectReference> &required_objects,
      const TaskMetricsKey &task_key) = 0;
  virtual void RemoveTaskDependencies(const TaskID &task_id) = 0;
  virtual bool TaskDependenciesBlocked(const TaskID &task_id) const = 0;
  virtual bool CheckObjectLocal(const ObjectID &object_id) const = 0;
  virtual ~TaskDependencyManagerInterface(){};
};

/// \class DependencyManager
///
/// Responsible for managing object dependencies for local workers calling
/// `ray.get` or `ray.wait` and arguments of queued tasks. The caller can
/// request object dependencies for a task or worker. The task manager will
/// determine which object dependencies are remote and will request that these
/// objects be made available locally, either via the object manager or by
/// storing an error if the object is lost.
class DependencyManager : public TaskDependencyManagerInterface {
 public:
  /// Create a task dependency manager.
  DependencyManager(ObjectManagerInterface &object_manager)
      : object_manager_(object_manager) {
    waiting_tasks_counter_.SetOnChangeCallback(
        [this](std::pair<std::string, bool> key) mutable {
          int64_t num_total = waiting_tasks_counter_.Get(key);
          // Of the waiting tasks of this name, some fraction may be inactive (blocked on
          // object store memory availability). Get this breakdown by querying the pull
          // manager.
          int64_t num_inactive = std::min(
              num_total, object_manager_.PullManagerNumInactivePullsByTaskName(key));
          // Offset the metric values recorded from the owner process.
          ray::stats::STATS_tasks.Record(
              -num_total,
              {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::PENDING_NODE_ASSIGNMENT)},
               {"Name", key.first},
               {"IsRetry", key.second ? "1" : "0"},
               {"Source", "dependency_manager"}});
          ray::stats::STATS_tasks.Record(
              num_total - num_inactive,
              {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::PENDING_ARGS_FETCH)},
               {"Name", key.first},
               {"IsRetry", key.second ? "1" : "0"},
               {"Source", "dependency_manager"}});
          ray::stats::STATS_tasks.Record(
              num_inactive,
              {{"State",
                rpc::TaskStatus_Name(rpc::TaskStatus::PENDING_OBJ_STORE_MEM_AVAIL)},
               {"Name", key.first},
               {"IsRetry", key.second ? "1" : "0"},
               {"Source", "dependency_manager"}});
        });
  }

  /// Check whether an object is locally available.
  ///
  /// \param object_id The object to check for.
  /// \return Whether the object is local.
  bool CheckObjectLocal(const ObjectID &object_id) const;

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

  /// Start or update a worker's `ray.wait` request. This will attempt to make
  /// any remote objects local, including previously requested objects. The
  /// `ray.wait` request will stay active until the objects are made local or
  /// the request for this worker is canceled, whichever occurs first.
  ///
  /// This method may be called multiple times per worker on the same objects.
  ///
  /// \param worker_id The ID of the worker that called `ray.wait`.
  /// \param required_objects The objects required by the worker.
  /// \return Void.
  void StartOrUpdateWaitRequest(
      const WorkerID &worker_id,
      const std::vector<rpc::ObjectReference> &required_objects);

  /// Cancel a worker's `ray.wait` request. We will no longer attempt to fetch
  /// any objects that this worker requested previously, if no other task or
  /// worker requires them.
  ///
  /// \param worker_id The ID of the worker whose `ray.wait` request we should
  /// cancel.
  /// \return Void.
  void CancelWaitRequest(const WorkerID &worker_id);

  /// Start or update a worker's `ray.get` request. This will attempt to make
  /// any remote objects local, including previously requested objects. The
  /// `ray.get` request will stay active until the request for this worker is
  /// canceled.
  ///
  /// This method may be called multiple times per worker on the same objects.
  ///
  /// \param worker_id The ID of the worker that called `ray.wait`.
  /// \param required_objects The objects required by the worker.
  /// \return Void.
  void StartOrUpdateGetRequest(const WorkerID &worker_id,
                               const std::vector<rpc::ObjectReference> &required_objects);

  /// Cancel a worker's `ray.get` request. We will no longer attempt to fetch
  /// any objects that this worker requested previously, if no other task or
  /// worker requires them.
  ///
  /// \param worker_id The ID of the worker whose `ray.get` request we should
  /// cancel.
  /// \return Void.
  void CancelGetRequest(const WorkerID &worker_id);

  /// Request dependencies for a queued task. This will attempt to make any
  /// remote objects local until the caller cancels the task's dependencies.
  ///
  /// This method can only be called once per task, until the task has been
  /// canceled.
  ///
  /// \param task_id The task that requires the objects.
  /// \param required_objects The objects required by the task.
  /// \return Void.
  bool RequestTaskDependencies(const TaskID &task_id,
                               const std::vector<rpc::ObjectReference> &required_objects,
                               const TaskMetricsKey &task_key);

  /// Cancel a task's dependencies. We will no longer attempt to fetch any
  /// remote dependencies, if no other task or worker requires them.
  ///
  /// This method can only be called on a task whose dependencies were added.
  ///
  /// \param task_id The task that requires the objects.
  /// \param required_objects The objects required by the task.
  /// \return Void.
  void RemoveTaskDependencies(const TaskID &task_id);

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  /// \return A list of task IDs. This contains all added tasks that now have
  /// all of their dependencies fulfilled.
  std::vector<TaskID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  /// \return A list of task IDs. This contains all added tasks that previously
  /// had all of their dependencies fulfilled, but are now missing this object
  /// dependency.
  std::vector<TaskID> HandleObjectMissing(const ray::ObjectID &object_id);

  /// Check whether a requested task's dependencies are not being fetched to
  /// the local node due to lack of memory.
  bool TaskDependenciesBlocked(const TaskID &task_id) const;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record time-series metrics.
  void RecordMetrics();

 private:
  /// Metadata for an object that is needed by at least one executing worker
  /// and/or one queued task.
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
    TaskDependencies(const absl::flat_hash_set<ObjectID> &deps,
                     CounterMap<std::pair<std::string, bool>> &counter_map,
                     const TaskMetricsKey &task_key)
        : dependencies(std::move(deps)),
          num_missing_dependencies(dependencies.size()),
          waiting_task_counter_map(counter_map),
          task_key(task_key) {
      if (num_missing_dependencies > 0) {
        waiting_task_counter_map.Increment(task_key);
      }
    }
    /// The objects that the task depends on. These are the arguments to the
    /// task. These must all be simultaneously local before the task is ready
    /// to execute. Objects are removed from this set once
    /// UnsubscribeGetDependencies is called.
    absl::flat_hash_set<ObjectID> dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    size_t num_missing_dependencies;
    /// Used to identify the pull request for the dependencies to the object
    /// manager.
    uint64_t pull_request_id = 0;
    /// Reference to the counter map for metrics tracking.
    CounterMap<std::pair<std::string, bool>> &waiting_task_counter_map;
    /// The task name / is_retry tuple used for metrics tracking.
    const TaskMetricsKey task_key;

    void IncrementMissingDependencies() {
      if (num_missing_dependencies == 0) {
        waiting_task_counter_map.Increment(task_key);
      }
      num_missing_dependencies++;
    }

    void DecrementMissingDependencies() {
      num_missing_dependencies--;
      if (num_missing_dependencies == 0) {
        waiting_task_counter_map.Decrement(task_key);
      }
    }
  };

  /// Stop tracking this object, if it is no longer needed by any worker or
  /// queued task.
  void RemoveObjectIfNotNeeded(
      absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator required_object_it);

  /// Start tracking an object that is needed by a worker and/or queued task.
  absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator GetOrInsertRequiredObject(
      const ObjectID &object_id, const rpc::ObjectReference &ref);

  /// The object manager, used to fetch required objects from remote nodes.
  ObjectManagerInterface &object_manager_;

  /// A map from the ID of a queued task to metadata about whether the task's
  /// dependencies are all local or not.
  absl::flat_hash_map<TaskID, std::unique_ptr<TaskDependencies>> queued_task_requests_;

  /// A map from worker ID to the set of objects that the worker called
  /// `ray.get` on and a pull request ID for these objects. The pull request ID
  /// should be used to cancel the pull request in the object manager once the
  /// worker cancels the `ray.get` request.
  absl::flat_hash_map<WorkerID, std::pair<absl::flat_hash_set<ObjectID>, uint64_t>>
      get_requests_;

  /// A map from worker ID to the set of objects that the worker called
  /// `ray.wait` on. Objects are removed from the set once they are made local,
  /// or the worker cancels the `ray.wait` request.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ObjectID>> wait_requests_;

  /// Deduplicated pool of objects required by all queued tasks and workers.
  /// Objects are removed from this set once there are no more tasks or workers
  /// that require it.
  absl::flat_hash_map<ObjectID, ObjectDependencies> required_objects_;

  /// The set of locally available objects. This is used to determine which
  /// tasks are ready to run and which `ray.wait` requests can be finished.
  std::unordered_set<ray::ObjectID> local_objects_;

  /// Counts the number of active task dependency fetches by task name. The counter
  /// total will be less than or equal to the size of queued_task_requests_.
  CounterMap<TaskMetricsKey> waiting_tasks_counter_;

  friend class DependencyManagerTest;
};

}  // namespace raylet

}  // namespace ray
