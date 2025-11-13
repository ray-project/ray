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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/object_manager/object_manager.h"
#include "ray/util/counter_map.h"

namespace ray {

namespace raylet {

using GetRequestId = int64_t;
using PullRequestId = int64_t;

/// Used for unit-testing the ClusterLeaseManager, which requests dependencies
/// for queued leases.
class LeaseDependencyManagerInterface {
 public:
  virtual bool RequestLeaseDependencies(
      const LeaseID &lease_id,
      const std::vector<rpc::ObjectReference> &required_objects,
      const TaskMetricsKey &lease_key) = 0;
  virtual void RemoveLeaseDependencies(const LeaseID &lease_id) = 0;
  virtual bool LeaseDependenciesBlocked(const LeaseID &lease_id) const = 0;
  virtual bool CheckObjectLocal(const ObjectID &object_id) const = 0;
  virtual ~LeaseDependencyManagerInterface() = default;
};

/// \class LeaseDependencyManager
///
/// Responsible for managing object dependencies for local workers calling
/// `ray.get` or `ray.wait` and arguments of queued tasks. The caller can
/// request object dependencies for a lease or worker. The lease manager will
/// determine which object dependencies are remote and will request that these
/// objects be made available locally, either via the object manager or by
/// storing an error if the object is lost.
class LeaseDependencyManager : public LeaseDependencyManagerInterface {
 public:
  /// Create a lease dependency manager.
  explicit LeaseDependencyManager(
      ObjectManagerInterface &object_manager,
      ray::observability::MetricInterface &task_by_state_counter)
      : object_manager_(object_manager), task_by_state_counter_(task_by_state_counter) {
    waiting_leases_counter_.SetOnChangeCallback(
        [this](std::pair<std::string, bool> key) mutable {
          int64_t num_total = waiting_leases_counter_.Get(key);
          // Of the waiting tasks of this name, some fraction may be inactive (blocked on
          // object store memory availability). Get this breakdown by querying the pull
          // manager.
          int64_t num_inactive = std::min(
              num_total, object_manager_.PullManagerNumInactivePullsByTaskName(key));
          // Offset the metric values recorded from the owner process.
          task_by_state_counter_.Record(
              -num_total,
              {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::PENDING_NODE_ASSIGNMENT)},
               {"Name", key.first},
               {"IsRetry", key.second ? "1" : "0"},
               {"Source", "dependency_manager"}});
          task_by_state_counter_.Record(
              num_total - num_inactive,
              {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::PENDING_ARGS_FETCH)},
               {"Name", key.first},
               {"IsRetry", key.second ? "1" : "0"},
               {"Source", "dependency_manager"}});
          task_by_state_counter_.Record(
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
  bool CheckObjectLocal(const ObjectID &object_id) const override;

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
  void StartOrUpdateWaitRequest(
      const WorkerID &worker_id,
      const std::vector<rpc::ObjectReference> &required_objects);

  /// Cancel a worker's `ray.wait` request. We will no longer attempt to fetch
  /// any objects that this worker requested previously, if no other task or
  /// worker requires them.
  ///
  /// \param worker_id The ID of the worker whose `ray.wait` request we should
  /// cancel.
  void CancelWaitRequest(const WorkerID &worker_id);

  /// \param worker_id The ID of the worker that called `ray.get`.
  /// \param required_objects The objects required by the worker.
  /// \param get_request_id The ID of the get request. It is used by the worker to clean
  /// up a GetRequest.
  void StartGetRequest(const WorkerID &worker_id,
                       std::vector<rpc::ObjectReference> &&required_objects,
                       int64_t get_request_id);

  /// Cleans up either an inflight or finished get request. Cancels the underlying
  /// pull if necessary.
  ///
  /// \param worker_id The ID of the worker that called `ray.get`.
  /// \param request_id The ID of the get request.
  /// \param required_objects The objects required by the worker.
  /// \return the request id which will be used for cleanup.
  void CancelGetRequest(const WorkerID &worker_id, const GetRequestId &request_id);

  /// Cancel all of a worker's `ray.get` requests. We will no longer attempt to fetch
  /// any objects that this worker requested previously, if no other lease or
  /// worker requires them.
  ///
  /// \param worker_id The ID of the worker whose `ray.get` request we should
  /// cancel.
  void CancelGetRequest(const WorkerID &worker_id);

  /// Request dependencies for a queued lease. This will attempt to make any
  /// remote objects local until the caller cancels the lease's dependencies.
  ///
  /// This method can only be called once per lease, until the lease has been
  /// canceled.
  ///
  /// \param lease_id The lease that requires the objects.
  /// \param required_objects The objects required by the lease.
  bool RequestLeaseDependencies(const LeaseID &lease_id,
                                const std::vector<rpc::ObjectReference> &required_objects,
                                const TaskMetricsKey &task_key) override;

  /// Cancel a lease's dependencies. We will no longer attempt to fetch any
  /// remote dependencies, if no other lease or worker requires them.
  ///
  /// This method can only be called on a lease whose dependencies were added.
  ///
  /// \param lease_id The lease that requires the objects.
  /// \param required_objects The objects required by the lease.
  void RemoveLeaseDependencies(const LeaseID &lease_id) override;

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  /// \return A list of lease IDs. This contains all granted leases that now have
  /// all of their dependencies fulfilled.
  std::vector<LeaseID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  /// \return A list of lease IDs. This contains all granted leases that previously
  /// had all of their dependencies fulfilled, but are now missing this object
  /// dependency.
  std::vector<LeaseID> HandleObjectMissing(const ray::ObjectID &object_id);

  /// Check whether a requested lease's dependencies are not being fetched to
  /// the local node due to lack of memory.
  bool LeaseDependenciesBlocked(const LeaseID &lease_id) const override;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record time-series metrics.
  void RecordMetrics();

 private:
  /// Metadata for an object that is needed by at least one executing worker
  /// and/or one queued lease.
  struct ObjectDependencies {
    explicit ObjectDependencies(const rpc::ObjectReference &ref)
        : owner_address(ref.owner_address()) {}
    /// The leases that depend on this object, either because the object is a lease
    /// argument or because the lease of the lease called `ray.get` on the object.
    std::unordered_set<LeaseID> dependent_leases;
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
      return dependent_leases.empty() && dependent_get_requests.empty() &&
             dependent_wait_requests.empty();
    }
  };

  /// A struct to represent the object dependencies of a task.
  struct LeaseDependencies {
    LeaseDependencies(absl::flat_hash_set<ObjectID> deps,
                      CounterMap<std::pair<std::string, bool>> &counter_map,
                      TaskMetricsKey task_key)
        : dependencies_(std::move(deps)),
          num_missing_dependencies_(dependencies_.size()),
          waiting_task_counter_map_(counter_map),
          task_key_(std::move(task_key)) {
      if (num_missing_dependencies_ > 0) {
        waiting_task_counter_map_.Increment(task_key_);
      }
    }
    /// The objects that the lease depends on. These are the arguments to the
    /// lease. These must all be simultaneously local before the lease is ready
    /// to execute. Objects are removed from this set once
    /// UnsubscribeGetDependencies is called.
    absl::flat_hash_set<ObjectID> dependencies_;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    size_t num_missing_dependencies_;
    /// Used to identify the pull request for the dependencies to the object
    /// manager.
    uint64_t pull_request_id_ = 0;
    /// Reference to the counter map for metrics tracking.
    CounterMap<std::pair<std::string, bool>> &waiting_task_counter_map_;
    /// The task name / is_retry tuple used for metrics tracking.
    const TaskMetricsKey task_key_;

    void IncrementMissingDependencies() {
      if (num_missing_dependencies_ == 0) {
        waiting_task_counter_map_.Increment(task_key_);
      }
      num_missing_dependencies_++;
    }

    void DecrementMissingDependencies() {
      num_missing_dependencies_--;
      if (num_missing_dependencies_ == 0) {
        waiting_task_counter_map_.Decrement(task_key_);
      }
    }

    LeaseDependencies(const LeaseDependencies &) = delete;
    LeaseDependencies &operator=(const LeaseDependencies &) = delete;

    ~LeaseDependencies() {
      if (num_missing_dependencies_ > 0) {
        waiting_task_counter_map_.Decrement(task_key_);
      }
    }
  };

  /// Stop tracking this object, if it is no longer needed by any worker or
  /// queued task.
  void RemoveObjectIfNotNeeded(
      absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator required_object_it);

  /// Start tracking an object that is needed by a worker and/or queued lease.
  absl::flat_hash_map<ObjectID, ObjectDependencies>::iterator GetOrInsertRequiredObject(
      const ObjectID &object_id, const rpc::ObjectReference &ref);

  /// The object manager, used to fetch required objects from remote nodes.
  ObjectManagerInterface &object_manager_;

  /// A map from the ID of a queued lease to metadata about whether the lease's
  /// dependencies are all local or not.
  absl::flat_hash_map<LeaseID, std::unique_ptr<LeaseDependencies>> queued_lease_requests_;

  // Maps a GetRequest to the PullRequest Id and the set of ObjectIDs.
  // Used to cleanup a finished or cancel an inflight get request.
  // TODO(57911): This can be slimmed down. We do not need to track the ObjectIDs.
  absl::flat_hash_map<std::pair<WorkerID, GetRequestId>,
                      std::pair<std::vector<ObjectID>, PullRequestId>,
                      absl::Hash<std::pair<WorkerID, GetRequestId>>>
      get_requests_;

  // Used to clean up all get requests for a worker.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<GetRequestId>> worker_to_requests_;

  /// A map from worker ID to the set of objects that the worker called
  /// `ray.wait` on. Objects are removed from the set once they are made local,
  /// or the worker cancels the `ray.wait` request.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ObjectID>> wait_requests_;

  /// Deduplicated pool of objects required by all queued leases and workers.
  /// Objects are removed from this set once there are no more leases or workers
  /// that require it.
  absl::flat_hash_map<ObjectID, ObjectDependencies> required_objects_;

  /// The set of locally available objects. This is used to determine which
  /// leases are ready to run and which `ray.wait` requests can be finished.
  absl::flat_hash_set<ray::ObjectID> local_objects_;

  /// Counts the number of active lease dependency fetches by lease name. The counter
  /// total will be less than or equal to the size of queued_lease_requests_.
  CounterMap<TaskMetricsKey> waiting_leases_counter_;

  // Metric to track the number of tasks by state.
  // Expected tags:
  // - State: the task state, as described by rpc::TaskState proto in common.proto
  // - Name: the name of the function called
  // - IsRetry: whether the task is a retry
  // - Source: component reporting, e.g., "core_worker", "executor", or "pull_manager"
  ray::observability::MetricInterface &task_by_state_counter_;

  friend class LeaseDependencyManagerTest;
};

}  // namespace raylet

}  // namespace ray
