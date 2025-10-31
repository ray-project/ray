// Copyright 2020-2021 The Ray Authors.
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

#include <deque>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/lease/lease.h"
#include "ray/common/ray_object.h"
#include "ray/raylet/lease_dependency_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/internal.h"
#include "ray/raylet/scheduling/local_lease_manager_interface.h"
#include "ray/raylet/worker_interface.h"
#include "ray/raylet/worker_pool.h"

namespace ray {
namespace raylet {

/// Manages the lifetime of a lease on the local node. It receives request from
/// cluster_lease_manager (the distributed scheduler) and does the following
/// steps:
/// 1. Pulling lease dependencies, add the lease into waiting queue.
/// 2. Once lease's dependencies are all pulled locally, the lease is added into
///    the grant queue.
/// 3. For all leases in the grant queue, we schedule them by first acquiring
///    local resources (including pinning the objects in memory and deducting
///    cpu/gpu and other resources from the local resource manager).
///    If a lease failed to acquire resources in step 3, we will try to
///    spill it to a different remote node.
/// 4. If all resources are acquired, we start a worker and return the worker
///    address to the client once worker starts up.
/// 5. When a worker finishes executing its task(s), the requester will return
///    the lease and we should release the resources in our view of the node's state.
/// 6. If a lease has been waiting for arguments for too long, it will also be
///    spilled back to a different node.
///
/// TODO(scv119): ideally, the local scheduler shouldn't be responsible for spilling,
/// as it should return the request to the distributed scheduler if
/// resource accusition failed, or a lease has arguments pending resolution for too long
/// time.
class LocalLeaseManager : public LocalLeaseManagerInterface {
 public:
  /// Create a local lease manager.
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  ///                                    the state of the cluster.
  /// \param lease_dependency_manager_ Used to fetch lease's dependencies.
  /// \param get_node_info: Function that returns the node info for a node.
  /// \param worker_pool: A reference to the worker pool.
  /// \param leased_workers: A reference to the leased workers map.
  /// \param get_lease_arguments: A callback for getting a leases' arguments by
  ///                            their ids.
  /// \param max_pinned_lease_arguments_bytes: The cap on pinned arguments.
  /// \param get_time_ms: A callback which returns the current time in milliseconds.
  /// \param sched_cls_cap_interval_ms: The time before we increase the cap
  ///                                   on the number of leases that can run per
  ///                                   scheduling class. If set to 0, there is no
  ///                                   cap. If it's a large number, the cap is hard.
  LocalLeaseManager(
      const NodeID &self_node_id,
      ClusterResourceScheduler &cluster_resource_scheduler,
      LeaseDependencyManagerInterface &lease_dependency_manager,
      internal::NodeInfoGetter get_node_info,
      WorkerPoolInterface &worker_pool,
      absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers,
      std::function<bool(const std::vector<ObjectID> &object_ids,
                         std::vector<std::unique_ptr<RayObject>> *results)>
          get_lease_arguments,
      size_t max_pinned_lease_arguments_bytes,
      std::function<int64_t(void)> get_time_ms =
          []() { return static_cast<int64_t>(absl::GetCurrentTimeNanos() / 1e6); },
      int64_t sched_cls_cap_interval_ms =
          RayConfig::instance().worker_cap_initial_backoff_delay_ms());

  /// Queue lease and schedule.
  void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) override;

  // Schedule and dispatch leases.
  void ScheduleAndGrantLeases() override;

  /// Move leases from waiting to ready for dispatch. Called when a lease's
  /// dependencies are resolved.
  ///
  /// \param ready_ids: The leases which are now ready to be granted.
  void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) override;

  /// Cleanup the lease and release the worker resources.
  /// This method will be removed and can be replaced by `ReleaseWorkerResources` directly
  /// once we remove the legacy scheduler.
  ///
  /// \param worker: The worker which was granted the lease.
  /// \param lease: Output parameter.
  void CleanupLease(std::shared_ptr<WorkerInterface> worker, RayLease *lease) override;

  /// Attempt to cancel all queued leases that match the predicate.
  ///
  /// \param predicate: A function that returns true if a lease needs to be cancelled.
  /// \param failure_type: The reason for cancellation.
  /// \param scheduling_failure_message: The reason message for cancellation.
  /// \return True if any lease was successfully cancelled.
  bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) override;

  std::vector<std::shared_ptr<internal::Work>> CancelLeasesWithoutReply(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate) override;

  /// Return with an exemplar if any leases are pending resource acquisition.
  ///
  /// \param[in,out] num_pending_actor_creation: Number of pending actor creation leases.
  /// \param[in,out] num_pending_leases: Number of pending leases.
  /// \return An example lease that is deadlocking if any leases are pending resource
  /// acquisition.
  const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const override;

  /// Call once a lease finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was granted the lease.
  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override;

  /// When a lease is blocked in ray.get or ray.wait, the worker who is executing the
  /// lease should give up the CPU resources allocated for the granted lease for the time
  /// being and the worker itself should also be marked as blocked.
  ///
  /// \param worker: The worker who will give up the CPU resources.
  /// \return true if the cpu resources of the specified worker are released successfully,
  /// else false.
  bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  /// When a lease is no longer blocked in a ray.get or ray.wait, the CPU resources that
  /// the worker gave up should be returned to it.
  ///
  /// \param worker The blocked worker.
  /// \return true if the cpu resources are returned back to the specified worker, else
  /// false.
  bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  /// TODO(Chong-Li): Removing this and maintaining normal task resources by local
  /// resource manager.
  /// Calculate normal task resources.
  ResourceSet CalcNormalTaskResources() const override;

  void SetWorkerBacklog(SchedulingClass scheduling_class,
                        const WorkerID &worker_id,
                        int64_t backlog_size) override;

  void ClearWorkerBacklog(const WorkerID &worker_id) override;

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetLeasesToGrant() const override {
    return leases_to_grant_;
  }

  const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const override {
    return backlog_tracker_;
  }

  void RecordMetrics() const override;

  void DebugStr(std::stringstream &buffer) const override;

  size_t GetNumLeaseSpilled() const override { return num_lease_spilled_; }
  size_t GetNumWaitingLeaseSpilled() const override { return num_waiting_lease_spilled_; }
  size_t GetNumUnschedulableLeaseSpilled() const override {
    return num_unschedulable_lease_spilled_;
  }

  bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                     const LeaseID &lease_id) const override;

  bool AddReplyCallback(const SchedulingClass &scheduling_class,
                        const LeaseID &lease_id,
                        rpc::SendReplyCallback send_reply_callback,
                        rpc::RequestWorkerLeaseReply *reply) override;

 private:
  struct SchedulingClassInfo;

  void RemoveFromGrantedLeasesIfExists(const RayLease &lease);

  /// Handle the popped worker from worker pool.
  bool PoppedWorkerHandler(const std::shared_ptr<WorkerInterface> worker,
                           PopWorkerStatus status,
                           const LeaseID &lease_id,
                           SchedulingClass scheduling_class,
                           const std::shared_ptr<internal::Work> &work,
                           bool is_detached_actor,
                           const rpc::Address &owner_address,
                           const std::string &runtime_env_setup_error_message);

  /// Cancels a lease in leases_to_grant_. Does not remove it from leases_to_grant_.
  void CancelLeaseToGrantWithoutReply(const std::shared_ptr<internal::Work> &work);

  /// Attempts to grant all leases which are ready to run. A lease
  /// will be granted if it is on `leases_to_grant_` and there are still
  /// available resources on the node.
  ///
  /// If there are not enough resources locally, up to one lease per resource
  /// shape (the lease at the head of the queue) will get spilled back to a
  /// different node.
  void GrantScheduledLeasesToWorkers();

  /// Helper method when the current node does not have the available resources to run a
  /// lease.
  ///
  /// \returns true if the lease was spilled. The lease may not be spilled if the
  /// spillback policy specifies the local node (which may happen if no other nodes have
  /// the requested resources available).
  bool TrySpillback(const std::shared_ptr<internal::Work> &work, bool &is_infeasible);

  // Try to spill waiting leases to a remote node, starting from the end of the
  // queue.
  void SpillWaitingLeases();

  /// Calculate the maximum number of granted leases for a given scheduling
  /// class. https://github.com/ray-project/ray/issues/16973
  ///
  /// \param sched_cls_id The scheduling class in question.
  /// \returns The maximum number instances of that scheduling class that
  ///          should be granted (or blocked) at once.
  uint64_t MaxGrantedLeasesPerSchedulingClass(SchedulingClass sched_cls_id) const;

  /// Recompute the debug stats.
  /// It is needed because updating the debug state is expensive for
  /// cluster_lease_manager.
  /// TODO(sang): Update the internal states value dynamically instead of iterating the
  /// data structure.
  void RecomputeDebugStats() const;

  void Grant(
      std::shared_ptr<WorkerInterface> worker,
      absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      const std::shared_ptr<TaskResourceInstances> &allocated_instances,
      const RayLease &lease,
      const std::vector<internal::ReplyCallback> &reply_callbacks);

  void Spillback(const NodeID &spillback_to, const std::shared_ptr<internal::Work> &work);

  // Helper function to pin a lease's args immediately before being granted. This
  // returns false if there are missing args (due to eviction) or if there is
  // not enough memory available to grant the lease, due to other granted
  // leases' arguments.
  bool PinLeaseArgsIfMemoryAvailable(const LeaseSpecification &lease_spec,
                                     bool *args_missing);

  // Helper functions to pin and release a granted lease's args.
  void PinLeaseArgs(const LeaseSpecification &lease_spec,
                    std::vector<std::unique_ptr<RayObject>> args);
  void ReleaseLeaseArgs(const LeaseID &lease_id);

 private:
  /// Determine whether a lease should be immediately granted,
  /// or placed on a wait queue.
  void WaitForLeaseArgsRequests(std::shared_ptr<internal::Work> work);

  const NodeID &self_node_id_;
  const scheduling::NodeID self_scheduling_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  ClusterResourceScheduler &cluster_resource_scheduler_;
  /// Class to make lease dependencies to be local.
  LeaseDependencyManagerInterface &lease_dependency_manager_;
  /// Function to get the node information of a given node id.
  internal::NodeInfoGetter get_node_info_;

  const int max_resource_shapes_per_load_report_;

  /// Tracking information about the currently granted leases in a scheduling
  /// class. This information is used to place a cap on the number of
  /// granted leases per scheduling class.
  struct SchedulingClassInfo {
    explicit SchedulingClassInfo(int64_t cap)
        : capacity(cap), next_update_time(std::numeric_limits<int64_t>::max()) {}
    /// Track the granted lease ids in this scheduling class.
    ///
    /// TODO(hjiang): Store cgroup manager along with lease id as the value for map.
    absl::flat_hash_set<LeaseID> granted_leases;
    /// The total number of leases that can run from this scheduling class.
    uint64_t capacity;
    /// The next time that a new lease of this scheduling class may be dispatched.
    int64_t next_update_time;
  };

  /// Mapping from scheduling class to information about the granted leases of
  /// the scheduling class. See `struct SchedulingClassInfo` above for more
  /// details about what information is tracked.
  absl::flat_hash_map<SchedulingClass, SchedulingClassInfo> info_by_sched_cls_;

  /// Queue of lease requests that should be scheduled onto workers.
  /// Leases move from scheduled | waiting -> granting.
  /// Leases can also move from granting -> waiting if one of their arguments is
  /// evicted.
  /// All leases in this map that have dependencies should be registered with
  /// the dependency manager, in case a dependency gets evicted while the lease
  /// is still queued.
  /// Note that if a queue exists, it should be guaranteed to be non-empty.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      leases_to_grant_;

  /// Leases waiting for arguments to be transferred locally.
  /// Leases move from waiting -> granting.
  /// Leases can also move from granting -> waiting if one of their arguments is
  /// evicted.
  /// All leases in this map that have dependencies should be registered with
  /// the dependency manager, so that they can be moved to granting once their
  /// dependencies are local.

  /// We keep these in a queue so that leases can be spilled back from the end
  /// of the queue. This is to try to prioritize spilling leases whose
  /// dependencies may not be fetched locally yet.

  /// Note that because leases can also move from grant -> waiting, the order
  /// in this queue may not match the order in which we initially received the
  /// leases. This also means that the PullManager may request dependencies for
  /// these leases in a different order than the waiting lease queue.
  /// Note that if a queue exists, it should be guaranteed to be non-empty.
  std::list<std::shared_ptr<internal::Work>> waiting_lease_queue_;

  /// An index for the above queue.
  absl::flat_hash_map<LeaseID, std::list<std::shared_ptr<internal::Work>>::iterator>
      waiting_leases_index_;

  /// Track the backlog of all workers belonging to this raylet.
  absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      backlog_tracker_;

  WorkerPoolInterface &worker_pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers_;

  /// Callback to get references to lease arguments. These will be pinned while
  /// the lease is granted.
  std::function<bool(const std::vector<ObjectID> &object_ids,
                     std::vector<std::unique_ptr<RayObject>> *results)>
      get_lease_arguments_;

  /// Arguments needed by currently granted leases. These should be pinned before
  /// the lease is granted to ensure that the arguments are not evicted.
  absl::flat_hash_map<LeaseID, std::vector<ObjectID>> granted_lease_args_;

  /// All arguments of granted leases, which are also pinned in the object store.
  /// The value is a pair: (the pointer to the object store that should be deleted
  /// once the object is no longer needed, number of leases that depend on the
  /// object).
  absl::flat_hash_map<ObjectID, std::pair<std::unique_ptr<RayObject>, size_t>>
      pinned_lease_arguments_;

  /// The total number of arguments pinned for granted leases.
  /// Used for debug purposes.
  size_t pinned_lease_arguments_bytes_ = 0;

  /// The maximum amount of bytes that can be used by granted lease arguments.
  size_t max_pinned_lease_arguments_bytes_;

  /// Returns the current time in milliseconds.
  std::function<int64_t()> get_time_ms_;

  /// Whether or not to enable the worker process cap.
  const bool sched_cls_cap_enabled_;

  /// The initial interval before the cap on the number of worker processes is increased.
  const int64_t sched_cls_cap_interval_ms_;

  const int64_t sched_cls_cap_max_ms_;

  size_t num_lease_spilled_ = 0;
  size_t num_waiting_lease_spilled_ = 0;
  size_t num_unschedulable_lease_spilled_ = 0;

  friend class SchedulerResourceReporter;
  friend class ClusterLeaseManagerTest;
  friend class SchedulerStats;
  friend class LocalLeaseManagerTest;
  FRIEND_TEST(ClusterLeaseManagerTest, FeasibleToNonFeasible);
  FRIEND_TEST(LocalLeaseManagerTest, TestLeaseGrantingOrder);
  friend size_t GetPendingLeaseWorkerCount(const LocalLeaseManager &local_lease_manager);
};
}  // namespace raylet
}  // namespace ray
