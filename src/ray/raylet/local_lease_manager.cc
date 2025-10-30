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

#include "ray/raylet/local_lease_manager.h"

#include <google/protobuf/map.h>

#include <algorithm>
#include <boost/range/join.hpp>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/placement_group_util.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"

namespace ray {
namespace raylet {

namespace {
void ReplyCancelled(const std::shared_ptr<internal::Work> &work,
                    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
                    const std::string &scheduling_failure_message) {
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->set_canceled(true);
    reply->set_failure_type(failure_type);
    reply->set_scheduling_failure_message(scheduling_failure_message);
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
}  // namespace

LocalLeaseManager::LocalLeaseManager(
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
    std::function<int64_t(void)> get_time_ms,
    int64_t sched_cls_cap_interval_ms)
    : self_node_id_(self_node_id),
      self_scheduling_node_id_(self_node_id.Binary()),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      lease_dependency_manager_(lease_dependency_manager),
      get_node_info_(get_node_info),
      max_resource_shapes_per_load_report_(
          RayConfig::instance().max_resource_shapes_per_load_report()),
      worker_pool_(worker_pool),
      leased_workers_(leased_workers),
      get_lease_arguments_(get_lease_arguments),
      max_pinned_lease_arguments_bytes_(max_pinned_lease_arguments_bytes),
      get_time_ms_(get_time_ms),
      sched_cls_cap_enabled_(RayConfig::instance().worker_cap_enabled()),
      sched_cls_cap_interval_ms_(sched_cls_cap_interval_ms),
      sched_cls_cap_max_ms_(RayConfig::instance().worker_cap_max_backoff_delay_ms()) {}

void LocalLeaseManager::QueueAndScheduleLease(std::shared_ptr<internal::Work> work) {
  // If the local node is draining, the cluster lease manager will
  // guarantee that the local node is not selected for scheduling.
  RAY_CHECK(!cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining());
  // The local node must be feasible if the cluster lease manager decides to run the task
  // locally.
  RAY_CHECK(cluster_resource_scheduler_.GetClusterResourceManager().HasFeasibleResources(
      self_scheduling_node_id_,
      ResourceMapToResourceRequest(work->lease_.GetLeaseSpecification()
                                       .GetRequiredPlacementResources()
                                       .GetResourceMap(),
                                   /*requires_object_store_memory=*/false)))
      << work->lease_.GetLeaseSpecification().DebugString() << " "
      << cluster_resource_scheduler_.GetClusterResourceManager()
             .GetNodeResources(self_scheduling_node_id_)
             .DebugString();
  WaitForLeaseArgsRequests(std::move(work));
  ScheduleAndGrantLeases();
}

void LocalLeaseManager::WaitForLeaseArgsRequests(std::shared_ptr<internal::Work> work) {
  const auto &lease = work->lease_;
  const auto &lease_id = lease.GetLeaseSpecification().LeaseId();
  const auto &scheduling_key = lease.GetLeaseSpecification().GetSchedulingClass();
  auto object_ids = lease.GetLeaseSpecification().GetDependencies();
  if (!object_ids.empty()) {
    bool args_ready = lease_dependency_manager_.RequestLeaseDependencies(
        lease_id,
        lease.GetLeaseSpecification().GetDependencies(),
        {lease.GetLeaseSpecification().GetTaskName(),
         lease.GetLeaseSpecification().IsRetry()});
    if (args_ready) {
      RAY_LOG(DEBUG) << "Args already ready, lease can be granted " << lease_id;
      leases_to_grant_[scheduling_key].emplace_back(std::move(work));
    } else {
      RAY_LOG(DEBUG) << "Waiting for args for lease: " << lease_id;
      auto it = waiting_lease_queue_.insert(waiting_lease_queue_.end(), std::move(work));
      RAY_CHECK(waiting_leases_index_.emplace(lease_id, it).second);
    }
  } else {
    RAY_LOG(DEBUG) << "No args, lease can be granted " << lease_id;
    leases_to_grant_[scheduling_key].emplace_back(std::move(work));
  }
}

void LocalLeaseManager::ScheduleAndGrantLeases() {
  GrantScheduledLeasesToWorkers();
  // TODO(swang): Spill from waiting queue first? Otherwise, we may end up
  // spilling a lease whose args are already local.
  // TODO(swang): Invoke ScheduleAndGrantLeases() when we run out of memory
  // in the PullManager or periodically, to make sure that we spill waiting
  // leases that are blocked.
  SpillWaitingLeases();
}

void LocalLeaseManager::GrantScheduledLeasesToWorkers() {
  // Check every lease in leases_to_grant queue to see
  // whether it can be granted and ran. This avoids head-of-line
  // blocking where a lease which cannot be granted because
  // there are not enough available resources blocks other
  // leases from being granted.
  for (auto shapes_it = leases_to_grant_.begin(); shapes_it != leases_to_grant_.end();) {
    auto &scheduling_class = shapes_it->first;
    auto &leases_to_grant_queue = shapes_it->second;

    auto sched_cls_iter = info_by_sched_cls_.find(scheduling_class);
    if (sched_cls_iter == info_by_sched_cls_.end()) {
      // Initialize the class info.
      sched_cls_iter =
          info_by_sched_cls_
              .emplace(scheduling_class,
                       SchedulingClassInfo(
                           MaxGrantedLeasesPerSchedulingClass(scheduling_class)))
              .first;
    }
    auto &sched_cls_info = sched_cls_iter->second;

    // Fair scheduling is applied only when the total CPU requests exceed the node's
    // capacity. This skips scheduling classes whose number of granted leases exceeds the
    // average number of granted leases per scheduling class.

    // The purpose of fair scheduling is to ensure that each scheduling class has an
    // equal chance of being selected for lease granting. For instance, in a pipeline with
    // both data producers and consumers, we aim for consumers to have the same chance to
    // be granted a lease as producers. This prevents memory peak caused by granting all
    // producer leases first.
    // A scheduling class is skipped from lease granting if its number of granted leases
    // exceeds the fair_share, which is the average number of granted leases among all
    // scheduling classes. For example, consider a scenario where we have 3 CPUs and 2
    // scheduling classes, `f` and `g`, each with 4 leases.
    // Status 1: The queue init with [f, f, f, f, g, g, g, g], and 0 granted leases.
    // Status 2: We grant 3 `f` leases. Now the queue is [f, g, g, g, g],
    //           with 3 `f` leases granted.
    // Status 3: Suppose 1 `f` lease finishes. When choosing the next lease to grant,
    //           the queue is [f, g, g, g, g], and there are 2 `f` leases granted.
    //           We calculate fair_share as follows:
    //           fair_share = number of granted leases / number of scheduling classes
    //                       = 2 / 2 = 1.
    //           Since the number of granted `f` leases (2) is greater than the
    //           fair_share (1), we skip `f` and choose to grant `g`.
    // Note 1: Fair_share is calculated as (total number of granted leases with >0 CPU)
    //         / (number of scheduling classes in leases_to_dispatch_).
    // Note 2: The decision to skip a scheduling class happens when loop through the
    //         scheduling classes (keys of leases_to_grant_). This means we check for
    //         fair dispatching when looping through the scheduling classes rather than
    //         for each individual lease, reducing the number of checks required.
    //         This is why in Status 2 of the example, we grant 3 `f` leases because
    //         we chose `f` for grant, and we continue granting all `f`
    //         leases until resources are fully utilized.

    // Currently, fair granting is implemented only for leases that require CPU
    // resources. CPU. For details, see https://github.com/ray-project/ray/pull/44733.

    // Calculate the total CPU requests for all leases in the leases_to_grant queue.
    double total_cpu_requests_ = 0.0;

    // Count the number of scheduling classes that require CPU and sum their total CPU
    // requests.
    size_t num_classes_with_cpu = 0;
    for (const auto &[_, cur_dispatch_queue] : leases_to_grant_) {
      // Only need to check the first because all tasks with the same scheduling class
      // have the same CPU resource requirements.
      RAY_CHECK(!cur_dispatch_queue.empty());
      const auto &work = cur_dispatch_queue.front();
      const auto &lease_spec = work->lease_.GetLeaseSpecification();
      auto cpu_request_ =
          lease_spec.GetRequiredResources().Get(scheduling::ResourceID::CPU()).Double();
      if (cpu_request_ > 0) {
        num_classes_with_cpu++;
        total_cpu_requests_ += cur_dispatch_queue.size() * cpu_request_;
      }
    }
    const auto &sched_cls_desc =
        SchedulingClassToIds::GetSchedulingClassDescriptor(scheduling_class);
    double total_cpus =
        cluster_resource_scheduler_.GetLocalResourceManager().GetNumCpus();

    // Compare total CPU requests with the node's total CPU capacity. If the requests
    // exceed the capacity, check if fair granting is needed.
    if (sched_cls_desc.resource_set.Get(scheduling::ResourceID::CPU()).Double() > 0 &&
        total_cpu_requests_ > total_cpus) {
      RAY_LOG(DEBUG)
          << "Applying fairness policy. Total CPU requests in leases_to_grant_ ("
          << total_cpu_requests_ << ") exceed total CPUs available (" << total_cpus
          << ").";
      // Get the total number of granted leases that require CPU.
      size_t total_cpu_granted_leases = 0;
      for (auto &entry : info_by_sched_cls_) {
        // Only consider CPU requests
        const auto &cur_sched_cls_desc =
            SchedulingClassToIds::GetSchedulingClassDescriptor(entry.first);
        if (cur_sched_cls_desc.resource_set.Get(scheduling::ResourceID::CPU()).Double() >
            0) {
          total_cpu_granted_leases += entry.second.granted_leases.size();
        }
      }

      // 1. We have confirmed that this is a scheduling class that requires CPU resources,
      //    hence num_classes_with_cpu >= 1 (cannot be 0) as this scheduling class is in
      //    leases_to_grant_.
      // 2. We will compute fair_share as the ideal distribution of leases among all
      //    scheduling classes in leases_to_grant_. Then, we will check if the number
      //    of granted leases for this scheduling class exceeds its ideal fair_share.
      // 3. Note: We should get the num_classes_with_cpu from leases_to_grant_
      //    instead of the info_by_sched_cls_ although total_cpu_granted_leases is
      //    obtained from the granted leases. First, info_by_sched_cls_ may not be
      //    initialized yet for some scheduling classes (as we initialize it in the loop).
      //    Second, we expect the number of granted leases for this scheduling class to
      //    not be much. However, if no leases of this scheduling class are granted, it
      //    will not be skipped.

      size_t fair_share = total_cpu_granted_leases / num_classes_with_cpu;
      if (sched_cls_info.granted_leases.size() > fair_share) {
        RAY_LOG(DEBUG) << "Skipping lease granting for scheduling class "
                       << scheduling_class << ". Granted leases ("
                       << sched_cls_info.granted_leases.size() << ") exceed fair share ("
                       << fair_share << ").";
        shapes_it++;
        continue;
      }
    }

    /// We cap the maximum granted leases of a scheduling class to avoid
    /// granting too many leases of a single type/depth, when there are
    /// deeper/other functions that should be run. We need to apply back
    /// pressure to limit the number of worker processes started in scenarios
    /// with nested tasks.
    bool is_infeasible = false;
    for (auto work_it = leases_to_grant_queue.begin();
         work_it != leases_to_grant_queue.end();) {
      auto &work = *work_it;
      const auto &lease = work->lease_;
      const auto &spec = lease.GetLeaseSpecification();
      LeaseID lease_id = spec.LeaseId();
      if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
        work_it++;
        continue;
      }

      // Check if the scheduling class is at capacity now.
      if (sched_cls_cap_enabled_ &&
          sched_cls_info.granted_leases.size() >= sched_cls_info.capacity &&
          work->GetState() == internal::WorkStatus::WAITING) {
        RAY_LOG(DEBUG) << "Hit cap! time=" << get_time_ms_()
                       << " next update time=" << sched_cls_info.next_update_time;
        if (get_time_ms_() < sched_cls_info.next_update_time) {
          // We're over capacity and it's not time to grant a new lease yet.
          // Calculate the next time we should grant a new lease.
          int64_t current_capacity = sched_cls_info.granted_leases.size();
          int64_t allowed_capacity = sched_cls_info.capacity;
          int64_t exp = current_capacity - allowed_capacity;
          int64_t wait_time = sched_cls_cap_interval_ms_ * (1L << exp);
          if (wait_time > sched_cls_cap_max_ms_) {
            wait_time = sched_cls_cap_max_ms_;
            RAY_LOG(WARNING) << "Starting too many worker processes for a single type of "
                                "task. Worker process startup is being throttled.";
          }

          int64_t target_time = get_time_ms_() + wait_time;
          sched_cls_info.next_update_time =
              std::min(target_time, sched_cls_info.next_update_time);

          // While we're over capacity and cannot grant the lease,
          // try to spill to a node that can.
          bool did_spill = TrySpillback(work, is_infeasible);
          if (did_spill) {
            work_it = leases_to_grant_queue.erase(work_it);
            continue;
          }

          break;
        }
      }

      bool args_missing = false;
      bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);
      // An argument was evicted since this lease was added to the grant
      // queue. Move it back to the waiting queue. The caller is responsible
      // for notifying us when the lease is unblocked again.
      if (!success) {
        if (args_missing) {
          // Insert the lease at the head of the waiting queue because we
          // prioritize spilling from the end of the queue.
          // TODO(scv119): where does pulling happen?
          auto it = waiting_lease_queue_.insert(waiting_lease_queue_.begin(),
                                                std::move(*work_it));
          RAY_CHECK(waiting_leases_index_.emplace(lease_id, it).second);
          work_it = leases_to_grant_queue.erase(work_it);
        } else {
          // The lease's args cannot be pinned due to lack of memory. We should
          // retry granting the lease once another lease finishes and releases
          // its arguments.
          RAY_LOG(DEBUG) << "Granting lease " << lease_id
                         << " would put this node over the max memory allowed for "
                            "arguments of granted leases ("
                         << max_pinned_lease_arguments_bytes_
                         << "). Waiting to grant lease until other leases are returned";
          RAY_CHECK(!granted_lease_args_.empty() && !pinned_lease_arguments_.empty())
              << "Cannot grant lease " << lease_id
              << " until another lease is returned and releases its arguments, but no "
                 "other lease is granted";
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
          work_it++;
        }
        continue;
      }

      // Check if the node is still schedulable. It may not be if dependency resolution
      // took a long time.
      auto allocated_instances = std::make_shared<TaskResourceInstances>();
      bool schedulable =
          !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
          cluster_resource_scheduler_.GetLocalResourceManager()
              .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                          allocated_instances);
      if (!schedulable) {
        ReleaseLeaseArgs(lease_id);
        // The local node currently does not have the resources to grant the lease, so we
        // should try spilling to another node.
        bool did_spill = TrySpillback(work, is_infeasible);
        if (!did_spill) {
          // There must not be any other available nodes in the cluster, so the lease
          // should stay on this node. We can skip the rest of the shape because the
          // scheduler will make the same decision.
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
          break;
        }
        work_it = leases_to_grant_queue.erase(work_it);
      } else {
        // Force us to recalculate the next update time the next time a task
        // comes through this queue. We should only do this when we're
        // confident we're ready to dispatch the task after all checks have
        // passed.
        sched_cls_info.next_update_time = std::numeric_limits<int64_t>::max();
        sched_cls_info.granted_leases.insert(lease_id);
        // The local node has the available resources to grant the lease, so we should
        // grant it.
        work->allocated_instances_ = allocated_instances;
        work->SetStateWaitingForWorker();
        bool is_detached_actor = spec.IsDetachedActor();
        auto &owner_address = spec.CallerAddress();
        /// TODO(scv119): if a worker is not started, the resources are leaked and
        // task might be hanging.
        worker_pool_.PopWorker(
            spec,
            [this, lease_id, scheduling_class, work, is_detached_actor, owner_address](
                const std::shared_ptr<WorkerInterface> worker,
                PopWorkerStatus status,
                const std::string &runtime_env_setup_error_message) -> bool {
              // TODO(hjiang): After getting the ready-to-use worker and lease id, we're
              // able to get physical execution context.
              //
              // ownership chain: raylet has-a node manager, node manager has-a local task
              // manager.
              //
              // - PID: could get from available worker
              // - Attempt id: could pass a global attempt id generator from raylet
              // - Cgroup application folder: could pass from raylet

              return PoppedWorkerHandler(worker,
                                         status,
                                         lease_id,
                                         scheduling_class,
                                         work,
                                         is_detached_actor,
                                         owner_address,
                                         runtime_env_setup_error_message);
            });
        work_it++;
      }
    }
    // In the beginning of the loop, we add scheduling_class
    // to the `info_by_sched_cls_` map.
    // In cases like dead owners, we may not add any tasks
    // to `granted_leases` so we can remove the map entry
    // for that scheduling_class to prevent memory leaks.
    if (sched_cls_info.granted_leases.size() == 0) {
      info_by_sched_cls_.erase(scheduling_class);
    }
    if (is_infeasible) {
      const auto &front_lease =
          leases_to_grant_queue.front()->lease_.GetLeaseSpecification();
      RAY_LOG(ERROR) << "A lease got granted to a node even though it was infeasible. "
                        "Please report an issue on GitHub.\nLease: "
                     << front_lease.DebugString();
      auto leases_to_grant_queue_iter = leases_to_grant_queue.begin();
      while (leases_to_grant_queue_iter != leases_to_grant_queue.end()) {
        CancelLeaseToGrantWithoutReply(*leases_to_grant_queue_iter);
        ReplyCancelled(*leases_to_grant_queue_iter,
                       rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
                       "Lease granting failed due to the lease becoming infeasible.");
        leases_to_grant_queue_iter =
            leases_to_grant_queue.erase(leases_to_grant_queue_iter);
      }
      leases_to_grant_.erase(shapes_it++);
    } else if (leases_to_grant_queue.empty()) {
      leases_to_grant_.erase(shapes_it++);
    } else {
      shapes_it++;
    }
  }
}

void LocalLeaseManager::SpillWaitingLeases() {
  // Try to spill waiting leases to a remote node, prioritizing those at the end
  // of the queue. Waiting leases are spilled if there are enough remote
  // resources AND (we have no resources available locally OR their
  // dependencies are not being fetched). We should not spill leases whose
  // dependencies are actively being fetched because some of their dependencies
  // may already be local or in-flight to this node.
  //
  // NOTE(swang): We do not iterate by scheduling class here, so if we break
  // due to lack of remote resources, it is possible that a waiting lease that
  // is earlier in the queue could have been scheduled to a remote node.
  // TODO(scv119): this looks very aggressive: we will try to spillback
  // all the leases in the waiting queue regardless of the wait time.
  auto it = waiting_lease_queue_.end();
  while (it != waiting_lease_queue_.begin()) {
    it--;
    const auto &lease = (*it)->lease_;
    const auto &lease_spec = lease.GetLeaseSpecification();
    const auto &lease_id = lease_spec.LeaseId();

    // Check whether this lease's dependencies are blocked (not being actively
    // pulled).  If this is true, then we should force the lease onto a remote
    // feasible node, even if we have enough resources available locally for
    // placement.
    bool lease_dependencies_blocked =
        lease_dependency_manager_.LeaseDependenciesBlocked(lease_id);
    RAY_LOG(DEBUG) << "Attempting to spill back waiting lease " << lease_id
                   << " to remote node. Dependencies blocked? "
                   << lease_dependencies_blocked;
    bool is_infeasible;
    // TODO(swang): The policy currently does not account for the amount of
    // object store memory availability. Ideally, we should pick the node with
    // the most memory availability.
    scheduling::NodeID scheduling_node_id;
    if (!lease_spec.IsSpreadSchedulingStrategy()) {
      scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease_spec,
          /*preferred_node_id*/ self_node_id_.Binary(),
          /*exclude_local_node*/ lease_dependencies_blocked,
          /*requires_object_store_memory*/ true,
          &is_infeasible);
    } else {
      // If scheduling strategy is spread, we prefer honoring spread decision
      // and waiting for lease dependencies to be pulled
      // locally than spilling back and causing uneven spread.
      scheduling_node_id = self_scheduling_node_id_;
    }

    if (!scheduling_node_id.IsNil() && scheduling_node_id != self_scheduling_node_id_) {
      NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
      Spillback(node_id, *it);
      if (!lease_spec.GetDependencies().empty()) {
        lease_dependency_manager_.RemoveLeaseDependencies(lease_id);
      }
      num_waiting_lease_spilled_++;
      waiting_leases_index_.erase(lease_id);
      it = waiting_lease_queue_.erase(it);
    } else {
      if (scheduling_node_id.IsNil()) {
        RAY_LOG(DEBUG) << "RayLease " << lease_id
                       << " has blocked dependencies, but no other node has resources, "
                          "keeping the lease local";
      } else {
        RAY_LOG(DEBUG) << "Keeping waiting lease " << lease_id << " local";
      }
      // We should keep the lease local. Note that an earlier lease in the queue
      // may have different resource requirements and could actually be
      // scheduled on a remote node.
      break;
    }
  }
}

bool LocalLeaseManager::TrySpillback(const std::shared_ptr<internal::Work> &work,
                                     bool &is_infeasible) {
  const auto &spec = work->lease_.GetLeaseSpecification();
  auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
      spec,
      // We should prefer to stay local if possible
      // to avoid unnecessary spillback
      // since this node is already selected by the cluster scheduler.
      /*preferred_node_id=*/self_node_id_.Binary(),
      /*exclude_local_node=*/false,
      /*requires_object_store_memory=*/false,
      &is_infeasible);

  if (is_infeasible || scheduling_node_id.IsNil() ||
      scheduling_node_id == self_scheduling_node_id_) {
    return false;
  }

  NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
  Spillback(node_id, work);
  num_unschedulable_lease_spilled_++;
  if (!spec.GetDependencies().empty()) {
    lease_dependency_manager_.RemoveLeaseDependencies(spec.LeaseId());
  }
  return true;
}

bool LocalLeaseManager::PoppedWorkerHandler(
    const std::shared_ptr<WorkerInterface> worker,
    PopWorkerStatus status,
    const LeaseID &lease_id,
    SchedulingClass scheduling_class,
    const std::shared_ptr<internal::Work> &work,
    bool is_detached_actor,
    const rpc::Address &owner_address,
    const std::string &runtime_env_setup_error_message) {
  const auto &reply_callbacks = work->reply_callbacks_;
  const bool canceled = work->GetState() == internal::WorkStatus::CANCELLED;
  const auto &lease = work->lease_;
  bool granted = false;

  if (!canceled) {
    const auto &required_resource =
        lease.GetLeaseSpecification().GetRequiredResources().GetResourceMap();
    for (auto &entry : required_resource) {
      // This is to make sure PG resource is not deleted during popping worker
      // unless the lease request is cancelled.
      RAY_CHECK(cluster_resource_scheduler_.GetLocalResourceManager().ResourcesExist(
          scheduling::ResourceID(entry.first)))
          << entry.first;
    }
  }

  // Erases the work from lease_to_grant_ queue, also removes the lease dependencies.
  //
  // IDEA(ryw): Make an RAII class to wrap the a shared_ptr<internal::Work> and
  // requests lease dependency upon ctor, and remove lease dependency upon dtor.
  // I tried this, it works, but we expose the map via GetLeasesToGrant() used in
  // scheduler_resource_reporter.cc. Maybe we can use `boost::any_range` to only expose
  // a view of the Work ptrs, but I got dependency issues
  // (can't include boost/range/any_range.hpp).
  auto erase_from_leases_to_grant_queue_fn =
      [this](const std::shared_ptr<internal::Work> &work_to_erase,
             const SchedulingClass &_scheduling_class) {
        auto shapes_it = leases_to_grant_.find(_scheduling_class);
        RAY_CHECK(shapes_it != leases_to_grant_.end());
        auto &leases_to_grant_queue = shapes_it->second;
        bool erased = false;
        for (auto work_it = leases_to_grant_queue.begin();
             work_it != leases_to_grant_queue.end();
             work_it++) {
          if (*work_it == work_to_erase) {
            leases_to_grant_queue.erase(work_it);
            erased = true;
            break;
          }
        }
        if (leases_to_grant_queue.empty()) {
          leases_to_grant_.erase(shapes_it);
        }
        RAY_CHECK(erased);

        const auto &_lease = work_to_erase->lease_;
        if (!_lease.GetLeaseSpecification().GetDependencies().empty()) {
          lease_dependency_manager_.RemoveLeaseDependencies(
              _lease.GetLeaseSpecification().LeaseId());
        }
      };

  if (canceled) {
    // Task has been canceled.
    RAY_LOG(DEBUG) << "Lease " << lease_id << " has been canceled when worker popped";
    RemoveFromGrantedLeasesIfExists(lease);
    // All the cleaning work has been done when canceled lease. Just return
    // false without doing anything.
    return false;
  }

  if (!worker) {
    granted = false;
    // We've already acquired resources so we need to release them.
    cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
        work->allocated_instances_);
    work->allocated_instances_ = nullptr;
    // Release pinned task args.
    ReleaseLeaseArgs(lease_id);
    RemoveFromGrantedLeasesIfExists(lease);

    // Empty worker popped.
    RAY_LOG(DEBUG).WithField(lease_id)
        << "This node has available resources, but no worker processes "
           "to grant the lease: status "
        << status;
    if (status == PopWorkerStatus::RuntimeEnvCreationFailed) {
      // In case of runtime env creation failed, we cancel this task
      // directly and raise a `RuntimeEnvSetupError` exception to user
      // eventually. The task will be removed from dispatch queue in
      // `CancelTask`.
      CancelLeases(
          [lease_id](const auto &w) {
            return lease_id == w->lease_.GetLeaseSpecification().LeaseId();
          },
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED,
          /*scheduling_failure_message*/ runtime_env_setup_error_message);
    } else if (status == PopWorkerStatus::JobFinished) {
      // The task job finished.
      // Just remove the task from dispatch queue.
      RAY_LOG(DEBUG) << "Call back to a job finished lease, lease id = " << lease_id;
      erase_from_leases_to_grant_queue_fn(work, scheduling_class);
    } else {
      // In other cases, set the work status `WAITING` to make this task
      // could be re-dispatched.
      internal::UnscheduledWorkCause cause =
          internal::UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST;
      if (status == PopWorkerStatus::JobConfigMissing) {
        cause = internal::UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST;
      } else if (status == PopWorkerStatus::WorkerPendingRegistration) {
        cause = internal::UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT;
      } else {
        RAY_LOG(FATAL) << "Unexpected state received for the empty pop worker. Status: "
                       << status;
      }
      work->SetStateWaiting(cause);
    }
  } else {
    // A worker has successfully popped for a valid lease. Grant the lease to
    // the worker.
    RAY_LOG(DEBUG) << "Granting lease " << lease_id << " to worker "
                   << worker->WorkerId();

    Grant(worker, leased_workers_, work->allocated_instances_, lease, reply_callbacks);
    erase_from_leases_to_grant_queue_fn(work, scheduling_class);
    granted = true;
  }

  return granted;
}

void LocalLeaseManager::Spillback(const NodeID &spillback_to,
                                  const std::shared_ptr<internal::Work> &work) {
  if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  num_lease_spilled_++;
  const auto &lease_spec = work->lease_.GetLeaseSpecification();
  RAY_LOG(DEBUG) << "Spilling lease " << lease_spec.LeaseId() << " to node "
                 << spillback_to;

  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          lease_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << lease_spec.LeaseId()
                   << " on a remote node that are no longer available";
  }

  auto node_info_ptr = get_node_info_(spillback_to);
  RAY_CHECK(node_info_ptr)
      << "Spilling back to a node manager, but no GCS info found for node "
      << spillback_to;
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        node_info_ptr->node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port(
        node_info_ptr->node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}

void LocalLeaseManager::LeasesUnblocked(const std::vector<LeaseID> &ready_ids) {
  if (ready_ids.empty()) {
    return;
  }

  for (const auto &lease_id : ready_ids) {
    auto it = waiting_leases_index_.find(lease_id);
    if (it != waiting_leases_index_.end()) {
      auto work = *it->second;
      const auto &lease = work->lease_;
      const auto &scheduling_key = lease.GetLeaseSpecification().GetSchedulingClass();
      RAY_LOG(DEBUG) << "Args ready, lease can be granted "
                     << lease.GetLeaseSpecification().LeaseId();
      leases_to_grant_[scheduling_key].push_back(work);
      waiting_lease_queue_.erase(it->second);
      waiting_leases_index_.erase(it);
    }
  }
  ScheduleAndGrantLeases();
}

void LocalLeaseManager::RemoveFromGrantedLeasesIfExists(const RayLease &lease) {
  auto sched_cls = lease.GetLeaseSpecification().GetSchedulingClass();
  auto it = info_by_sched_cls_.find(sched_cls);
  if (it != info_by_sched_cls_.end()) {
    // TODO(hjiang): After remove the lease id from `granted_leases`, corresponding cgroup
    // will be updated.
    it->second.granted_leases.erase(lease.GetLeaseSpecification().LeaseId());
    if (it->second.granted_leases.size() == 0) {
      info_by_sched_cls_.erase(it);
    }
  }
}

void LocalLeaseManager::CleanupLease(std::shared_ptr<WorkerInterface> worker,
                                     RayLease *lease) {
  RAY_CHECK(worker != nullptr && lease != nullptr);
  *lease = worker->GetGrantedLease();
  RemoveFromGrantedLeasesIfExists(*lease);

  ReleaseLeaseArgs(lease->GetLeaseSpecification().LeaseId());
  if (worker->GetAllocatedInstances() != nullptr) {
    ReleaseWorkerResources(worker);
  }
}

// TODO(scv119): lease args related logic probaly belongs lease dependency manager.
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &lease_spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = lease_spec.GetDependencyIds();
  if (!deps.empty()) {
    // This gets refs to the arguments stored in plasma. The refs should be
    // deleted once we no longer need to pin the arguments.
    if (!get_lease_arguments_(deps, &args)) {
      *args_missing = true;
      return false;
    }
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        // This can happen if the lease's arguments were all local at some
        // point, but then at least one was evicted before the lease could
        // be granted to a worker.
        RAY_LOG(DEBUG)
            << "RayLease " << lease_spec.LeaseId() << " argument " << deps[i]
            << " was evicted before the lease could be granted. This can happen "
               "when there are many objects needed on this node. The lease will be "
               "granted once all of its dependencies are local.";
        *args_missing = true;
        return false;
      }
    }
  }

  *args_missing = false;
  size_t lease_arg_bytes = 0;
  for (auto &arg : args) {
    lease_arg_bytes += arg->GetSize();
  }
  RAY_LOG(DEBUG) << "RayLease " << lease_spec.LeaseId() << " has args of size "
                 << lease_arg_bytes;
  PinLeaseArgs(lease_spec, std::move(args));
  RAY_LOG(DEBUG) << "Size of pinned task args is now " << pinned_lease_arguments_bytes_;
  if (max_pinned_lease_arguments_bytes_ == 0) {
    // Max threshold for pinned args is not set.
    return true;
  }

  if (lease_arg_bytes > max_pinned_lease_arguments_bytes_) {
    RAY_LOG(WARNING)
        << "Granted lease " << lease_spec.LeaseId() << " has arguments of size "
        << lease_arg_bytes
        << ", but the max memory allowed for arguments of granted leases is only "
        << max_pinned_lease_arguments_bytes_;
  } else if (pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_) {
    ReleaseLeaseArgs(lease_spec.LeaseId());
    RAY_LOG(DEBUG) << "Cannot grant lease " << lease_spec.LeaseId()
                   << " with arguments of size " << lease_arg_bytes
                   << " current pinned bytes is " << pinned_lease_arguments_bytes_;
    return false;
  }

  return true;
}

void LocalLeaseManager::PinLeaseArgs(const LeaseSpecification &lease_spec,
                                     std::vector<std::unique_ptr<RayObject>> args) {
  const auto &deps = lease_spec.GetDependencyIds();
  // TODO(swang): This should really be an assertion, but we can sometimes
  // receive a duplicate lease request if there is a failure and the original
  // version of the lease has not yet been canceled.
  auto executed_lease_inserted =
      granted_lease_args_.emplace(lease_spec.LeaseId(), deps).second;
  if (executed_lease_inserted) {
    for (size_t i = 0; i < deps.size(); i++) {
      auto [it, pinned_lease_inserted] =
          pinned_lease_arguments_.emplace(deps[i], std::make_pair(std::move(args[i]), 0));
      if (pinned_lease_inserted) {
        // This is the first lease that needed this argument.
        pinned_lease_arguments_bytes_ += it->second.first->GetSize();
      }
      it->second.second++;
    }
  } else {
    RAY_LOG(DEBUG) << "Scheduler received duplicate lease " << lease_spec.LeaseId()
                   << ", most likely because the first execution failed";
  }
}

void LocalLeaseManager::ReleaseLeaseArgs(const LeaseID &lease_id) {
  auto it = granted_lease_args_.find(lease_id);
  // TODO(swang): This should really be an assertion, but we can sometimes
  // receive a duplicate lease request if there is a failure and the original
  // version of the lease has not yet been canceled.
  if (it != granted_lease_args_.end()) {
    for (auto &arg : it->second) {
      auto arg_it = pinned_lease_arguments_.find(arg);
      RAY_CHECK(arg_it != pinned_lease_arguments_.end());
      RAY_CHECK(arg_it->second.second > 0);
      arg_it->second.second--;
      if (arg_it->second.second == 0) {
        // This is the last lease that needed this argument.
        pinned_lease_arguments_bytes_ -= arg_it->second.first->GetSize();
        pinned_lease_arguments_.erase(arg_it);
      }
    }
    granted_lease_args_.erase(it);
  }
}

std::vector<std::shared_ptr<internal::Work>> LocalLeaseManager::CancelLeasesWithoutReply(
    std::function<bool(const std::shared_ptr<internal::Work> &)> predicate) {
  std::vector<std::shared_ptr<internal::Work>> cancelled_works;

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      leases_to_grant_, [&](const std::shared_ptr<internal::Work> &work) {
        if (!predicate(work)) {
          return false;
        }
        CancelLeaseToGrantWithoutReply(work);
        cancelled_works.push_back(work);
        return true;
      });

  ray::erase_if<std::shared_ptr<internal::Work>>(
      waiting_lease_queue_, [&](const std::shared_ptr<internal::Work> &work) {
        if (!predicate(work)) {
          return false;
        }
        if (!work->lease_.GetLeaseSpecification().GetDependencies().empty()) {
          lease_dependency_manager_.RemoveLeaseDependencies(
              work->lease_.GetLeaseSpecification().LeaseId());
        }
        waiting_leases_index_.erase(work->lease_.GetLeaseSpecification().LeaseId());
        cancelled_works.push_back(work);
        return true;
      });

  return cancelled_works;
}

bool LocalLeaseManager::CancelLeases(
    std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  auto cancelled_works = CancelLeasesWithoutReply(predicate);
  for (const auto &work : cancelled_works) {
    ReplyCancelled(work, failure_type, scheduling_failure_message);
  }
  return !cancelled_works.empty();
}

void LocalLeaseManager::CancelLeaseToGrantWithoutReply(
    const std::shared_ptr<internal::Work> &work) {
  const LeaseID lease_id = work->lease_.GetLeaseSpecification().LeaseId();
  RAY_LOG(DEBUG) << "Canceling lease " << lease_id << " from leases_to_grant_queue.";
  if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
    // We've already acquired resources so we need to release them.
    cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
        work->allocated_instances_);
    // Release pinned lease args.
    ReleaseLeaseArgs(lease_id);
  }
  if (!work->lease_.GetLeaseSpecification().GetDependencies().empty()) {
    lease_dependency_manager_.RemoveLeaseDependencies(
        work->lease_.GetLeaseSpecification().LeaseId());
  }
  RemoveFromGrantedLeasesIfExists(work->lease_);
  work->SetStateCancelled();
}

const RayLease *LocalLeaseManager::AnyPendingLeasesForResourceAcquisition(
    int *num_pending_actor_creation, int *num_pending_leases) const {
  const RayLease *exemplar = nullptr;
  // We are guaranteed that these leases are blocked waiting for resources after a
  // call to ScheduleAndGrantLeases(). They may be waiting for workers as well, but
  // this should be a transient condition only.
  for (const auto &shapes_it : leases_to_grant_) {
    auto &work_queue = shapes_it.second;
    for (const auto &work_it : work_queue) {
      const auto &work = *work_it;
      const auto &lease = work_it->lease_;

      // If the work is not in the waiting state, it will be scheduled soon or won't be
      // scheduled. Consider as non-pending.
      if (work.GetState() != internal::WorkStatus::WAITING) {
        continue;
      }

      // If the work is not waiting for acquiring resources, we don't consider it as
      // there's resource deadlock.
      if (work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION &&
          work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE &&
          work.GetUnscheduledCause() !=
              internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY) {
        continue;
      }

      if (lease.GetLeaseSpecification().IsActorCreationTask()) {
        *num_pending_actor_creation += 1;
      } else {
        *num_pending_leases += 1;
      }

      if (exemplar == nullptr) {
        exemplar = &lease;
      }
    }
  }
  return exemplar;
}

void LocalLeaseManager::Grant(
    std::shared_ptr<WorkerInterface> worker,
    absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers,
    const std::shared_ptr<TaskResourceInstances> &allocated_instances,
    const RayLease &lease,
    const std::vector<internal::ReplyCallback> &reply_callbacks) {
  const auto &lease_spec = lease.GetLeaseSpecification();

  if (lease_spec.IsActorCreationTask()) {
    // The actor belongs to this worker now.
    worker->SetLifetimeAllocatedInstances(allocated_instances);
  } else {
    worker->SetAllocatedInstances(allocated_instances);
  }
  worker->GrantLease(lease);

  // Pass the contact info of the worker to use.
  for (const auto &reply_callback : reply_callbacks) {
    reply_callback.reply_->set_worker_pid(worker->GetProcess().GetId());
    reply_callback.reply_->mutable_worker_address()->set_ip_address(worker->IpAddress());
    reply_callback.reply_->mutable_worker_address()->set_port(worker->Port());
    reply_callback.reply_->mutable_worker_address()->set_worker_id(
        worker->WorkerId().Binary());
    reply_callback.reply_->mutable_worker_address()->set_node_id(self_node_id_.Binary());
  }

  RAY_CHECK(!leased_workers.contains(lease_spec.LeaseId()));
  leased_workers[lease_spec.LeaseId()] = worker;
  cluster_resource_scheduler_.GetLocalResourceManager().SetBusyFootprint(
      WorkFootprint::NODE_WORKERS);

  // Update our internal view of the cluster state.
  std::shared_ptr<TaskResourceInstances> allocated_resources;
  if (lease_spec.IsActorCreationTask()) {
    allocated_resources = worker->GetLifetimeAllocatedInstances();
  } else {
    allocated_resources = worker->GetAllocatedInstances();
  }
  for (auto &resource_id : allocated_resources->ResourceIds()) {
    auto instances = allocated_resources->Get(resource_id);
    for (const auto &reply_callback : reply_callbacks) {
      ::ray::rpc::ResourceMapEntry *resource = nullptr;
      for (size_t inst_idx = 0; inst_idx < instances.size(); inst_idx++) {
        if (instances[inst_idx] > 0.) {
          // Set resource name only if at least one of its instances has available
          // capacity.
          if (resource == nullptr) {
            resource = reply_callback.reply_->add_resource_mapping();
            resource->set_name(resource_id.Binary());
          }
          auto rid = resource->add_resource_ids();
          rid->set_index(inst_idx);
          rid->set_quantity(instances[inst_idx].Double());
        }
      }
    }
  }
  // Send the result back to the clients.
  for (const auto &reply_callback : reply_callbacks) {
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}

void LocalLeaseManager::ClearWorkerBacklog(const WorkerID &worker_id) {
  for (auto it = backlog_tracker_.begin(); it != backlog_tracker_.end();) {
    it->second.erase(worker_id);
    if (it->second.empty()) {
      backlog_tracker_.erase(it++);
    } else {
      ++it;
    }
  }
}

void LocalLeaseManager::SetWorkerBacklog(SchedulingClass scheduling_class,
                                         const WorkerID &worker_id,
                                         int64_t backlog_size) {
  if (backlog_size == 0) {
    backlog_tracker_[scheduling_class].erase(worker_id);
    if (backlog_tracker_[scheduling_class].empty()) {
      backlog_tracker_.erase(scheduling_class);
    }
  } else {
    backlog_tracker_[scheduling_class][worker_id] = backlog_size;
  }
}

void LocalLeaseManager::ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) {
  RAY_CHECK(worker != nullptr);
  auto allocated_instances = worker->GetAllocatedInstances()
                                 ? worker->GetAllocatedInstances()
                                 : worker->GetLifetimeAllocatedInstances();
  if (allocated_instances == nullptr) {
    return;
  }

  if (worker->IsBlocked()) {
    // If the worker is blocked, its CPU instances have already been released. We clear
    // the CPU instances to avoid double freeing.

    // For PG, there may be two cpu resources: wildcard and indexed.
    std::vector<ResourceID> cpu_resource_ids;
    for (const auto &resource_id : allocated_instances->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        cpu_resource_ids.emplace_back(resource_id);
      }
    }

    for (const auto &cpu_resource_id : cpu_resource_ids) {
      allocated_instances->Remove(cpu_resource_id);
    }
  }

  cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
      allocated_instances);
  worker->ClearAllocatedInstances();
  worker->ClearLifetimeAllocatedInstances();
}

bool LocalLeaseManager::ReleaseCpuResourcesFromBlockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || worker->IsBlocked()) {
    return false;
  }

  bool cpu_resources_released = false;
  if (worker->GetAllocatedInstances() != nullptr) {
    for (const auto &resource_id : worker->GetAllocatedInstances()->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        auto cpu_instances = worker->GetAllocatedInstances()->GetDouble(resource_id);
        cluster_resource_scheduler_.GetLocalResourceManager().AddResourceInstances(
            resource_id, cpu_instances);
        cpu_resources_released = true;

        // Cannot break since we need to release
        // both PG wildcard and indexed CPU resources.
      }
    }
  }

  if (cpu_resources_released) {
    worker->MarkBlocked();
    return true;
  } else {
    return false;
  }
}

bool LocalLeaseManager::ReturnCpuResourcesToUnblockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || !worker->IsBlocked()) {
    return false;
  }

  bool cpu_resources_returned = false;
  if (worker->GetAllocatedInstances() != nullptr) {
    for (const auto &resource_id : worker->GetAllocatedInstances()->ResourceIds()) {
      if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
        auto cpu_instances = worker->GetAllocatedInstances()->GetDouble(resource_id);
        // Important: we allow going negative here, since otherwise you can use infinite
        // CPU resources by repeatedly blocking / unblocking a task. By allowing it to go
        // negative, at most one task can "borrow" this worker's resources.
        cluster_resource_scheduler_.GetLocalResourceManager().SubtractResourceInstances(
            resource_id, cpu_instances, /*allow_going_negative=*/true);
        cpu_resources_returned = true;

        // Cannot break since we need to return
        // both PG wildcard and indexed CPU resources.
      }
    }
  }

  if (cpu_resources_returned) {
    worker->MarkUnblocked();
    return true;
  } else {
    return false;
  }
}

ResourceSet LocalLeaseManager::CalcNormalTaskResources() const {
  ResourceSet total_normal_task_resources;
  for (auto &entry : leased_workers_) {
    std::shared_ptr<WorkerInterface> worker = entry.second;
    auto &lease_spec = worker->GetGrantedLease().GetLeaseSpecification();
    if (!lease_spec.PlacementGroupBundleId().first.IsNil()) {
      continue;
    }

    auto actor_id = worker->GetActorId();
    if (!actor_id.IsNil() && lease_spec.IsActorCreationTask()) {
      // This task ID corresponds to an actor creation task.
      continue;
    }

    if (auto allocated_instances = worker->GetAllocatedInstances()) {
      auto resource_set = allocated_instances->ToResourceSet();
      // Blocked normal task workers have temporarily released its allocated CPU.
      if (worker->IsBlocked()) {
        for (const auto &resource_id : allocated_instances->ResourceIds()) {
          if (IsCPUOrPlacementGroupCPUResource(resource_id)) {
            resource_set.Set(resource_id, 0);
          }
        }
      }
      total_normal_task_resources += resource_set;
    }
  }
  return total_normal_task_resources;
}

uint64_t LocalLeaseManager::MaxGrantedLeasesPerSchedulingClass(
    SchedulingClass sched_cls_id) const {
  auto sched_cls = SchedulingClassToIds::GetSchedulingClassDescriptor(sched_cls_id);
  double cpu_req = sched_cls.resource_set.Get(ResourceID::CPU()).Double();
  uint64_t total_cpus =
      cluster_resource_scheduler_.GetLocalResourceManager().GetNumCpus();

  if (cpu_req == 0 || total_cpus == 0) {
    return std::numeric_limits<uint64_t>::max();
  }
  return static_cast<uint64_t>(std::round(total_cpus / cpu_req));
}

void LocalLeaseManager::RecordMetrics() const {
  ray::stats::STATS_scheduler_tasks.Record(granted_lease_args_.size(), "Executing");
  ray::stats::STATS_scheduler_tasks.Record(waiting_leases_index_.size(), "Waiting");
}

void LocalLeaseManager::DebugStr(std::stringstream &buffer) const {
  buffer << "Waiting leases size: " << waiting_leases_index_.size() << "\n";
  buffer << "Number of granted lease arguments: " << granted_lease_args_.size() << "\n";
  buffer << "Number of pinned lease arguments: " << pinned_lease_arguments_.size()
         << "\n";
  buffer << "Number of total spilled leases: " << num_lease_spilled_ << "\n";
  buffer << "Number of spilled waiting leases: " << num_waiting_lease_spilled_ << "\n";
  buffer << "Number of spilled unschedulable leases: " << num_unschedulable_lease_spilled_
         << "\n";
  buffer << "Resource usage {\n";

  // Calculates how much resources are occupied by leases.
  // Only iterate up to this number to avoid excessive CPU usage.
  auto max_iteration = RayConfig::instance().worker_max_resource_analysis_iteration();
  uint32_t iteration = 0;
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers(
           /*filter_dead_workers*/ true)) {
    if (max_iteration < iteration++) {
      break;
    }
    if (worker->IsDead()        // worker is dead
        || worker->IsBlocked()  // worker is blocked by blocking Ray API
        || (worker->GetGrantedLeaseId().IsNil() &&
            worker->GetActorId().IsNil())) {  // Lease not assigned
      // TODO(#55923) probably don't need to above check for ActorId since LeaseId is not
      // reset for actors either
      // Then this shouldn't have allocated resources.
      continue;
    }

    const auto &task_or_actor_name = worker->GetGrantedLease()
                                         .GetLeaseSpecification()
                                         .FunctionDescriptor()
                                         ->CallString();
    buffer << "    - (language="
           << rpc::Language_descriptor()->FindValueByNumber(worker->GetLanguage())->name()
           << " "
           << "actor_or_task" << task_or_actor_name << " "
           << "pid=" << worker->GetProcess().GetId() << " "
           << "worker_id=" << worker->WorkerId() << "): "
           << worker->GetGrantedLease()
                  .GetLeaseSpecification()
                  .GetRequiredResources()
                  .DebugString()
           << "\n";
  }
  buffer << "}\n";
  buffer << "Backlog Size per scheduling descriptor :{workerId: num backlogs}:\n";
  for (const auto &[sched_cls, worker_to_backlog_size] : backlog_tracker_) {
    const auto &descriptor =
        SchedulingClassToIds::GetSchedulingClassDescriptor(sched_cls);
    buffer << "\t" << descriptor.ResourceSetStr() << ": {\n";
    for (const auto &[worker_id, backlog_size] : worker_to_backlog_size) {
      buffer << "\t\t" << worker_id << ": " << backlog_size << "\n";
    }
    buffer << "\t}\n";
  }
  buffer << "\n";
  buffer << "Granted leases by scheduling class:\n";

  for (const auto &pair : info_by_sched_cls_) {
    const auto &sched_cls = pair.first;
    const auto &info = pair.second;
    const auto &descriptor =
        SchedulingClassToIds::GetSchedulingClassDescriptor(sched_cls);
    buffer << "    - " << descriptor.DebugString() << ": " << info.granted_leases.size()
           << "/" << info.capacity << "\n";
  }
}

bool LocalLeaseManager::IsLeaseQueued(const SchedulingClass &scheduling_class,
                                      const LeaseID &lease_id) const {
  if (waiting_leases_index_.contains(lease_id)) {
    return true;
  }
  auto leases_to_grant_it = leases_to_grant_.find(scheduling_class);
  if (leases_to_grant_it != leases_to_grant_.end()) {
    for (const auto &work : leases_to_grant_it->second) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        return true;
      }
    }
  }
  return false;
}

bool LocalLeaseManager::AddReplyCallback(const SchedulingClass &scheduling_class,
                                         const LeaseID &lease_id,
                                         rpc::SendReplyCallback send_reply_callback,
                                         rpc::RequestWorkerLeaseReply *reply) {
  if (leases_to_grant_.contains(scheduling_class)) {
    for (const auto &work : leases_to_grant_[scheduling_class]) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        work->reply_callbacks_.emplace_back(std::move(send_reply_callback), reply);
        return true;
      }
    }
  }
  auto it = waiting_leases_index_.find(lease_id);
  if (it != waiting_leases_index_.end()) {
    (*it->second)->reply_callbacks_.emplace_back(std::move(send_reply_callback), reply);
    return true;
  }
  return false;
}

}  // namespace raylet
}  // namespace ray
