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

#include "ray/raylet/scheduling/cluster_lease_manager.h"

#include <google/protobuf/map.h>

#include <deque>
#include <memory>
#include <string>
#include <utility>

#include "ray/util/logging.h"
#include "ray/util/string_utils.h"

namespace ray {
namespace raylet {

ClusterLeaseManager::ClusterLeaseManager(
    const NodeID &self_node_id,
    ClusterResourceScheduler &cluster_resource_scheduler,
    internal::NodeInfoGetter get_node_info,
    std::function<void(const RayLease &)> announce_infeasible_lease,
    LocalLeaseManagerInterface &local_lease_manager,
    std::function<int64_t(void)> get_time_ms)
    : self_node_id_(self_node_id),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      get_node_info_(std::move(get_node_info)),
      announce_infeasible_lease_(std::move(announce_infeasible_lease)),
      local_lease_manager_(local_lease_manager),
      scheduler_resource_reporter_(
          leases_to_schedule_, infeasible_leases_, local_lease_manager_),
      internal_stats_(*this, local_lease_manager_),
      get_time_ms_(std::move(get_time_ms)) {}

void ClusterLeaseManager::QueueAndScheduleLease(
    RayLease lease,
    bool grant_or_reject,
    bool is_selected_based_on_locality,
    std::vector<internal::ReplyCallback> reply_callbacks) {
  RAY_LOG(DEBUG) << "Queuing and scheduling lease "
                 << lease.GetLeaseSpecification().LeaseId();
  const auto scheduling_class = lease.GetLeaseSpecification().GetSchedulingClass();
  auto work = std::make_shared<internal::Work>(std::move(lease),
                                               grant_or_reject,
                                               is_selected_based_on_locality,
                                               std::move(reply_callbacks));
  // If the scheduling class is infeasible, just add the work to the infeasible queue
  // directly.
  auto infeasible_leases_iter = infeasible_leases_.find(scheduling_class);
  if (infeasible_leases_iter != infeasible_leases_.end()) {
    infeasible_leases_iter->second.emplace_back(std::move(work));
  } else {
    leases_to_schedule_[scheduling_class].emplace_back(std::move(work));
  }
  ScheduleAndGrantLeases();
}

namespace {
void ReplyCancelled(const internal::Work &work,
                    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
                    const std::string &scheduling_failure_message) {
  for (const auto &reply_callback : work.reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->set_canceled(true);
    reply->set_failure_type(failure_type);
    reply->set_scheduling_failure_message(scheduling_failure_message);
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
}  // namespace

bool ClusterLeaseManager::CancelLeases(
    std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  bool leases_cancelled = false;

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      leases_to_schedule_, [&](const std::shared_ptr<internal::Work> &work) {
        if (predicate(work)) {
          RAY_LOG(DEBUG) << "Canceling lease "
                         << work->lease_.GetLeaseSpecification().LeaseId()
                         << " from schedule queue.";
          ReplyCancelled(*work, failure_type, scheduling_failure_message);
          leases_cancelled = true;
          return true;
        } else {
          return false;
        }
      });

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      infeasible_leases_, [&](const std::shared_ptr<internal::Work> &work) {
        if (predicate(work)) {
          RAY_LOG(DEBUG) << "Canceling lease "
                         << work->lease_.GetLeaseSpecification().LeaseId()
                         << " from infeasible queue.";
          ReplyCancelled(*work, failure_type, scheduling_failure_message);
          leases_cancelled = true;
          return true;
        } else {
          return false;
        }
      });

  if (local_lease_manager_.CancelLeases(
          predicate, failure_type, scheduling_failure_message)) {
    leases_cancelled = true;
  }

  return leases_cancelled;
}

bool ClusterLeaseManager::CancelLeasesWithResourceShapes(
    const std::vector<ResourceSet> target_resource_shapes) {
  auto predicate = [target_resource_shapes,
                    this](const std::shared_ptr<internal::Work> &work) {
    return this->IsWorkWithResourceShape(work, target_resource_shapes);
  };

  const std::string resource_shapes_str =
      ray::VectorToString(target_resource_shapes, &ResourceSet::DebugString);
  RAY_LOG(WARNING) << "Cancelling infeasible tasks with resource shapes "
                   << resource_shapes_str;

  bool lease_cancelled = CancelLeases(
      predicate,
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
      absl::StrCat(
          "Tasks or actors with resource shapes ",
          resource_shapes_str,
          " failed to schedule because there are not enough resources for the tasks "
          "or actors on the whole cluster."));

  RAY_LOG(INFO) << "Infeasible tasks cancellation complete with result="
                << lease_cancelled << ",resource shapes=" << resource_shapes_str;

  return lease_cancelled;
}

bool ClusterLeaseManager::IsWorkWithResourceShape(
    const std::shared_ptr<internal::Work> &work,
    const std::vector<ResourceSet> &target_resource_shapes) {
  SchedulingClass scheduling_class =
      work->lease_.GetLeaseSpecification().GetSchedulingClass();
  ResourceSet resource_set =
      SchedulingClassToIds::GetSchedulingClassDescriptor(scheduling_class).resource_set;
  for (const auto &target_resource_shape : target_resource_shapes) {
    if (resource_set == target_resource_shape) {
      return true;
    }
  }
  return false;
}

bool ClusterLeaseManager::CancelAllLeasesOwnedBy(
    const NodeID &node_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  // Only tasks and regular actors are canceled because their lifetime is
  // the same as the owner.
  auto predicate = [node_id](const std::shared_ptr<internal::Work> &work) {
    return !work->lease_.GetLeaseSpecification().IsDetachedActor() &&
           work->lease_.GetLeaseSpecification().CallerNodeId() == node_id;
  };

  return CancelLeases(predicate, failure_type, scheduling_failure_message);
}

bool ClusterLeaseManager::CancelAllLeasesOwnedBy(
    const WorkerID &worker_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  // Only tasks and regular actors are canceled because their lifetime is
  // the same as the owner.
  auto predicate = [worker_id](const std::shared_ptr<internal::Work> &work) {
    return !work->lease_.GetLeaseSpecification().IsDetachedActor() &&
           work->lease_.GetLeaseSpecification().CallerWorkerId() == worker_id;
  };

  return CancelLeases(predicate, failure_type, scheduling_failure_message);
}

void ClusterLeaseManager::ScheduleAndGrantLeases() {
  // Always try to schedule infeasible tasks in case they are now feasible.
  TryScheduleInfeasibleLease();
  std::deque<std::shared_ptr<internal::Work>> works_to_cancel;
  for (auto shapes_it = leases_to_schedule_.begin();
       shapes_it != leases_to_schedule_.end();) {
    auto &work_queue = shapes_it->second;
    bool is_infeasible = false;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      // Check every lease in lease_to_schedule queue to see
      // whether it can be scheduled. This avoids head-of-line
      // blocking where a lease which cannot be scheduled because
      // there are not enough available resources blocks other
      // leases from being scheduled.
      const std::shared_ptr<internal::Work> &work = *work_it;
      RayLease lease = work->lease_;
      RAY_LOG(DEBUG) << "Scheduling pending lease "
                     << lease.GetLeaseSpecification().LeaseId();
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(),
          /*preferred_node_id*/ work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                            : lease.GetPreferredNodeID(),
          /*exclude_local_node*/ false,
          /*requires_object_store_memory*/ false,
          &is_infeasible);

      // There is no node that has available resources to run the request.
      // Move on to the next shape.
      if (scheduling_node_id.IsNil()) {
        RAY_LOG(DEBUG) << "No node found to schedule a lease "
                       << lease.GetLeaseSpecification().LeaseId() << " is infeasible?"
                       << is_infeasible;

        auto affinity_values =
            GetHardNodeAffinityValues(lease.GetLeaseSpecification().GetLabelSelector());
        if ((lease.GetLeaseSpecification().IsNodeAffinitySchedulingStrategy() &&
             !lease.GetLeaseSpecification().GetNodeAffinitySchedulingStrategySoft()) ||
            (affinity_values.has_value() && !affinity_values->empty())) {
          // This can only happen if the target node doesn't exist or is infeasible.
          // The lease will never be schedulable in either case so we should fail it.
          if (cluster_resource_scheduler_.IsLocalNodeWithRaylet()) {
            ReplyCancelled(
                *work,
                rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
                "The node specified via NodeAffinitySchedulingStrategy doesn't exist "
                "any more or is infeasible, and soft=False was specified.");
            // We don't want to trigger the normal infeasible task logic (i.e. waiting),
            // but rather we want to fail the task immediately.
            work_it = work_queue.erase(work_it);
          } else {
            // If scheduling is done by gcs, we can not `ReplyCancelled` now because it
            // would synchronously call `ClusterLeaseManager::CancelLease`, where
            // `lease_to_schedule_`'s iterator will be invalidated. So record this work
            // and it will be handled below (out of the loop).
            works_to_cancel.push_back(*work_it);
            work_it++;
          }
          is_infeasible = false;
          continue;
        }

        break;
      }

      NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
      ScheduleOnNode(node_id, work);
      work_it = work_queue.erase(work_it);
    }

    if (is_infeasible) {
      RAY_CHECK(!work_queue.empty());
      // Only announce the first item as infeasible.
      auto &cur_work_queue = shapes_it->second;
      const auto &work = cur_work_queue[0];
      const RayLease lease = work->lease_;
      if (announce_infeasible_lease_) {
        announce_infeasible_lease_(lease);
      }

      infeasible_leases_[shapes_it->first] = std::move(shapes_it->second);
      leases_to_schedule_.erase(shapes_it++);
    } else if (work_queue.empty()) {
      leases_to_schedule_.erase(shapes_it++);
    } else {
      shapes_it++;
    }
  }

  for (const auto &work : works_to_cancel) {
    // All works in `works_to_cancel` are scheduled by gcs. So `ReplyCancelled`
    // will synchronously call `ClusterLeaseManager::CancelLease`, where works are
    // erased from the pending queue.
    ReplyCancelled(*work,
                   rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
                   "The node specified via NodeAffinitySchedulingStrategy doesn't exist "
                   "any more or is infeasible, and soft=False was specified.");
  }
  works_to_cancel.clear();

  local_lease_manager_.ScheduleAndGrantLeases();
}

void ClusterLeaseManager::TryScheduleInfeasibleLease() {
  for (auto shapes_it = infeasible_leases_.begin();
       shapes_it != infeasible_leases_.end();) {
    auto &work_queue = shapes_it->second;
    RAY_CHECK(!work_queue.empty())
        << "Empty work queue shouldn't have been added as a infeasible shape.";
    // We only need to check the first item because every task has the same shape.
    // If the first entry is infeasible, that means everything else is the same.
    const auto work = work_queue[0];
    RayLease lease = work->lease_;
    RAY_LOG(DEBUG)
        << "Check if the infeasible lease is schedulable in any node. lease_id:"
        << lease.GetLeaseSpecification().LeaseId();
    bool is_infeasible;
    cluster_resource_scheduler_.GetBestSchedulableNode(
        lease.GetLeaseSpecification(),
        /*preferred_node_id*/ work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                          : lease.GetPreferredNodeID(),
        /*exclude_local_node*/ false,
        /*requires_object_store_memory*/ false,
        &is_infeasible);

    // There is no node that has feasible resources to run the request.
    // Move on to the next shape.
    if (is_infeasible) {
      RAY_LOG(DEBUG) << "No feasible node found for lease "
                     << lease.GetLeaseSpecification().LeaseId();
      shapes_it++;
    } else {
      RAY_LOG(DEBUG) << "Infeasible lease of lease id "
                     << lease.GetLeaseSpecification().LeaseId()
                     << " is now feasible. Move the entry back to leases_to_schedule_";
      leases_to_schedule_[shapes_it->first] = std::move(shapes_it->second);
      infeasible_leases_.erase(shapes_it++);
    }
  }
}

bool ClusterLeaseManager::CancelLease(
    const LeaseID &lease_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  auto predicate = [lease_id](const std::shared_ptr<internal::Work> &work) {
    return work->lease_.GetLeaseSpecification().LeaseId() == lease_id;
  };

  return CancelLeases(predicate, failure_type, scheduling_failure_message);
}

void ClusterLeaseManager::FillResourceUsage(rpc::ResourcesData &data) {
  // This populates load information.
  scheduler_resource_reporter_.FillResourceUsage(data);
  // This populates usage information.
  syncer::ResourceViewSyncMessage resource_view_sync_message;
  cluster_resource_scheduler_.GetLocalResourceManager().PopulateResourceViewSyncMessage(
      resource_view_sync_message);
  (*data.mutable_resources_total()) =
      std::move(*resource_view_sync_message.mutable_resources_total());
  (*data.mutable_resources_available()) =
      std::move(*resource_view_sync_message.mutable_resources_available());
  data.set_object_pulls_queued(resource_view_sync_message.object_pulls_queued());
  data.set_idle_duration_ms(resource_view_sync_message.idle_duration_ms());
  data.set_is_draining(resource_view_sync_message.is_draining());
  data.set_draining_deadline_timestamp_ms(
      resource_view_sync_message.draining_deadline_timestamp_ms());
}

const RayLease *ClusterLeaseManager::AnyPendingLeasesForResourceAcquisition(
    int *num_pending_actor_creation, int *num_pending_leases) const {
  const RayLease *exemplar = nullptr;
  // We are guaranteed that these leases are blocked waiting for resources after a
  // call to ScheduleAndGrantLeases(). They may be waiting for workers as well, but
  // this should be a transient condition only.
  for (const auto &shapes_it : leases_to_schedule_) {
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

  auto local_lease_exemplar = local_lease_manager_.AnyPendingLeasesForResourceAcquisition(
      num_pending_actor_creation, num_pending_leases);
  // Prefer returning the cluster lease manager exemplar if it exists.
  return exemplar == nullptr ? local_lease_exemplar : exemplar;
}

void ClusterLeaseManager::RecordMetrics() const {
  internal_stats_.RecordMetrics();
  cluster_resource_scheduler_.GetLocalResourceManager().RecordMetrics();
}

std::string ClusterLeaseManager::DebugStr() const {
  return internal_stats_.ComputeAndReportDebugStr();
}

void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }

  if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  internal_stats_.LeaseSpilled();

  const auto &lease = work->lease_;
  const auto &lease_spec = lease.GetLeaseSpecification();
  RAY_LOG(DEBUG) << "Spilling lease " << lease_spec.LeaseId() << " to node "
                 << spillback_to;

  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          lease_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << lease_spec.LeaseId()
                   << " on a remote node that are no longer available";
  }

  auto node_info = get_node_info_(spillback_to);
  RAY_CHECK(node_info.has_value());
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port((*node_info).node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}

ClusterResourceScheduler &ClusterLeaseManager::GetClusterResourceScheduler() const {
  return cluster_resource_scheduler_;
}

size_t ClusterLeaseManager::GetInfeasibleQueueSize() const {
  size_t count = 0;
  for (const auto &cls_entry : infeasible_leases_) {
    count += cls_entry.second.size();
  }
  return count;
}

size_t ClusterLeaseManager::GetPendingQueueSize() const {
  size_t count = 0;
  for (const auto &cls_entry : leases_to_schedule_) {
    count += cls_entry.second.size();
  }
  return count;
}

void ClusterLeaseManager::FillPendingActorInfo(rpc::ResourcesData &data) const {
  scheduler_resource_reporter_.FillPendingActorCountByShape(data);
}

bool ClusterLeaseManager::IsLeaseQueued(const SchedulingClass &scheduling_class,
                                        const LeaseID &lease_id) const {
  auto it = leases_to_schedule_.find(scheduling_class);
  if (it != leases_to_schedule_.end()) {
    for (const auto &work : it->second) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        return true;
      }
    }
  }

  auto infeasible_it = infeasible_leases_.find(scheduling_class);
  if (infeasible_it != infeasible_leases_.end()) {
    for (const auto &work : infeasible_it->second) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        return true;
      }
    }
  }

  return false;
}

bool ClusterLeaseManager::AddReplyCallback(const SchedulingClass &scheduling_class,
                                           const LeaseID &lease_id,
                                           rpc::SendReplyCallback send_reply_callback,
                                           rpc::RequestWorkerLeaseReply *reply) {
  if (leases_to_schedule_.contains(scheduling_class)) {
    for (const auto &work : leases_to_schedule_[scheduling_class]) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        work->reply_callbacks_.emplace_back(std::move(send_reply_callback), reply);
        return true;
      }
    }
  }
  if (infeasible_leases_.contains(scheduling_class)) {
    for (const auto &work : infeasible_leases_[scheduling_class]) {
      if (work->lease_.GetLeaseSpecification().LeaseId() == lease_id) {
        work->reply_callbacks_.emplace_back(std::move(send_reply_callback), reply);
        return true;
      }
    }
  }
  return false;
}

}  // namespace raylet
}  // namespace ray
