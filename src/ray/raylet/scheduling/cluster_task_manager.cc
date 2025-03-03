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

#include "ray/raylet/scheduling/cluster_task_manager.h"

#include <google/protobuf/map.h>

#include <boost/range/join.hpp>
#include <deque>
#include <memory>
#include <string>
#include <utility>

#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"

namespace ray {
namespace raylet {

ClusterTaskManager::ClusterTaskManager(
    const NodeID &self_node_id,
    ClusterResourceScheduler &cluster_resource_scheduler,
    internal::NodeInfoGetter get_node_info,
    std::function<void(const RayTask &)> announce_infeasible_task,
    ILocalTaskManager &local_task_manager,
    std::function<int64_t(void)> get_time_ms)
    : self_node_id_(self_node_id),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      get_node_info_(get_node_info),
      announce_infeasible_task_(announce_infeasible_task),
      local_task_manager_(local_task_manager),
      scheduler_resource_reporter_(
          tasks_to_schedule_, infeasible_tasks_, local_task_manager_),
      internal_stats_(*this, local_task_manager_),
      get_time_ms_(get_time_ms) {}

void ClusterTaskManager::QueueAndScheduleTask(
    RayTask task,
    bool grant_or_reject,
    bool is_selected_based_on_locality,
    rpc::RequestWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Queuing and scheduling task "
                 << task.GetTaskSpecification().TaskId();
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  auto work = std::make_shared<internal::Work>(
      std::move(task),
      grant_or_reject,
      is_selected_based_on_locality,
      reply,
      [send_reply_callback = std::move(send_reply_callback)] {
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
  // If the scheduling class is infeasible, just add the work to the infeasible queue
  // directly.
  auto infeasible_tasks_iter = infeasible_tasks_.find(scheduling_class);
  if (infeasible_tasks_iter != infeasible_tasks_.end()) {
    infeasible_tasks_iter->second.emplace_back(std::move(work));
  } else {
    tasks_to_schedule_[scheduling_class].emplace_back(std::move(work));
  }
  ScheduleAndDispatchTasks();
}

namespace {
void ReplyCancelled(const internal::Work &work,
                    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
                    const std::string &scheduling_failure_message) {
  auto reply = work.reply;
  auto callback = work.callback;
  reply->set_canceled(true);
  reply->set_failure_type(failure_type);
  reply->set_scheduling_failure_message(scheduling_failure_message);
  callback();
}
}  // namespace

bool ClusterTaskManager::CancelTasks(
    std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  bool tasks_cancelled = false;

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      tasks_to_schedule_, [&](const std::shared_ptr<internal::Work> &work) {
        if (predicate(work)) {
          RAY_LOG(DEBUG) << "Canceling task "
                         << work->task.GetTaskSpecification().TaskId()
                         << " from schedule queue.";
          ReplyCancelled(*work, failure_type, scheduling_failure_message);
          tasks_cancelled = true;
          return true;
        } else {
          return false;
        }
      });

  ray::erase_if<SchedulingClass, std::shared_ptr<internal::Work>>(
      infeasible_tasks_, [&](const std::shared_ptr<internal::Work> &work) {
        if (predicate(work)) {
          RAY_LOG(DEBUG) << "Canceling task "
                         << work->task.GetTaskSpecification().TaskId()
                         << " from infeasible queue.";
          ReplyCancelled(*work, failure_type, scheduling_failure_message);
          tasks_cancelled = true;
          return true;
        } else {
          return false;
        }
      });

  if (local_task_manager_.CancelTasks(
          predicate, failure_type, scheduling_failure_message)) {
    tasks_cancelled = true;
  }

  return tasks_cancelled;
}

bool ClusterTaskManager::CancelTasksWithResourceShapes(
    const std::vector<ResourceSet> target_resource_shapes) {
  auto predicate = [target_resource_shapes,
                    this](const std::shared_ptr<internal::Work> &work) {
    return this->IsWorkWithResourceShape(work, target_resource_shapes);
  };

  const std::string resource_shapes_str =
      ray::VectorToString(target_resource_shapes, &ResourceSet::DebugString);
  RAY_LOG(WARNING) << "Cancelling infeasible tasks with resource shapes "
                   << resource_shapes_str;

  bool task_cancelled = CancelTasks(
      predicate,
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
      absl::StrCat(
          "Tasks or actors with resource shapes ",
          resource_shapes_str,
          " failed to schedule because there are not enough resources for the tasks "
          "or actors on the whole cluster."));

  RAY_LOG(INFO) << "Infeasible tasks cancellation complete with result=" << task_cancelled
                << ",resource shapes=" << resource_shapes_str;

  return task_cancelled;
}

bool ClusterTaskManager::IsWorkWithResourceShape(
    const std::shared_ptr<internal::Work> &work,
    const std::vector<ResourceSet> &target_resource_shapes) {
  SchedulingClass scheduling_class =
      work->task.GetTaskSpecification().GetSchedulingClass();
  ResourceSet resource_set =
      TaskSpecification::GetSchedulingClassDescriptor(scheduling_class).resource_set;
  for (const auto &target_resource_shape : target_resource_shapes) {
    if (resource_set == target_resource_shape) {
      return true;
    }
  }
  return false;
}

bool ClusterTaskManager::CancelAllTaskOwnedBy(
    const WorkerID &worker_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  // Only tasks and regular actors are canceled because their lifetime is
  // the same as the owner.
  auto predicate = [worker_id](const std::shared_ptr<internal::Work> &work) {
    return !work->task.GetTaskSpecification().IsDetachedActor() &&
           work->task.GetTaskSpecification().CallerWorkerId() == worker_id;
  };

  return CancelTasks(predicate, failure_type, scheduling_failure_message);
}

void ClusterTaskManager::ScheduleAndDispatchTasks() {
  // Always try to schedule infeasible tasks in case they are now feasible.
  TryScheduleInfeasibleTask();
  std::deque<std::shared_ptr<internal::Work>> works_to_cancel;
  for (auto shapes_it = tasks_to_schedule_.begin();
       shapes_it != tasks_to_schedule_.end();) {
    auto &work_queue = shapes_it->second;
    bool is_infeasible = false;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      // Check every task in task_to_schedule queue to see
      // whether it can be scheduled. This avoids head-of-line
      // blocking where a task which cannot be scheduled because
      // there are not enough available resources blocks other
      // tasks from being scheduled.
      const std::shared_ptr<internal::Work> &work = *work_it;
      RayTask task = work->task;
      RAY_LOG(DEBUG) << "Scheduling pending task "
                     << task.GetTaskSpecification().TaskId();
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          task.GetTaskSpecification(),
          /*preferred_node_id*/ work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                            : task.GetPreferredNodeID(),
          /*exclude_local_node*/ false,
          /*requires_object_store_memory*/ false,
          &is_infeasible);

      // There is no node that has available resources to run the request.
      // Move on to the next shape.
      if (scheduling_node_id.IsNil()) {
        RAY_LOG(DEBUG) << "No node found to schedule a task "
                       << task.GetTaskSpecification().TaskId() << " is infeasible?"
                       << is_infeasible;

        if (task.GetTaskSpecification().IsNodeAffinitySchedulingStrategy() &&
            !task.GetTaskSpecification().GetNodeAffinitySchedulingStrategySoft()) {
          // This can only happen if the target node doesn't exist or is infeasible.
          // The task will never be schedulable in either case so we should fail it.
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
            // would synchronously call `ClusterTaskManager::CancelTask`, where
            // `task_to_schedule_`'s iterator will be invalidated. So record this work and
            // it will be handled below (out of the loop).
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
      auto &work_queue = shapes_it->second;
      const auto &work = work_queue[0];
      const RayTask task = work->task;
      if (announce_infeasible_task_) {
        announce_infeasible_task_(task);
      }

      // TODO(sang): Use a shared pointer deque to reduce copy overhead.
      infeasible_tasks_[shapes_it->first] = shapes_it->second;
      tasks_to_schedule_.erase(shapes_it++);
    } else if (work_queue.empty()) {
      tasks_to_schedule_.erase(shapes_it++);
    } else {
      shapes_it++;
    }
  }

  for (const auto &work : works_to_cancel) {
    // All works in `works_to_cancel` are scheduled by gcs. So `ReplyCancelled`
    // will synchronously call `ClusterTaskManager::CancelTask`, where works are
    // erased from the pending queue.
    ReplyCancelled(*work,
                   rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE,
                   "The node specified via NodeAffinitySchedulingStrategy doesn't exist "
                   "any more or is infeasible, and soft=False was specified.");
  }
  works_to_cancel.clear();

  local_task_manager_.ScheduleAndDispatchTasks();
}

void ClusterTaskManager::TryScheduleInfeasibleTask() {
  for (auto shapes_it = infeasible_tasks_.begin();
       shapes_it != infeasible_tasks_.end();) {
    auto &work_queue = shapes_it->second;
    RAY_CHECK(!work_queue.empty())
        << "Empty work queue shouldn't have been added as a infeasible shape.";
    // We only need to check the first item because every task has the same shape.
    // If the first entry is infeasible, that means everything else is the same.
    const auto work = work_queue[0];
    RayTask task = work->task;
    RAY_LOG(DEBUG) << "Check if the infeasible task is schedulable in any node. task_id:"
                   << task.GetTaskSpecification().TaskId();
    bool is_infeasible;
    cluster_resource_scheduler_.GetBestSchedulableNode(
        task.GetTaskSpecification(),
        /*preferred_node_id*/ work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                          : task.GetPreferredNodeID(),
        /*exclude_local_node*/ false,
        /*requires_object_store_memory*/ false,
        &is_infeasible);

    // There is no node that has available resources to run the request.
    // Move on to the next shape.
    if (is_infeasible) {
      RAY_LOG(DEBUG) << "No feasible node found for task "
                     << task.GetTaskSpecification().TaskId();
      shapes_it++;
    } else {
      RAY_LOG(DEBUG) << "Infeasible task of task id "
                     << task.GetTaskSpecification().TaskId()
                     << " is now feasible. Move the entry back to tasks_to_schedule_";
      tasks_to_schedule_[shapes_it->first] = shapes_it->second;
      infeasible_tasks_.erase(shapes_it++);
    }
  }
}

bool ClusterTaskManager::CancelTask(
    const TaskID &task_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  auto predicate = [task_id](const std::shared_ptr<internal::Work> &work) {
    return work->task.GetTaskSpecification().TaskId() == task_id;
  };

  return CancelTasks(predicate, failure_type, scheduling_failure_message);
}

void ClusterTaskManager::FillResourceUsage(rpc::ResourcesData &data) {
  // This populates load information.
  scheduler_resource_reporter_.FillResourceUsage(data);
  // This populates usage information.
  syncer::ResourceViewSyncMessage resource_view_sync_message;
  cluster_resource_scheduler_.GetLocalResourceManager().PopulateResourceViewSyncMessage(
      resource_view_sync_message);
  (*data.mutable_resources_total()) =
      std::move(resource_view_sync_message.resources_total());
  (*data.mutable_resources_available()) =
      std::move(resource_view_sync_message.resources_available());
  data.set_object_pulls_queued(resource_view_sync_message.object_pulls_queued());
  data.set_idle_duration_ms(resource_view_sync_message.idle_duration_ms());
  data.set_is_draining(resource_view_sync_message.is_draining());
  data.set_draining_deadline_timestamp_ms(
      resource_view_sync_message.draining_deadline_timestamp_ms());
}

bool ClusterTaskManager::AnyPendingTasksForResourceAcquisition(
    RayTask *exemplar,
    bool *any_pending,
    int *num_pending_actor_creation,
    int *num_pending_tasks) const {
  // We are guaranteed that these tasks are blocked waiting for resources after a
  // call to ScheduleAndDispatchTasks(). They may be waiting for workers as well, but
  // this should be a transient condition only.
  for (const auto &shapes_it : tasks_to_schedule_) {
    auto &work_queue = shapes_it.second;
    for (const auto &work_it : work_queue) {
      const auto &work = *work_it;
      const auto &task = work_it->task;

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

      if (task.GetTaskSpecification().IsActorCreationTask()) {
        *num_pending_actor_creation += 1;
      } else {
        *num_pending_tasks += 1;
      }

      if (!*any_pending) {
        *exemplar = task;
        *any_pending = true;
      }
    }
  }

  local_task_manager_.AnyPendingTasksForResourceAcquisition(
      exemplar, any_pending, num_pending_actor_creation, num_pending_tasks);

  // If there's any pending task, at this point, there's no progress being made.
  return *any_pending;
}

void ClusterTaskManager::RecordMetrics() const {
  internal_stats_.RecordMetrics();
  cluster_resource_scheduler_.GetLocalResourceManager().RecordMetrics();
}

std::string ClusterTaskManager::DebugStr() const {
  return internal_stats_.ComputeAndReportDebugStr();
}

void ClusterTaskManager::ScheduleOnNode(const NodeID &spillback_to,
                                        const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_task_manager_.QueueAndScheduleTask(work);
    return;
  }

  auto send_reply_callback = work->callback;

  if (work->grant_or_reject) {
    work->reply->set_rejected(true);
    send_reply_callback();
    return;
  }

  internal_stats_.TaskSpilled();

  const auto &task = work->task;
  const auto &task_spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Spilling task " << task_spec.TaskId() << " to node " << spillback_to;

  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          task_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << task_spec.TaskId()
                   << " on a remote node that are no longer available";
  }

  auto node_info_ptr = get_node_info_(spillback_to);
  RAY_CHECK(node_info_ptr)
      << "Spilling back to a node manager, but no GCS info found for node "
      << spillback_to;
  auto reply = work->reply;
  reply->mutable_retry_at_raylet_address()->set_ip_address(
      node_info_ptr->node_manager_address());
  reply->mutable_retry_at_raylet_address()->set_port(node_info_ptr->node_manager_port());
  reply->mutable_retry_at_raylet_address()->set_raylet_id(spillback_to.Binary());

  send_reply_callback();
}

ClusterResourceScheduler &ClusterTaskManager::GetClusterResourceScheduler() const {
  return cluster_resource_scheduler_;
}

size_t ClusterTaskManager::GetInfeasibleQueueSize() const {
  size_t count = 0;
  for (const auto &cls_entry : infeasible_tasks_) {
    count += cls_entry.second.size();
  }
  return count;
}

size_t ClusterTaskManager::GetPendingQueueSize() const {
  size_t count = 0;
  for (const auto &cls_entry : tasks_to_schedule_) {
    count += cls_entry.second.size();
  }
  return count;
}

void ClusterTaskManager::FillPendingActorInfo(rpc::ResourcesData &data) const {
  scheduler_resource_reporter_.FillPendingActorCountByShape(data);
}

}  // namespace raylet
}  // namespace ray
