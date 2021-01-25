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

#include "ray/raylet/node_manager.h"

namespace ray {
namespace raylet {

void NodeManager::ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) {
  auto const &task_resources = worker->GetTaskResourceIds();
  local_available_resources_.ReleaseConstrained(
      task_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
  cluster_resource_map_[self_node_id_].Release(task_resources.ToResourceSet());
  worker->ResetTaskResourceIds();

  auto const &lifetime_resources = worker->GetLifetimeResourceIds();
  local_available_resources_.ReleaseConstrained(
      lifetime_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
  cluster_resource_map_[self_node_id_].Release(lifetime_resources.ToResourceSet());
  worker->ResetLifetimeResourceIds();
}

bool NodeManager::ReleaseCpuResourcesFromUnblockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || worker->IsBlocked()) {
    return false;
  }

  auto const cpu_resource_ids = worker->ReleaseTaskCpuResources();
  local_available_resources_.Release(cpu_resource_ids);
  cluster_resource_map_[self_node_id_].Release(cpu_resource_ids.ToResourceSet());
  return true;
}

bool NodeManager::ReturnCpuResourcesToBlockedWorker(
    std::shared_ptr<WorkerInterface> worker) {
  if (!worker || !worker->IsBlocked()) {
    return false;
  }

  const TaskID &task_id = worker->GetAssignedTaskId();
  const Task &task = local_queues_.GetTaskOfState(task_id, TaskState::RUNNING);
  const auto &required_resources = task.GetTaskSpecification().GetRequiredResources();
  const ResourceSet cpu_resources = required_resources.GetNumCpus();
  bool oversubscribed = !local_available_resources_.Contains(cpu_resources);
  if (!oversubscribed) {
    // Reacquire the CPU resources for the worker. Note that care needs to be
    // taken if the user is using the specific CPU IDs since the IDs that we
    // reacquire here may be different from the ones that the task started with.
    auto const resource_ids = local_available_resources_.Acquire(cpu_resources);
    worker->AcquireTaskCpuResources(resource_ids);
    cluster_resource_map_[self_node_id_].Acquire(cpu_resources);
  } else {
    // In this case, we simply don't reacquire the CPU resources for the worker.
    // The worker can keep running and when the task finishes, it will simply
    // not have any CPU resources to release.
    RAY_LOG(WARNING)
        << "Resources oversubscribed: "
        << cluster_resource_map_[self_node_id_].GetAvailableResources().ToString();
  }
  return true;
}

void NodeManager::ScheduleAndDispatchTasks() {
  DispatchTasks(local_queues_.GetReadyTasksByClass());
}

void NodeManager::TasksUnblocked(const std::vector<TaskID> &ready_ids) {
  if (ready_ids.empty()) {
    return;
  }

  std::unordered_set<TaskID> ready_task_id_set(ready_ids.begin(), ready_ids.end());

  // First filter out the tasks that should not be moved to READY.
  local_queues_.FilterState(ready_task_id_set, TaskState::BLOCKED);
  local_queues_.FilterState(ready_task_id_set, TaskState::RUNNING);
  local_queues_.FilterState(ready_task_id_set, TaskState::DRIVER);

  // Make sure that the remaining tasks are all WAITING or direct call
  // actors.
  auto ready_task_id_set_copy = ready_task_id_set;
  local_queues_.FilterState(ready_task_id_set_copy, TaskState::WAITING);
  // Filter out direct call actors. These are not tracked by the raylet and
  // their assigned task ID is the actor ID.
  for (const auto &id : ready_task_id_set_copy) {
    ready_task_id_set.erase(id);
  }

  // Queue and dispatch the tasks that are ready to run (i.e., WAITING).
  auto ready_tasks = local_queues_.RemoveTasks(ready_task_id_set);
  local_queues_.QueueTasks(ready_tasks, TaskState::READY);
  DispatchTasks(MakeTasksByClass(ready_tasks));
}

void NodeManager::FillResourceUsage(std::shared_ptr<rpc::ResourcesData> resources_data) {
  SchedulingResources &local_resources = cluster_resource_map_[self_node_id_];
  local_resources.SetLoadResources(local_queues_.GetTotalResourceLoad());
  auto last_heartbeat_resources = gcs_client_->NodeResources().GetLastResourceUsage();
  if (!last_heartbeat_resources->GetLoadResources().IsEqual(
          local_resources.GetLoadResources())) {
    resources_data->set_resource_load_changed(true);
    for (const auto &resource_pair :
         local_resources.GetLoadResources().GetResourceMap()) {
      (*resources_data->mutable_resource_load())[resource_pair.first] =
          resource_pair.second;
    }
    last_heartbeat_resources->SetLoadResources(
        ResourceSet(local_resources.GetLoadResources()));
  }

  // Add resource load by shape. This will be used by the new autoscaler.
  auto resource_load = local_queues_.GetResourceLoadByShape(
      RayConfig::instance().max_resource_shapes_per_load_report());
  resources_data->mutable_resource_load_by_shape()->Swap(&resource_load);
}

void NodeManager::TaskFinished(std::shared_ptr<WorkerInterface> worker, Task *task) {
  RAY_CHECK(worker != nullptr && task != nullptr);
  const auto &task_id = worker->GetAssignedTaskId();
  // (See design_docs/task_states.rst for the state transition diagram.)
  RAY_CHECK(local_queues_.RemoveTask(task_id, task)) << task_id;

  // Release task's resources. The worker's lifetime resources are still held.
  auto const &task_resources = worker->GetTaskResourceIds();
  local_available_resources_.ReleaseConstrained(
      task_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
  cluster_resource_map_[self_node_id_].Release(task_resources.ToResourceSet());
  worker->ResetTaskResourceIds();
}

void NodeManager::ReturnWorkerResources(std::shared_ptr<WorkerInterface> worker) {
  // Do nothing.
}

bool NodeManager::CancelTask(const TaskID &task_id) {
  Task removed_task;
  TaskState removed_task_state;
  bool canceled = local_queues_.RemoveTask(task_id, &removed_task, &removed_task_state);
  if (!canceled) {
    // We do not have the task. This could be because we haven't received the
    // lease request yet, or because we already granted the lease request and
    // it has already been returned.
  } else {
    if (removed_task.OnDispatch()) {
      // We have not yet granted the worker lease. Cancel it now.
      removed_task.OnCancellation()();
      if (removed_task_state == TaskState::WAITING) {
        dependency_manager_.RemoveTaskDependencies(task_id);
      }
    } else {
      // We already granted the worker lease and sent the reply. Re-queue the
      // task and wait for the requester to return the leased worker.
      local_queues_.QueueTasks({removed_task}, removed_task_state);
    }
  }
  return canceled;
}

void NodeManager::QueueAndScheduleTask(const Task &task,
                                       rpc::RequestWorkerLeaseReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  // Override the task dispatch to call back to the client instead of executing the
  // task directly on the worker.
  TaskID task_id = task.GetTaskSpecification().TaskId();
  rpc::Address owner_address = task.GetTaskSpecification().CallerAddress();
  Task &mutable_task = const_cast<Task &>(task);
  mutable_task.OnDispatchInstead(
      [this, owner_address, reply, send_reply_callback](
          const std::shared_ptr<void> granted, const std::string &address, int port,
          const WorkerID &worker_id, const ResourceIdSet &resource_ids) {
        auto worker = std::static_pointer_cast<Worker>(granted);
        uint32_t worker_pid = static_cast<uint32_t>(worker->GetProcess().GetId());

        reply->mutable_worker_address()->set_ip_address(address);
        reply->mutable_worker_address()->set_port(port);
        reply->mutable_worker_address()->set_worker_id(worker_id.Binary());
        reply->mutable_worker_address()->set_raylet_id(self_node_id_.Binary());
        reply->set_worker_pid(worker_pid);
        for (const auto &mapping : resource_ids.AvailableResources()) {
          auto resource = reply->add_resource_mapping();
          resource->set_name(mapping.first);
          for (const auto &id : mapping.second.WholeIds()) {
            auto rid = resource->add_resource_ids();
            rid->set_index(id);
            rid->set_quantity(1.0);
          }
          for (const auto &id : mapping.second.FractionalIds()) {
            auto rid = resource->add_resource_ids();
            rid->set_index(id.first);
            rid->set_quantity(id.second.ToDouble());
          }
        }

        auto reply_failure_handler = [this, worker_id]() {
          RAY_LOG(WARNING)
              << "Failed to reply to GCS server, because it might have restarted. GCS "
                 "cannot obtain the information of the leased worker, so we need to "
                 "release the leased worker to avoid leakage.";
          leased_workers_.erase(worker_id);
          metrics_num_task_executed_ -= 1;
        };
        metrics_num_task_executed_ += 1;
        send_reply_callback(Status::OK(), nullptr, reply_failure_handler);
        RAY_CHECK(leased_workers_.find(worker_id) == leased_workers_.end())
            << "Worker is already leased out " << worker_id;

        leased_workers_[worker_id] = worker;
      });
  mutable_task.OnSpillbackInstead(
      [this, reply, task_id, send_reply_callback](const NodeID &spillback_to,
                                                  const std::string &address, int port) {
        RAY_LOG(DEBUG) << "Worker lease request SPILLBACK " << task_id;
        reply->mutable_retry_at_raylet_address()->set_ip_address(address);
        reply->mutable_retry_at_raylet_address()->set_port(port);
        reply->mutable_retry_at_raylet_address()->set_raylet_id(spillback_to.Binary());
        metrics_num_task_spilled_back_ += 1;
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
  mutable_task.OnCancellationInstead([reply, task_id, send_reply_callback]() {
    RAY_LOG(DEBUG) << "Task lease request canceled " << task_id;
    reply->set_canceled(true);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  });
  SubmitTask(task);
}

void NodeManager::ScheduleInfeasibleTasks() { TryLocalInfeasibleTaskScheduling(); }

/// Return if any tasks are pending resource acquisition.
///
/// \param[in] exemplar An example task that is deadlocking.
/// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
/// \param[in] num_pending_tasks Number of pending tasks.
/// \param[in] any_pending True if there's any pending exemplar.
/// \return True if any progress is any tasks are pending.
bool NodeManager::AnyPendingTasks(Task *exemplar, bool *any_pending,
                                  int *num_pending_actor_creation,
                                  int *num_pending_tasks) const {
  // See if any tasks are blocked trying to acquire resources.
  for (const auto &task : local_queues_.GetTasks(TaskState::READY)) {
    const TaskSpecification &spec = task.GetTaskSpecification();
    if (spec.IsActorCreationTask()) {
      *num_pending_actor_creation += 1;
    } else {
      *num_pending_tasks += 1;
    }
    if (!any_pending) {
      *exemplar = task;
      *any_pending = true;
    }
  }
  return *any_pending;
}

void NodeManager::OnNodeResourceUsageUpdated(const NodeID &node_id,
                                             const rpc::ResourcesData &resource_data) {
  auto it = cluster_resource_map_.find(node_id);
  if (it == cluster_resource_map_.end()) {
    return;
  }

  // Extract decision for this raylet.
  auto decision =
      scheduling_policy_.SpillOver(it->second, cluster_resource_map_[self_node_id_]);
  std::unordered_set<TaskID> local_task_ids;
  for (const auto &task_id : decision) {
    // (See design_docs/task_states.rst for the state transition diagram.)
    Task task;
    TaskState state;
    if (!local_queues_.RemoveTask(task_id, &task, &state)) {
      return;
    }
    // Since we are spilling back from the ready and waiting queues, we need
    // to unsubscribe the dependencies.
    if (state != TaskState::INFEASIBLE) {
      // Don't unsubscribe for infeasible tasks because we never subscribed in
      // the first place.
      dependency_manager_.RemoveTaskDependencies(task_id);
    }
    // Attempt to forward the task. If this fails to forward the task,
    // the task will be resubmit locally.
    ForwardTaskOrResubmit(task, node_id);
  }
}

void NodeManager::FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const {
  // TODO(Shanly): Implement.
}

void NodeManager::OnObjectMissing(const ObjectID &object_id,
                                  const std::vector<TaskID> &waiting_task_ids) {
  RAY_UNUSED(object_id);
  // Transition any tasks that were in the runnable state and are dependent on
  // this object to the waiting state.
  if (!waiting_task_ids.empty()) {
    std::unordered_set<TaskID> waiting_task_id_set(waiting_task_ids.begin(),
                                                   waiting_task_ids.end());

    // NOTE(zhijunfu): For direct actors, the worker is initially assigned actor
    // creation task ID, which will not be reset after the task finishes. And later
    // tasks of this actor will reuse this task ID to require objects from plasma with
    // FetchOrReconstruct, since direct actor task IDs are not known to raylet.
    // To support actor reconstruction for direct actor, raylet marks actor creation
    // task as completed and removes it from `local_queues_` when it receives `TaskDone`
    // message from worker. This is necessary because the actor creation task will be
    // re-submitted during reconstruction, if the task is not removed previously, the
    // new submitted task will be marked as duplicate and thus ignored. So here we check
    // for direct actor creation task explicitly to allow this case.
    auto iter = waiting_task_id_set.begin();
    while (iter != waiting_task_id_set.end()) {
      if (IsActorCreationTask(*iter)) {
        RAY_LOG(DEBUG) << "Ignoring direct actor creation task " << *iter
                       << " when handling object missing for " << object_id;
        iter = waiting_task_id_set.erase(iter);
      } else {
        ++iter;
      }
    }

    // First filter out any tasks that can't be transitioned to READY. These
    // are running workers or drivers, now blocked in a get.
    local_queues_.FilterState(waiting_task_id_set, TaskState::RUNNING);
    local_queues_.FilterState(waiting_task_id_set, TaskState::DRIVER);
    // Transition the tasks back to the waiting state. They will be made
    // runnable once the deleted object becomes available again.
    local_queues_.MoveTasks(waiting_task_id_set, TaskState::READY, TaskState::WAITING);
    RAY_CHECK(waiting_task_id_set.empty());
    // Moving ready tasks to waiting may have changed the load, making space for placing
    // new tasks locally.
    ScheduleTasks(cluster_resource_map_);
  }
}

std::string NodeManager::DebugStr() const {
  // As the NodeManager inherites from ClusterTaskManager and the
  // `cluster_task_manager_->DebugString()` is invoked inside
  // `NodeManager::DebugString()`, which will leads to infinite loop and cause stack
  // overflow, so we should rename `ClusterTaskManager::DebugString` to
  // `ClusterTaskManager::DebugStr` to avoid this.
  return "";
}

}  // namespace raylet

}  // namespace ray
