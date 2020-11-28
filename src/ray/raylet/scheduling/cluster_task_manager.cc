#include "ray/raylet/scheduling/cluster_task_manager.h"

#include <google/protobuf/map.h>

#include "ray/util/logging.h"

namespace ray {
namespace raylet {

ClusterTaskManager::ClusterTaskManager(
    const NodeID &self_node_id,
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
    std::function<bool(const Task &)> fulfills_dependencies_func,
    std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive,
    NodeInfoGetter get_node_info)
    : self_node_id_(self_node_id),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      fulfills_dependencies_func_(fulfills_dependencies_func),
      is_owner_alive_(is_owner_alive),
      get_node_info_(get_node_info) {}

bool ClusterTaskManager::SchedulePendingTasks() {
  bool did_schedule = false;
  for (auto shapes_it = tasks_to_schedule_.begin();
       shapes_it != tasks_to_schedule_.end();) {
    auto &work_queue = shapes_it->second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      // Check every task in task_to_schedule queue to see
      // whether it can be scheduled. This avoids head-of-line
      // blocking where a task which cannot be scheduled because
      // there are not enough available resources blocks other
      // tasks from being scheduled.
      const Work &work = *work_it;
      Task task = std::get<0>(work);
      RAY_LOG(DEBUG) << "Scheduling pending task "
                     << task.GetTaskSpecification().TaskId();
      auto placement_resources =
          task.GetTaskSpecification().GetRequiredPlacementResources().GetResourceMap();
      int64_t _unused;
      // TODO (Alex): We should distinguish between infeasible tasks and a fully
      // utilized cluster.
      std::string node_id_string = cluster_resource_scheduler_->GetBestSchedulableNode(
          placement_resources, task.GetTaskSpecification().IsActorCreationTask(),
          &_unused);
      if (node_id_string.empty()) {
        // There is no node that has available resources to run the request.
        // Move on to the next shape.
        break;
      } else {
        if (node_id_string == self_node_id_.Binary()) {
          // Warning: WaitForTaskArgsRequests must execute (do not let it short
          // circuit if did_schedule is true).
          bool task_scheduled = WaitForTaskArgsRequests(work);
          did_schedule = task_scheduled || did_schedule;
        } else {
          // Should spill over to a different node.
          NodeID node_id = NodeID::FromBinary(node_id_string);
          Spillback(node_id, work);
        }
        work_it = work_queue.erase(work_it);
      }
    }
    if (work_queue.empty()) {
      shapes_it = tasks_to_schedule_.erase(shapes_it);
    } else {
      shapes_it++;
    }
  }
  return did_schedule;
}

bool ClusterTaskManager::WaitForTaskArgsRequests(Work work) {
  const auto &task = std::get<0>(work);
  const auto &scheduling_key = task.GetTaskSpecification().GetSchedulingClass();
  auto object_ids = task.GetTaskSpecification().GetDependencies();
  bool can_dispatch = true;
  if (object_ids.size() > 0) {
    bool args_ready = fulfills_dependencies_func_(task);
    if (args_ready) {
      RAY_LOG(DEBUG) << "Args already ready, task can be dispatched "
                     << task.GetTaskSpecification().TaskId();
      tasks_to_dispatch_[scheduling_key].push_back(work);
    } else {
      can_dispatch = false;
      TaskID task_id = task.GetTaskSpecification().TaskId();
      waiting_tasks_[task_id] = work;
    }
  } else {
    RAY_LOG(DEBUG) << "No args, task can be dispatched "
                   << task.GetTaskSpecification().TaskId();
    tasks_to_dispatch_[scheduling_key].push_back(work);
  }
  return can_dispatch;
}

void ClusterTaskManager::DispatchScheduledTasksToWorkers(
    WorkerPoolInterface &worker_pool,
    std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers) {
  // Check every task in task_to_dispatch queue to see
  // whether it can be dispatched and ran. This avoids head-of-line
  // blocking where a task which cannot be dispatched because
  // there are not enough available resources blocks other
  // tasks from being dispatched.
  for (auto shapes_it = tasks_to_dispatch_.begin();
       shapes_it != tasks_to_dispatch_.end();) {
    auto &dispatch_queue = shapes_it->second;
    for (auto work_it = dispatch_queue.begin(); work_it != dispatch_queue.end();) {
      auto &work = *work_it;
      auto &task = std::get<0>(work);
      auto &spec = task.GetTaskSpecification();

      std::shared_ptr<WorkerInterface> worker = worker_pool.PopWorker(spec);
      if (!worker) {
        // No worker available, we won't be able to schedule any kind of task.
        return;
      }

      const auto owner_worker_id = WorkerID::FromBinary(spec.CallerAddress().worker_id());
      const auto owner_node_id = NodeID::FromBinary(spec.CallerAddress().raylet_id());
      // If the owner has died since this task was queued, cancel the task by
      // killing the worker (unless this task is for a detached actor).
      if (!spec.IsDetachedActor() && !is_owner_alive_(owner_worker_id, owner_node_id)) {
        RAY_LOG(WARNING) << "Task: " << task.GetTaskSpecification().TaskId()
                         << "'s caller is no longer running. Cancelling task.";
        worker_pool.PushWorker(worker);
        work_it = dispatch_queue.erase(work_it);
      } else {
        bool worker_leased;
        bool remove = AttemptDispatchWork(*work_it, worker, &worker_leased);
        if (worker_leased) {
          auto reply = std::get<1>(*work_it);
          auto callback = std::get<2>(*work_it);
          Dispatch(worker, leased_workers, spec, reply, callback);
        } else {
          worker_pool.PushWorker(worker);
        }
        if (remove) {
          work_it = dispatch_queue.erase(work_it);
        } else {
          break;
        }
      }
    }
    if (dispatch_queue.empty()) {
      shapes_it = tasks_to_dispatch_.erase(shapes_it);
    } else {
      shapes_it++;
    }
  }
}

bool ClusterTaskManager::AttemptDispatchWork(const Work &work,
                                             std::shared_ptr<WorkerInterface> &worker,
                                             bool *worker_leased) {
  const auto &task = std::get<0>(work);
  const auto &spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Attempting to dispatch task " << spec.TaskId();

  std::shared_ptr<TaskResourceInstances> allocated_instances(new TaskResourceInstances());
  bool schedulable = cluster_resource_scheduler_->AllocateLocalTaskResources(
      spec.GetRequiredResources().GetResourceMap(), allocated_instances);
  bool dispatched = false;
  if (!schedulable) {
    *worker_leased = false;
    // Spill at most one task from this queue, then move on to the next
    // queue.
    int64_t _unused;
    auto placement_resources = spec.GetRequiredPlacementResources().GetResourceMap();
    std::string node_id_string = cluster_resource_scheduler_->GetBestSchedulableNode(
        placement_resources, spec.IsActorCreationTask(), &_unused);
    if (node_id_string != self_node_id_.Binary() && !node_id_string.empty()) {
      NodeID node_id = NodeID::FromBinary(node_id_string);
      Spillback(node_id, work);
      dispatched = true;
    }
  } else {
    worker->SetOwnerAddress(spec.CallerAddress());
    if (spec.IsActorCreationTask()) {
      // The actor belongs to this worker now.
      worker->SetLifetimeAllocatedInstances(allocated_instances);
    } else {
      worker->SetAllocatedInstances(allocated_instances);
    }
    worker->AssignTaskId(spec.TaskId());
    if (!RayConfig::instance().enable_multi_tenancy()) {
      worker->AssignJobId(spec.JobId());
    }
    worker->SetAssignedTask(task);
    *worker_leased = true;
    dispatched = true;
  }
  return dispatched;
}

void ClusterTaskManager::QueueTask(const Task &task, rpc::RequestWorkerLeaseReply *reply,
                                   std::function<void(void)> callback) {
  RAY_LOG(DEBUG) << "Queuing task " << task.GetTaskSpecification().TaskId();
  Work work = std::make_tuple(task, reply, callback);
  const auto &scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  tasks_to_schedule_[scheduling_class].push_back(work);
}

void ClusterTaskManager::TasksUnblocked(const std::vector<TaskID> ready_ids) {
  for (const auto &task_id : ready_ids) {
    auto it = waiting_tasks_.find(task_id);
    if (it != waiting_tasks_.end()) {
      auto work = it->second;
      const auto &task = std::get<0>(work);
      const auto &scheduling_key = task.GetTaskSpecification().GetSchedulingClass();
      RAY_LOG(DEBUG) << "Args ready, task can be dispatched "
                     << task.GetTaskSpecification().TaskId();
      tasks_to_dispatch_[scheduling_key].push_back(work);
      waiting_tasks_.erase(it);
    }
  }
}

void ClusterTaskManager::HandleTaskFinished(std::shared_ptr<WorkerInterface> worker) {
  cluster_resource_scheduler_->FreeLocalTaskResources(worker->GetAllocatedInstances());
  worker->ClearAllocatedInstances();
  cluster_resource_scheduler_->FreeLocalTaskResources(
      worker->GetLifetimeAllocatedInstances());
  worker->ClearLifetimeAllocatedInstances();
}

void ReplyCancelled(Work &work) {
  auto reply = std::get<1>(work);
  auto callback = std::get<2>(work);
  reply->set_canceled(true);
  callback();
}

bool ClusterTaskManager::CancelTask(const TaskID &task_id) {
  for (auto shapes_it = tasks_to_schedule_.begin(); shapes_it != tasks_to_schedule_.end();
       shapes_it++) {
    auto &work_queue = shapes_it->second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end(); work_it++) {
      if (std::get<0>(*work_it).GetTaskSpecification().TaskId() == task_id) {
        ReplyCancelled(*work_it);
        work_queue.erase(work_it);
        if (work_queue.empty()) {
          tasks_to_schedule_.erase(shapes_it);
        }
        return true;
      }
    }
  }
  for (auto shapes_it = tasks_to_dispatch_.begin(); shapes_it != tasks_to_dispatch_.end();
       shapes_it++) {
    auto &work_queue = shapes_it->second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end(); work_it++) {
      if (std::get<0>(*work_it).GetTaskSpecification().TaskId() == task_id) {
        ReplyCancelled(*work_it);
        work_queue.erase(work_it);
        if (work_queue.empty()) {
          tasks_to_dispatch_.erase(shapes_it);
        }
        return true;
      }
    }
  }

  auto iter = waiting_tasks_.find(task_id);
  if (iter != waiting_tasks_.end()) {
    ReplyCancelled(iter->second);
    waiting_tasks_.erase(iter);
    return true;
  }

  return false;
}

void ClusterTaskManager::Heartbeat(bool light_heartbeat_enabled,
                                   std::shared_ptr<HeartbeatTableData> data) const {
  // TODO (WangTao): Find a way to check if load changed and combine it with light
  // heartbeat. Now we just report it every time.
  data->set_resource_load_changed(true);
  auto resource_loads = data->mutable_resource_load();
  auto resource_load_by_shape =
      data->mutable_resource_load_by_shape()->mutable_resource_demands();

  // TODO (Alex): Implement the 1-CPU task optimization.
  for (const auto &pair : tasks_to_schedule_) {
    const auto &scheduling_class = pair.first;
    const auto &resources =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class)
            .GetResourceMap();
    const auto &queue = pair.second;
    const auto &count = queue.size();

    auto by_shape_entry = resource_load_by_shape->Add();

    for (const auto &resource : resources) {
      // Add to `resource_loads`.
      const auto &label = resource.first;
      const auto &quantity = resource.second;
      (*resource_loads)[label] += quantity * count;

      // Add to `resource_load_by_shape`.
      (*by_shape_entry->mutable_shape())[label] = quantity;
      // TODO (Alex): Technically being on `tasks_to_schedule` could also mean
      // that the entire cluster is utilized.
      by_shape_entry->set_num_infeasible_requests_queued(count);
    }
  }

  for (const auto &pair : tasks_to_dispatch_) {
    const auto &scheduling_class = pair.first;
    const auto &resources =
        TaskSpecification::GetSchedulingClassDescriptor(scheduling_class)
            .GetResourceMap();
    const auto &queue = pair.second;
    const auto &count = queue.size();

    auto by_shape_entry = resource_load_by_shape->Add();

    for (const auto &resource : resources) {
      // Add to `resource_loads`.
      const auto &label = resource.first;
      const auto &quantity = resource.second;
      (*resource_loads)[label] += quantity * count;

      // Add to `resource_load_by_shape`.
      (*by_shape_entry->mutable_shape())[label] = quantity;
      // TODO (Alex): Technically being on `tasks_to_schedule` could also mean
      // that the entire cluster is utilized.
      by_shape_entry->set_num_ready_requests_queued(count);
    }
  }
}

std::string ClusterTaskManager::DebugString() const {
  std::stringstream buffer;
  buffer << "========== Node: " << self_node_id_ << " =================\n";
  buffer << "Schedule queue length: " << tasks_to_schedule_.size() << "\n";
  buffer << "Dispatch queue length: " << tasks_to_dispatch_.size() << "\n";
  buffer << "Waiting tasks size: " << waiting_tasks_.size() << "\n";
  buffer << "cluster_resource_scheduler state: "
         << cluster_resource_scheduler_->DebugString() << "\n";
  buffer << "==================================================";
  return buffer.str();
}

void ClusterTaskManager::Dispatch(
    std::shared_ptr<WorkerInterface> worker,
    std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers,
    const TaskSpecification &task_spec, rpc::RequestWorkerLeaseReply *reply,
    std::function<void(void)> send_reply_callback) {
  RAY_LOG(DEBUG) << "Dispatching task " << task_spec.TaskId();
  // Pass the contact info of the worker to use.
  reply->mutable_worker_address()->set_ip_address(worker->IpAddress());
  reply->mutable_worker_address()->set_port(worker->Port());
  reply->mutable_worker_address()->set_worker_id(worker->WorkerId().Binary());
  reply->mutable_worker_address()->set_raylet_id(self_node_id_.Binary());

  RAY_CHECK(leased_workers.find(worker->WorkerId()) == leased_workers.end());
  leased_workers[worker->WorkerId()] = worker;

  // Update our internal view of the cluster state.
  std::shared_ptr<TaskResourceInstances> allocated_resources;
  if (task_spec.IsActorCreationTask()) {
    allocated_resources = worker->GetLifetimeAllocatedInstances();
  } else {
    allocated_resources = worker->GetAllocatedInstances();
  }
  auto predefined_resources = allocated_resources->predefined_resources;
  ::ray::rpc::ResourceMapEntry *resource;
  for (size_t res_idx = 0; res_idx < predefined_resources.size(); res_idx++) {
    bool first = true;  // Set resource name only if at least one of its
                        // instances has available capacity.
    for (size_t inst_idx = 0; inst_idx < predefined_resources[res_idx].size();
         inst_idx++) {
      if (predefined_resources[res_idx][inst_idx] > 0.) {
        if (first) {
          resource = reply->add_resource_mapping();
          resource->set_name(
              cluster_resource_scheduler_->GetResourceNameFromIndex(res_idx));
          first = false;
        }
        auto rid = resource->add_resource_ids();
        rid->set_index(inst_idx);
        rid->set_quantity(predefined_resources[res_idx][inst_idx].Double());
      }
    }
  }
  auto custom_resources = allocated_resources->custom_resources;
  for (auto it = custom_resources.begin(); it != custom_resources.end(); ++it) {
    bool first = true;  // Set resource name only if at least one of its
                        // instances has available capacity.
    for (size_t inst_idx = 0; inst_idx < it->second.size(); inst_idx++) {
      if (it->second[inst_idx] > 0.) {
        if (first) {
          resource = reply->add_resource_mapping();
          resource->set_name(
              cluster_resource_scheduler_->GetResourceNameFromIndex(it->first));
          first = false;
        }
        auto rid = resource->add_resource_ids();
        rid->set_index(inst_idx);
        rid->set_quantity(it->second[inst_idx].Double());
      }
    }
  }

  // Send the result back.
  send_reply_callback();
}

void ClusterTaskManager::Spillback(const NodeID &spillback_to, const Work &work) {
  const auto &task_spec = std::get<0>(work).GetTaskSpecification();
  RAY_LOG(DEBUG) << "Spilling task " << task_spec.TaskId() << " to node " << spillback_to;
  cluster_resource_scheduler_->AllocateRemoteTaskResources(
      spillback_to.Binary(), task_spec.GetRequiredResources().GetResourceMap());

  auto node_info_opt = get_node_info_(spillback_to);
  RAY_CHECK(node_info_opt)
      << "Spilling back to a node manager, but no GCS info found for node "
      << spillback_to;
  auto reply = std::get<1>(work);
  reply->mutable_retry_at_raylet_address()->set_ip_address(
      node_info_opt->node_manager_address());
  reply->mutable_retry_at_raylet_address()->set_port(node_info_opt->node_manager_port());
  reply->mutable_retry_at_raylet_address()->set_raylet_id(spillback_to.Binary());

  auto send_reply_callback = std::get<2>(work);
  send_reply_callback();
}

}  // namespace raylet
}  // namespace ray
