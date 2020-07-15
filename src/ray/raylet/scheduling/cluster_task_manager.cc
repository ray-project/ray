#include "cluster_task_manager.h"
#include "ray/util/logging.h"

namespace ray {
namespace raylet {

ClusterTaskManager::ClusterTaskManager(
    const ClientID &self_node_id,
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
    std::function<bool(const Task &)> fulfills_dependencies_func,
    std::shared_ptr<gcs::GcsClient> gcs_client)
    : self_node_id_(self_node_id),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      fulfills_dependencies_func_(fulfills_dependencies_func),
      gcs_client_(gcs_client) {
}

bool ClusterTaskManager::SchedulePendingTasks() {
  size_t queue_size = tasks_to_schedule_.size();
  bool did_schedule = false;

  // Check every task in task_to_schedule queue to see
  // whether it can be scheduled. This avoids head-of-line
  // blocking where a task which cannot be scheduled because
  // there are not enough available resources blocks other
  // tasks from being scheduled.
  while (queue_size-- > 0) {
    Work work = tasks_to_schedule_.front();
    tasks_to_schedule_.pop_front();
    Task task = work.second;
    auto request_resources =
        task.GetTaskSpecification().GetRequiredResources().GetResourceMap();
    int64_t violations = 0;
    std::string node_id_string = cluster_resource_scheduler_->GetBestSchedulableNode(
        request_resources, &violations);
    if (node_id_string.empty()) {
      /// There is no node that has available resources to run the request.
      tasks_to_schedule_.push_back(work);
      continue;
    } else {
      if (node_id_string == self_node_id_.Binary()) {
        did_schedule = did_schedule || WaitForTaskArgsRequests(work);
      } else {
        // Should spill over to a different node.
        cluster_resource_scheduler_->AllocateRemoteTaskResources(node_id_string,
                                                                 request_resources);

        ClientID node_id = ClientID::FromBinary(node_id_string);
        auto node_info_opt = gcs_client_->Nodes().Get(node_id);
        RAY_CHECK(node_info_opt)
            << "Spilling back to a node manager, but no GCS info found for node "
            << node_id;
        work.first(nullptr, node_id, node_info_opt->node_manager_address(),
                   node_info_opt->node_manager_port());
      }
    }
  }
  return did_schedule;
}

bool ClusterTaskManager::WaitForTaskArgsRequests(Work work) {
  Task task = work.second;
  auto t1 = task.GetTaskSpecification();
  std::vector<ObjectID> object_ids = t1.GetDependencies();
  bool can_dispatch = true;
  if (object_ids.size() > 0) {
    bool args_ready = fulfills_dependencies_func_(task);
    if (args_ready) {
      tasks_to_dispatch_.push_back(work);
    } else {
      can_dispatch = false;
      TaskID task_id = task.GetTaskSpecification().TaskId();
      waiting_tasks_.try_emplace(task_id, work);
    }
  } else {
    tasks_to_dispatch_.push_back(work);
  }
  return can_dispatch;
}

void ClusterTaskManager::DispatchScheduledTasksToWorkers(WorkerPool &worker_pool) {
  // Check every task in task_to_dispatch queue to see
  // whether it can be dispatched and ran. This avoids head-of-line
  // blocking where a task which cannot be dispatched because
  // there are not enough available resources blocks other
  // tasks from being dispatched.
  for (size_t queue_size = tasks_to_dispatch_.size(); queue_size > 0; queue_size--) {
    auto task = tasks_to_dispatch_.front();
    auto reply = task.first;
    auto spec = task.second.GetTaskSpecification();
    tasks_to_dispatch_.pop_front();

    std::shared_ptr<Worker> worker = worker_pool.PopWorker(spec);
    if (!worker) {
      // No worker available to schedule this task.
      // Put the task back in the dispatch queue.
      tasks_to_dispatch_.push_front(task);
      return;
    }

    std::shared_ptr<TaskResourceInstances> allocated_instances(
        new TaskResourceInstances());
    bool schedulable = cluster_resource_scheduler_->AllocateLocalTaskResources(
        spec.GetRequiredResources().GetResourceMap(), allocated_instances);
    if (!schedulable) {
      // Not enough resources to schedule this task.
      // Put it back at the end of the dispatch queue.
      tasks_to_dispatch_.push_back(task);
      worker_pool.PushWorker(worker);
      // Try next task in the dispatch queue.
      continue;
    }

    worker->SetOwnerAddress(spec.CallerAddress());
    if (spec.IsActorCreationTask()) {
      // The actor belongs to this worker now.
      worker->SetLifetimeAllocatedInstances(allocated_instances);
    } else {
      worker->SetAllocatedInstances(allocated_instances);
    }
    worker->AssignTaskId(spec.TaskId());
    worker->AssignJobId(spec.JobId());
    worker->SetAssignedTask(task.second);

    reply(worker, ClientID::Nil(), "", -1);
  }
}

void ClusterTaskManager::QueueTask(ScheduleFn fn, const Task &task) {
  Work work = std::make_pair(fn, task);
  tasks_to_schedule_.push_back(work);
}

void ClusterTaskManager::TasksUnblocked(const std::vector<TaskID> readyIds) {
  for (auto task_id : readyIds) {
    auto it = waiting_tasks_.find(task_id);
    if (it == waiting_tasks_.end()) {
      const auto &work = *it;
      tasks_to_dispatch_.push_back(work.second);
      waiting_tasks_.erase(it);
    }
  }
}
}  // namespace raylet
}  // namespace ray
