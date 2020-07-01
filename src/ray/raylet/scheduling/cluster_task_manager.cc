#include "cluster_task_manager.h"

using namespace ray::raylet;

ClusterTaskManager::ClusterTaskManager(
                                       std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
                                       std::function<bool(const Task &)> fulfills_dependencies_func,
                                       const WorkerPool &worker_pool) :
    cluster_resource_scheduler_(cluster_resource_scheduler),
    fulfills_dependencies_func_(fulfills_dependencies_func),
    worker_pool_(worker_pool) {

}

void ClusterTaskManager::NewSchedulerSchedulePendingTasks() {
  size_t queue_size = tasks_to_schedule_.size();

  // Check every task in task_to_schedule queue to see
  // whether it can be scheduled. This avoids head-of-line
  // blocking where a task which cannot be scheduled because
  // there are not enough available resources blocks other
  // tasks from being scheduled.
  while (queue_size-- > 0) {
    auto work = tasks_to_schedule_.front();
    tasks_to_schedule_.pop_front();
    auto task = work.second;
    auto request_resources =
        task.GetTaskSpecification().GetRequiredResources().GetResourceMap();
    int64_t violations = 0;
    std::string node_id_string =
        cluster_resource_scheduler_->GetBestSchedulableNode(request_resources, &violations);
    if (node_id_string.empty()) {
      /// There is no node that has available resources to run the request.
      tasks_to_schedule_.push_back(work);
      continue;
    } else {
      if (node_id_string == self_node_id_.Binary()) {
        WaitForTaskArgsRequests(work);
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
  DispatchScheduledTasksToWorkers();
}

std::unique_ptr<std::vector<std::pair<Task, Worker>>> DispatchScheduledTasksToWorkers() {

  std::unique_ptr<std::vector<std::Pair<Task, Worker>>> dispatchable(new std::vector());
  auto idle_workers = worker_pool_.GetIdleWorkers();
  auto worker_it = idle_workers.begin();

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

    std::shared_ptr<Worker> worker = worker_pool_.PeekWorker(spec);
    if (!worker) {
      // No worker available to schedule this task.
      // Put the task back in the dispatch queue.
      tasks_to_dispatch_.push_front(task);
      return;
    }

    std::shared_ptr<TaskResourceInstances> allocated_instances(
        new TaskResourceInstances())
    bool schedulable = cluster_resource_scheduler_->AllocateLocalTaskResources(
        spec.GetRequiredResources().GetResourceMap(), allocated_instances);
    if (!schedulable) {
      // Not enough resources to schedule this task.
      // Put it back at the end of the dispatch queue.
      tasks_to_dispatch_.push_back(task);
      // Try next task in the dispatch queue.
      continue;
    }

    dispatchable->push_back((task, *worker_it));
    worker_it++;
    // worker->SetOwnerAddress(spec.CallerAddress());
    // if (spec.IsActorCreationTask()) {
    //   // The actor belongs to this worker now.
    //   worker->SetLifetimeAllocatedInstances(allocated_instances);
    // } else {
    //   worker->SetAllocatedInstances(allocated_instances);
    // }
    // worker->AssignTaskId(spec.TaskId());
    // worker->AssignJobId(spec.JobId());
    // worker->SetAssignedTask(task.second);

    // reply(worker, ClientID::Nil(), "", -1);
  }
  return dispatchable;
}

void ClusterTaskManager::QueueTask(const Task &task) {
  tasks_to_schedule_.push_back(task);
}

void TasksUnblocked(const std::vector<TaskID> readyIds) {
  for (auto task_id : readyIds) {
    auto work = waiting_tasks_.find(task_id);
    if (work == waiting_tasks_.end) {
      tasks_to_dispatch_.push_back(work)
      waiting_tasks_.erase(work);
    }
  }
}
