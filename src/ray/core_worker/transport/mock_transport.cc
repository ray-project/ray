
#include "ray/core_worker/transport/mock_transport.h"

namespace ray {

Status CoreWorkerMockTaskPool::SubmitTask(const TaskSpec &task) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::unordered_set<ObjectID> unready_objects = GetUnreadyObjects(task);
  auto task_ptr = std::make_shared<TaskSpec>(task);
  if (unready_objects.empty()) {
    PutReadyTask(task_ptr);
  } else {
    for (auto &object_id : unready_objects) {
      auto wrapped_task = std::make_shared<std::pair<std::shared_ptr<TaskSpec>, size_t>>(
          task_ptr, unready_objects.size());
      auto it = waiting_tasks_.find(object_id);
      if (it == waiting_tasks_.end()) {
        std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>>
            set;
        set.insert(wrapped_task);
        waiting_tasks_.emplace(object_id, set);
      } else {
        it->second.insert(wrapped_task);
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerMockTaskPool::GetTasks(std::shared_ptr<WorkerContext> worker_context,
                                        std::vector<TaskSpec> *tasks) {
  auto &actor_id = worker_context->GetCurrentActorID();
  std::shared_ptr<TaskSpec> task;
  std::lock_guard<std::mutex> guard(mutex_);
  while (!task) {
    if (!actor_id.IsNil()) {
      auto it = actor_ready_tasks_.find(actor_id);
      if (it == actor_ready_tasks_.end() || it->second.empty()) {
        usleep(100 * 1000);
      } else {
        task = it->second.front();
        it->second.pop_front();
      }
    } else {
      while (other_ready_tasks_.empty()) {
        usleep(100 * 1000);
      }
      task = other_ready_tasks_.front();
      other_ready_tasks_.pop_front();
    }
  }
  RAY_CHECK((*tasks).empty());
  (*tasks).emplace_back(*task);
  return Status::OK();
}

void CoreWorkerMockTaskPool::OnObjectPut(const ObjectID &object_id) {
  auto it = waiting_tasks_.find(object_id);
  if (it != waiting_tasks_.end()) {
    std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>>
        tasks = it->second;
    waiting_tasks_.erase(it);
    for (auto &wrapped_task : tasks) {
      wrapped_task->second--;
      if (wrapped_task->second == 0) {
        PutReadyTask(wrapped_task->first);
      }
    }
  }
}

void CoreWorkerMockTaskPool::SetMockStoreProvider(
    std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider) {
  RAY_CHECK(!mock_store_provider_);
  mock_store_provider_ = mock_store_provider;
}

void CoreWorkerMockTaskPool::PutReadyTask(std::shared_ptr<TaskSpec> task) {
  if (task->GetTaskSpecification().IsActorTask()) {
    const ActorID &actor_id = task->GetTaskSpecification().ActorId();
    auto it = actor_ready_tasks_.find(actor_id);
    if (it == actor_ready_tasks_.end()) {
      actor_ready_tasks_.emplace(actor_id, std::list<std::shared_ptr<TaskSpec>>());
    }
    actor_ready_tasks_[actor_id].emplace_back(task);
  } else {
    other_ready_tasks_.emplace_back(task);
  }
}

std::unordered_set<ObjectID> CoreWorkerMockTaskPool::GetUnreadyObjects(
    const TaskSpec &task) {
  std::unordered_set<ObjectID> unready_objects;
  auto &task_spec = task.GetTaskSpecification();
  for (int64_t i = 0; i < task_spec.NumArgs(); i++) {
    if (task_spec.ArgByRef(i)) {
      for (int64_t j = 0; j < task_spec.ArgIdCount(i); j++) {
        ObjectID object_id = task_spec.ArgId(i, j);
        if (!mock_store_provider_->IsObjectReady(object_id)) {
          unready_objects.insert(object_id);
        }
      }
    }
  }
  return unready_objects;
}

CoreWorkerMockTaskSubmitter::CoreWorkerMockTaskSubmitter(
    std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool)
    : mock_task_pool_(mock_task_pool) {}

Status CoreWorkerMockTaskSubmitter::SubmitTask(const TaskSpec &task) {
  return mock_task_pool_->SubmitTask(task);
}

CoreWorkerMockTaskReceiver::CoreWorkerMockTaskReceiver(
    std::shared_ptr<WorkerContext> worker_context,
    std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool)
    : worker_context_(worker_context), mock_task_pool_(mock_task_pool) {}

Status CoreWorkerMockTaskReceiver::GetTasks(std::vector<TaskSpec> *tasks) {
  return mock_task_pool_->GetTasks(worker_context_, tasks);
}

}  // namespace ray
