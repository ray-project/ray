
#include "ray/core_worker/transport/mock_transport.h"
#include "ray/core_worker/store_provider/mock_store_provider.h"

namespace ray {

CoreWorkerMockTaskSubmitterReceiver::CoreWorkerMockTaskSubmitterReceiver() {}

CoreWorkerMockTaskSubmitterReceiver &CoreWorkerMockTaskSubmitterReceiver::Instance() {
  return instance_;
}

Status CoreWorkerMockTaskSubmitterReceiver::SubmitTask(const TaskSpec &task) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::unordered_set<ObjectID> unready_objects = GetUnreadyObjects(task);
  auto task_ptr = std::make_shared<TaskSpec>(task);
  if (unready_objects.empty()) {
    ready_tasks_.emplace_back(task_ptr);
  } else {
    for (auto &object_id : unready_objects) {
      auto wrapped_task = std::make_shared<std::pair<std::shared_ptr<TaskSpec>, size_t>>(task_ptr, unready_objects.size());
      auto it = waiting_tasks_.find(object_id);
      if (it == waiting_tasks_.end()) {
        std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>> set;
        set.insert(wrapped_task);
        waiting_tasks_.emplace(object_id, set);
      } else {
        it->second.insert(wrapped_task);
      }
    }
  }
}

Status CoreWorkerMockTaskSubmitterReceiver::GetTasks(std::vector<TaskSpec> *tasks) {
  std::lock_guard<std::mutex> guard(mutex_);
  while (ready_tasks_.empty()) {
    usleep(100 * 1000);
  }
  auto task = ready_tasks_.front();
  ready_tasks_.pop_front();
  RAY_CHECK((*tasks).empty());
  (*tasks).emplace_back(*task);
  return Status::OK();
}

void CoreWorkerMockTaskSubmitterReceiver::OnObjectPut(const ObjectID &object_id) {
  auto it = waiting_tasks_.find(object_id);
  if (it != waiting_tasks_.end()) {
    std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>> tasks = it->second;
    waiting_tasks_.erase(it);
    for (auto &wrapped_task : tasks) {
      wrapped_task->second--;
      if (wrapped_task->second == 0) {
        ready_tasks_.emplace_back(wrapped_task->first);
      }
    }
  }
}

std::unordered_set<ObjectID> CoreWorkerMockTaskSubmitterReceiver::GetUnreadyObjects(const TaskSpec &task) {
  std::unordered_set<ObjectID> unready_objects;
  auto &task_spec = task.GetTaskSpecification();
  for (int64_t i = 0; i < task_spec.NumArgs(); i++) {
    if (task_spec.ArgByRef(i)) {
      for (int64_t j = 0; j < task_spec.ArgIdCount(i); j++) {
        ObjectID object_id = task_spec.ArgId(i, j);
        if (!CoreWorkerMockStoreProvider::Instance().IsObjectReady(object_id)) {
          unready_objects.insert(object_id);
        }
      }
    }
  }
  return unready_objects;
}

}  // namespace ray
