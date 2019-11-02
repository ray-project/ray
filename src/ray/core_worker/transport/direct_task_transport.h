#ifndef RAY_CORE_WORKER_DIRECT_TASK_H
#define RAY_CORE_WORKER_DIRECT_TASK_H

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

//struct TaskState {
//  Task task;
//  std::vector<ObjectID> local_dependencies;
//  std::vector<ObjectID> remote_dependencies;
//};
//
//// This class is NOT thread-safe.
//class DependencyResolver {
// public:
//  DependencyResolver();
//
//  // inserts into pending_tasks
//  void ResolveDependencies(const TaskSpecification& task, std::function<void(Task)> on_complete) {
//    // first fill in any local dependencies if possible
//    if (task.local_dependencies.empty() && task.remote_dependencies.empty()) {
//      on_complete(task);
//      return;
//    }
//    auto tag = next_request_id_++;
//    pending_tasks_[tag] = TaskState(task);
//    for (const ObjectID& dep : task.local_dependencies) {  // has direct bit set
//      if (!localAlready(dep)) {
//        local_wait_[dep].push_back(tag);
//      }
//    }
//    // TODO(ekl) should just assume remote deps are fine for now
//    if (!task.remote_dependencies.empty()) {
//      raylet_client->WaitForDirectActorCallArgs(tag, task.remote_dependencies);
//    }
//  };
//
//  // gRPC callback from raylet
//  void OnRemoteWaitComplete(int64_t tag) {
//    auto& task = pending_tasks_[tag];
//    task.remote_dependencies.clear();
//    if (task.local_dependencies.empty()) {
//      pending_tasks_.erase(tag);
//      on_complete(task);
//    }
//  }
//
//  // gRPC callback on task completion
//  void OnLocalObjectAvailable(const ObjectID& obj_id) {
//    auto it = local_wait_.find(obj_id);
//    if (it != local_wait_.end()) {
//      for (const auto& task : *it) {
//        task.local_dependencies.remove(obj_id);
//        if (task.local_dependencies.empty()) {
//          on_complete(task);
//          erase(task);
//        }
//      }
//    }
//  }
//
// private:
//  int64_t next_request_id_ = 0;
//  absl::flat_hash_map<int64_t, TaskState> pending_tasks_;
//  absl::flat_hash_map<ObjectID, std::vector<int64_t>> local_wait_;
//}

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(RayletClient& raylet_client, std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider)
      : raylet_client_(raylet_client),
        store_provider_(std::move(store_provider)) {}

  Status SubmitTask(const TaskSpecification &task_spec) {
    absl::MutexLock lock(&mu_);
//    queued_tasks_[sched_spec].push_back(task);
//    if (!worker_lease_requests_.contains(sched_spec)) {
//      worker_lease_requests_[sched_spec] = RequestNewWorker(sched_spec);
//    }
  }

//  // gRPC callback from worker
//  void OnWorkerTaskDone(Worker* worker) {
//    {
//      absl::MutexLock lock(&mu_);
//      if (queued_tasks_[sched_spec].empty()) {
//        // release worker to raylet
//      } else {
//        auto task = queued_tasks_[sched_spec].front();
//        // schedule the task
//      }
//    }
//    // process the task result
//  };
//
//  // gRPC callback from raylet
//  void OnWorkerRequestGranted(TaskSchedulingSpec sched_spec) {
//    absl::MutexLock lock(&mu_);
//    auto& queue = queued_tasks_[sched_spec];
//    if (queue.size() > 1) {
//      RequestNewWorker(sched_spec);
//    }
//    auto task = queue.front();
//    queue.pop_front();
//    // schedule the task
//  };
//
//  // outbound raylet client to raylet
//  void RequestNewWorker(TaskSchedulingSpec sched_spec) {
//    absl::MutexLock lock(&mu_);
//    raylet_client_->RequestNewWorkerAsync(task_spec);
//  };

 private:
  RayletClient& raylet_client_;

  absl::Mutex mu_;
//  absl::flat_hash_map<TaskSchedulingSpec, std::deque<TaskFn>> queued_tasks_ GUARDED_BY(mu_);
//  absl::flat_hash_map<TaskID, std::unique_ptr<Worker>> active_workers_ GUARDED_BY(mu_);

  /// The store provider.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider_;

//  DependencyResolver resolver_ GUARDED_BY(mu_);
};  // namespace ray


#endif // RAY_CORE_WORKER_DIRECT_TASK_H
