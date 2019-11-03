#ifndef RAY_CORE_WORKER_DIRECT_TASK_H
#define RAY_CORE_WORKER_DIRECT_TASK_H

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/direct_actor_client.h"

namespace ray {

// struct TaskState {
//  Task task;
//  std::vector<ObjectID> local_dependencies;
//  std::vector<ObjectID> remote_dependencies;
//};
//
//// This class is NOT thread-safe.
// class DependencyResolver {
// public:
//  DependencyResolver();
//
//  // inserts into pending_tasks
//  void ResolveDependencies(const TaskSpecification& task, std::function<void(Task)>
//  on_complete) {
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

typedef std::pair<std::string, int> WorkerAddress;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(
      RayletClient &raylet_client,
      CoreWorkerDirectActorTaskSubmitter &direct_actor_submitter,
      std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider)
      : raylet_client_(raylet_client),
        direct_actor_submitter_(direct_actor_submitter),
        store_provider_(std::move(store_provider)) {}

  Status SubmitTask(const TaskSpecification &task_spec);

  // gRPC callback from raylet
  void HandleWorkerLeaseGranted(const std::string &address, int port);

 private:
  void WorkerIdle(const WorkerAddress &addr);

  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec);

  // XXX this uses the direct actor submitter to push the task
  void PushTask(const WorkerAddress &addr, rpc::DirectActorClient &client,
                std::unique_ptr<rpc::PushTaskRequest> request);

  // Reference to the shared raylet client for leasing workers.
  RayletClient &raylet_client_;

  // Reference to direct actor submitter.
  CoreWorkerDirectActorTaskSubmitter &direct_actor_submitter_;

  /// The store provider.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<WorkerAddress, std::unique_ptr<rpc::DirectActorClient>>
      client_cache_ GUARDED_BY(mu_);

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<std::unique_ptr<rpc::PushTaskRequest>> queued_tasks_ GUARDED_BY(mu_);
};

//  DependencyResolver resolver_ GUARDED_BY(mu_);
};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
