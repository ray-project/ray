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

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(
      RayletClient &raylet_client,
      CoreWorkerDirectActorTaskSubmitter direct_actor_submitter,
      std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider)
      : raylet_client_(raylet_client),
        direct_actor_submitter_(direct_actor_submitter),
        store_provider_(std::move(store_provider)) {}

  Status SubmitTask(const TaskSpecification &task_spec) {
    absl::MutexLock lock(&mu_);
    // TODO(ekl) should have a queue per distinct resource type required
    queued_tasks_.push_back(task_spec);
    RequestNewWorkerIfNeeded(task_spec);
  }

  // gRPC callback from raylet
  void HandleWorkerLeaseGranted(const std::string &address, int port) {
    absl::MutexLock lock(&mu_);
    RAY_LOG(ERROR) << "Worker lease granted" << address << ":" << port;
    worker_request_pending_ = false;
    std::string worker_id = address + ":" + std::to_string(port);

    auto it = client_cache_.find(worker_id);
    if (it == client_cache_.end()) {
      //      std::shared_ptr<rpc::DirectActorClient> grpc_client =
      //          rpc::DirectActorClient::make(address, port, client_call_manager_);
      //      RAY_CHECK(rpc_clients_.emplace(port, std::move(grpc_client)).second);
    }

    auto &client = it->second;
    PushTask(client, queued_tasks_.front());
    queued_tasks_.pop_front();
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
  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec)
      EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (worker_request_pending_) {
      return;
    }
    RAY_CHECK_OK(raylet_client_.RequestWorkerLease(resource_spec));
    worker_request_pending_ = true;
  }

  // XXX this uses the direct actor submitter to push the task
  void PushTask(rpc::DirectActorClient &client, const TaskSpecification &task_spec)
      EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
    auto status = client.PushTask(
        std::move(request), [this](Status status, const rpc::PushTaskReply &reply) {
          RAY_CHECK_OK(status) << "Task failed with error: " << status;
          for (int i = 0; i < reply.return_objects_size(); i++) {
            const auto &return_object = reply.return_objects(i);
            ObjectID object_id = ObjectID::FromBinary(return_object.object_id());
            std::shared_ptr<LocalMemoryBuffer> data_buffer;
            if (return_object.data().size() > 0) {
              data_buffer = std::make_shared<LocalMemoryBuffer>(
                  const_cast<uint8_t *>(
                      reinterpret_cast<const uint8_t *>(return_object.data().data())),
                  return_object.data().size());
            }
            std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
            if (return_object.metadata().size() > 0) {
              metadata_buffer = std::make_shared<LocalMemoryBuffer>(
                  const_cast<uint8_t *>(
                      reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
                  return_object.metadata().size());
            }
            RAY_CHECK_OK(
                store_provider_->Put(RayObject(data_buffer, metadata_buffer), object_id));
          }
        });
    RAY_CHECK_OK(status);
  }

  // Reference to the shared raylet client for leasing workers.
  RayletClient &raylet_client_;

  // Reference to direct actor submitter.
  CoreWorkerDirectActorTaskSubmitter &direct_actor_submitter_;

  /// The store provider.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<std::string, rpc::DirectActorClient> client_cache_;

  // Protects task submission state below.
  absl::Mutex mu_;

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<TaskSpecification> queued_tasks_ GUARDED_BY(mu_);
};

//  DependencyResolver resolver_ GUARDED_BY(mu_);
};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
