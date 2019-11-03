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

struct TaskState {
  const TaskSpecification task;
  absl::flat_hash_set<ObjectID> local_dependencies;
  absl::flat_hash_set<ObjectID> remote_dependencies;
};

// This class is thread-safe.
class DependencyResolver {
 public:
  DependencyResolver(
      RayletClient& raylet_client,
      CoreWorkerMemoryStoreProvider& store_provider)
    : raylet_client_(raylet_client), store_provider_(store_provider) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call ids in arguments are converted to values.
  void ResolveDependencies(
      const TaskSpecification& task, std::function<void()> on_complete);

 private:
  // Reference to the shared raylet client for waiting for deps.
  RayletClient &raylet_client_;

  /// The store provider.
  CoreWorkerMemoryStoreProvider& store_provider_;

  absl::Mutex mu_;
  int64_t next_request_id_ GUARDED_BY(mu_) = 1000000000;
  absl::flat_hash_map<int64_t, std::unique_ptr<TaskState>> pending_;
};

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
        store_provider_(std::move(store_provider)),
        resolver_(raylet_client, *store_provider_) {}

  Status SubmitTask(const TaskSpecification &task_spec);

  // gRPC callback from raylet
  void HandleWorkerLeaseGranted(const std::string &address, int port);

 private:
  void WorkerIdle(const WorkerAddress &addr);

  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // XXX this uses the direct actor submitter to push the task
  void PushTask(const WorkerAddress &addr, rpc::DirectActorClient &client,
                std::unique_ptr<rpc::PushTaskRequest> request);

  // Reference to the shared raylet client for leasing workers.
  RayletClient &raylet_client_;

  // Reference to direct actor submitter.
  CoreWorkerDirectActorTaskSubmitter &direct_actor_submitter_;

  /// The store provider.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider_;

  /// Resolve local and remote dependencies;
  DependencyResolver resolver_;

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
