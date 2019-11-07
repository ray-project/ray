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
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

struct TaskState {
  /// The task to be run.
  const TaskSpecification task;
  /// The remaining dependencies to resolve for this task.
  absl::flat_hash_set<ObjectID> local_dependencies;
};

// This class is thread-safe. TODO(ekl) expose metrics.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(CoreWorkerMemoryStoreProvider &store_provider)
      : in_memory_store_(store_provider) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call ids in arguments are converted to values.
  void ResolveDependencies(const TaskSpecification &task,
                           std::function<void()> on_complete);

 private:
  /// The store provider.
  CoreWorkerMemoryStoreProvider &in_memory_store_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;
};

typedef std::pair<std::string, int> WorkerAddress;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(
      RayletClient &raylet_client, rpc::ClientCallManager &client_call_manager,
      std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider)
      : raylet_client_(raylet_client),
        client_call_manager_(client_call_manager),
        in_memory_store_(std::move(store_provider)),
        resolver_(*in_memory_store_) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(const TaskSpecification &task_spec);

  /// Callback for when the raylet grants us a worker lease. The worker is returned
  /// to the raylet once it finishes its task and either the lease term has
  /// expired, or there is no more work it can take on.
  ///
  /// \param[in] address The address of the worker.
  /// \param[in] port The port of the worker.
  void HandleWorkerLeaseGranted(const std::string &address, int port);

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission.
  void WorkerIdle(const WorkerAddress &addr);

  /// Request a new worker from the raylet if no such requests are currently in
  /// flight.
  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Push a task to a specific worker.
  void PushTask(const WorkerAddress &addr, rpc::CoreWorkerClient &client,
                std::unique_ptr<rpc::PushTaskRequest> request);

  /// Mark a direct call as failed by storing errors for its return objects.
  void TreatTaskAsFailed(const TaskID &task_id, int num_returns,
                         const rpc::ErrorType &error_type);

  // Reference to the shared raylet client for leasing workers.
  RayletClient &raylet_client_;

  /// The shared `ClientCallManager` object.
  rpc::ClientCallManager &client_call_manager_;

  /// The store provider.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> in_memory_store_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<WorkerAddress, std::shared_ptr<rpc::CoreWorkerClient>> client_cache_
      GUARDED_BY(mu_);

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<std::unique_ptr<rpc::PushTaskRequest>> queued_tasks_ GUARDED_BY(mu_);
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
