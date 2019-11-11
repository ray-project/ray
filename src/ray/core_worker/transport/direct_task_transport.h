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
  TaskSpecification task;
  /// The remaining dependencies to resolve for this task.
  absl::flat_hash_set<ObjectID> local_dependencies;
};

// This class is thread-safe.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(CoreWorkerMemoryStoreProvider &store_provider)
      : in_memory_store_(store_provider), num_pending_(0) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call ids in arguments are converted to values.
  void ResolveDependencies(const TaskSpecification &task,
                           std::function<void()> on_complete);

  /// Return the number of tasks pending dependency resolution.
  /// TODO(ekl) this should be exposed in worker stats.
  int NumPendingTasks() const { return num_pending_; }

 private:
  /// The store provider.
  CoreWorkerMemoryStoreProvider in_memory_store_;

  /// Number of tasks pending dependency resolution.
  std::atomic<int> num_pending_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;
};

typedef std::pair<std::string, int> WorkerAddress;
typedef std::function<std::shared_ptr<rpc::CoreWorkerClientInterface>(WorkerAddress)>
    ClientFactoryFn;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(WorkerLeaseInterface &lease_client,
                                ClientFactoryFn client_factory,
                                CoreWorkerMemoryStoreProvider store_provider)
      : lease_client_(lease_client),
        client_factory_(client_factory),
        in_memory_store_(store_provider),
        resolver_(in_memory_store_) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

  /// Callback for when the raylet grants us a worker lease. The worker is returned
  /// to the raylet once it finishes its task and either the lease term has
  /// expired, or there is no more work it can take on.
  ///
  /// \param[in] addr The (addr, port) pair identifying the worker.
  void HandleWorkerLeaseGranted(const WorkerAddress addr);

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission. If an error was encountered
  /// processing the worker, we don't attempt to re-use the worker.
  void OnWorkerIdle(const WorkerAddress &addr, bool was_error);

  /// Request a new worker from the raylet if no such requests are currently in
  /// flight.
  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Push a task to a specific worker.
  void PushNormalTask(const WorkerAddress &addr, rpc::CoreWorkerClientInterface &client,
                      TaskSpecification &task_spec);

  /// Mark a direct call as failed by storing errors for its return objects.
  void TreatTaskAsFailed(const TaskID &task_id, int num_returns,
                         const rpc::ErrorType &error_type);

  // Client that can be used to lease and return workers.
  WorkerLeaseInterface &lease_client_;

  /// Factory for producing new core worker clients.
  ClientFactoryFn client_factory_;

  /// The store provider.
  CoreWorkerMemoryStoreProvider in_memory_store_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<WorkerAddress, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      client_cache_ GUARDED_BY(mu_);

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<TaskSpecification> queued_tasks_ GUARDED_BY(mu_);
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
