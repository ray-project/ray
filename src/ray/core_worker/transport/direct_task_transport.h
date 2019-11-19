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
  LocalDependencyResolver(std::shared_ptr<CoreWorkerMemoryStoreProvider> store_provider)
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
  std::shared_ptr<CoreWorkerMemoryStoreProvider> in_memory_store_;

  /// Number of tasks pending dependency resolution.
  std::atomic<int> num_pending_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;
};

typedef std::pair<std::string, int> WorkerAddress;
typedef std::function<std::shared_ptr<rpc::CoreWorkerClientInterface>(WorkerAddress)>
    ClientFactoryFn;
typedef std::function<std::shared_ptr<WorkerLeaseInterface>(const rpc::Address &)>
    LeaseClientFactoryFn;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(
      std::shared_ptr<WorkerLeaseInterface> lease_client, ClientFactoryFn client_factory,
      LeaseClientFactoryFn lease_client_factory,
      std::shared_ptr<CoreWorkerMemoryStoreProvider> store_provider)
      : local_lease_client_(lease_client),
        client_factory_(client_factory),
        lease_client_factory_(lease_client_factory),
        in_memory_store_(store_provider),
        resolver_(in_memory_store_) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission. If an error was encountered
  /// processing the worker, we don't attempt to re-use the worker.
  void OnWorkerIdle(const WorkerAddress &addr, bool was_error);

  /// Get an existing lease client or connect a new one. If a raylet_address is
  /// provided, this connects to a remote raylet. Else, this connects to the
  /// local raylet.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address *raylet_address) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Request a new worker from the raylet if no such requests are currently in
  /// flight and there are tasks queued. If a raylet address is provided, then
  /// the worker should be requested from the raylet at that address. Else, the
  /// worker should be requested from the local raylet.
  void RequestNewWorkerIfNeeded(const TaskSpecification &resource_spec,
                                const rpc::Address *raylet_address = nullptr)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Callback for when the raylet grants us a worker lease. The worker is returned
  /// to the raylet via the given lease client once the task queue is empty.
  /// TODO: Implement a lease term by which we need to return the worker.
  void HandleWorkerLeaseGranted(const WorkerAddress &addr,
                                std::shared_ptr<WorkerLeaseInterface> lease_client);

  /// Push a task to a specific worker.
  void PushNormalTask(const WorkerAddress &addr, rpc::CoreWorkerClientInterface &client,
                      TaskSpecification &task_spec);

  // Client that can be used to lease and return workers from the local raylet.
  std::shared_ptr<WorkerLeaseInterface> local_lease_client_;

  /// Cache of gRPC clients to remote raylets.
  absl::flat_hash_map<ClientID, std::shared_ptr<WorkerLeaseInterface>>
      remote_lease_clients_ GUARDED_BY(mu_);

  /// Factory for producing new core worker clients.
  ClientFactoryFn client_factory_;

  /// Factory for producing new clients to request leases from remote nodes.
  LeaseClientFactoryFn lease_client_factory_;

  /// The store provider.
  std::shared_ptr<CoreWorkerMemoryStoreProvider> in_memory_store_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<WorkerAddress, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      client_cache_ GUARDED_BY(mu_);

  /// Map from worker address to the lease client through which it should be
  /// returned.
  absl::flat_hash_map<WorkerAddress, std::shared_ptr<WorkerLeaseInterface>>
      worker_to_lease_client_ GUARDED_BY(mu_);

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<TaskSpecification> queued_tasks_ GUARDED_BY(mu_);
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
