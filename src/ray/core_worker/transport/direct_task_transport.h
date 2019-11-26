#ifndef RAY_CORE_WORKER_DIRECT_TASK_H
#define RAY_CORE_WORKER_DIRECT_TASK_H

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

typedef std::function<std::shared_ptr<WorkerLeaseInterface>(const rpc::Address &)>
    LeaseClientFactoryFn;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(std::shared_ptr<WorkerLeaseInterface> lease_client,
                                rpc::ClientFactoryFn client_factory,
                                LeaseClientFactoryFn lease_client_factory,
                                std::shared_ptr<CoreWorkerMemoryStore> store,
                                std::shared_ptr<TaskFinisherInterface> task_finisher,
                                int64_t lease_timeout_ms)
      : local_lease_client_(lease_client),
        client_factory_(client_factory),
        lease_client_factory_(lease_client_factory),
        resolver_(store),
        task_finisher_(task_finisher),
        lease_timeout_ms_(lease_timeout_ms) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission. If an error was encountered
  /// processing the worker, we don't attempt to re-use the worker.
  void OnWorkerIdle(const rpc::WorkerAddress &addr, bool was_error);

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
  void HandleWorkerLeaseGranted(const rpc::WorkerAddress &addr,
                                std::shared_ptr<WorkerLeaseInterface> lease_client);

  /// Push a task to a specific worker.
  void PushNormalTask(const rpc::WorkerAddress &addr,
                      rpc::CoreWorkerClientInterface &client,
                      TaskSpecification &task_spec);

  // Client that can be used to lease and return workers from the local raylet.
  std::shared_ptr<WorkerLeaseInterface> local_lease_client_;

  /// Cache of gRPC clients to remote raylets.
  absl::flat_hash_map<ClientID, std::shared_ptr<WorkerLeaseInterface>>
      remote_lease_clients_ GUARDED_BY(mu_);

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Factory for producing new clients to request leases from remote nodes.
  LeaseClientFactoryFn lease_client_factory_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  std::shared_ptr<TaskFinisherInterface> task_finisher_;

  /// The timeout for worker leases; after this duration, workers will be returned
  /// to the raylet.
  int64_t lease_timeout_ms_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  absl::flat_hash_map<rpc::WorkerAddress, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      client_cache_ GUARDED_BY(mu_);

  /// Map from worker address to the lease client through which it should be
  /// returned and its lease expiration time.
  absl::flat_hash_map<rpc::WorkerAddress,
                      std::pair<std::shared_ptr<WorkerLeaseInterface>, int64_t>>
      worker_to_lease_client_ GUARDED_BY(mu_);

  // Whether we have a request to the Raylet to acquire a new worker in flight.
  bool worker_request_pending_ GUARDED_BY(mu_) = false;

  // Tasks that are queued for execution in this submitter..
  std::deque<TaskSpecification> queued_tasks_ GUARDED_BY(mu_);
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
