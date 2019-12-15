#ifndef RAY_CORE_WORKER_DIRECT_TASK_H
#define RAY_CORE_WORKER_DIRECT_TASK_H

#include <google/protobuf/repeated_field.h>

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

typedef std::function<std::shared_ptr<WorkerLeaseInterface>(const std::string &ip_address,
                                                            int port)>
    LeaseClientFactoryFn;

// The task queues are keyed on resource shape & function descriptor
// (encapsulated in SchedulingClass) to defer resource allocation decisions to the raylet
// and ensure fairness between different tasks, as well as plasma task dependencies as
// a performance optimization because the raylet will fetch plasma dependencies to the
// scheduled worker.
using SchedulingKey = std::pair<SchedulingClass, std::vector<ObjectID>>;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  CoreWorkerDirectTaskSubmitter(std::shared_ptr<WorkerLeaseInterface> lease_client,
                                rpc::ClientFactoryFn client_factory,
                                LeaseClientFactoryFn lease_client_factory,
                                std::shared_ptr<CoreWorkerMemoryStore> store,
                                std::shared_ptr<TaskFinisherInterface> task_finisher,
                                ClientID local_raylet_id, int64_t lease_timeout_ms)
      : local_lease_client_(lease_client),
        client_factory_(client_factory),
        lease_client_factory_(lease_client_factory),
        resolver_(store),
        task_finisher_(task_finisher),
        local_raylet_id_(local_raylet_id),
        lease_timeout_ms_(lease_timeout_ms) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission. If an error was encountered
  /// processing the worker, we don't attempt to re-use the worker.
  ///
  /// \param[in] addr The address of the worker.
  /// \param[in] task_queue_key The scheduling class of the worker.
  /// \param[in] was_error Whether the task failed to be submitted.
  /// \param[in] assigned_resources Resource ids previously assigned to the worker.
  void OnWorkerIdle(
      const rpc::WorkerAddress &addr, const SchedulingKey &task_queue_key, bool was_error,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Retry a failed lease request.
  void RetryLeaseRequest(Status status,
                         std::shared_ptr<WorkerLeaseInterface> lease_client,
                         const SchedulingKey &scheduling_key)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Get an existing lease client or connect a new one. If a raylet_address is
  /// provided, this connects to a remote raylet. Else, this connects to the
  /// local raylet.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address *raylet_address) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Request a new worker from the raylet if no such requests are currently in
  /// flight and there are tasks queued. If a raylet address is provided, then
  /// the worker should be requested from the raylet at that address. Else, the
  /// worker should be requested from the local raylet.
  void RequestNewWorkerIfNeeded(const SchedulingKey &task_queue_key,
                                const rpc::Address *raylet_address = nullptr)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Set up client state for newly granted worker lease.
  void AddWorkerLeaseClient(const rpc::WorkerAddress &addr,
                            std::shared_ptr<WorkerLeaseInterface> lease_client)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Push a task to a specific worker.
  void PushNormalTask(const rpc::WorkerAddress &addr,
                      rpc::CoreWorkerClientInterface &client,
                      const SchedulingKey &task_queue_key,
                      const TaskSpecification &task_spec,
                      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry>
                          &assigned_resources);

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

  /// The local raylet ID. Used to make sure that we use the local lease client
  /// if a remote raylet tells us to spill the task back to the local raylet.
  const ClientID local_raylet_id_;

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

  // Keeps track of pending worker lease requests to the raylet.
  absl::flat_hash_set<SchedulingKey> pending_lease_requests_ GUARDED_BY(mu_);

  // Tasks that are queued for execution. We keep individual queues per
  // scheduling class to ensure fairness.
  // Invariant: if a queue is in this map, it has at least one task.
  absl::flat_hash_map<SchedulingKey, std::deque<TaskSpecification>> task_queues_
      GUARDED_BY(mu_);
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
