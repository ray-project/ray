// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
// scheduled worker. It's also keyed on actor ID to ensure the actor creation task
// would always request a new worker lease. We need this to let raylet know about
// direct actor creation task, and reconstruct the actor if it dies. Otherwise if
// the actor creation task just reuses an existing worker, then raylet will not
// be aware of the actor and is not able to manage it.
using SchedulingKey = std::tuple<SchedulingClass, std::vector<ObjectID>, ActorID>;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  explicit CoreWorkerDirectTaskSubmitter(
      rpc::Address rpc_address, std::shared_ptr<WorkerLeaseInterface> lease_client,
      rpc::ClientFactoryFn client_factory, LeaseClientFactoryFn lease_client_factory,
      std::shared_ptr<CoreWorkerMemoryStore> store,
      std::shared_ptr<TaskFinisherInterface> task_finisher, ClientID local_raylet_id,
      int64_t lease_timeout_ms,
      std::function<Status(const TaskSpecification &, const gcs::StatusCallback &)>
          actor_create_callback = nullptr,
      absl::optional<boost::asio::steady_timer> cancel_timer = absl::nullopt)
      : rpc_address_(rpc_address),
        local_lease_client_(lease_client),
        client_factory_(client_factory),
        lease_client_factory_(lease_client_factory),
        resolver_(store, task_finisher),
        task_finisher_(task_finisher),
        lease_timeout_ms_(lease_timeout_ms),
        local_raylet_id_(local_raylet_id),
        actor_create_callback_(std::move(actor_create_callback)),
        cancel_retry_timer_(std::move(cancel_timer)) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

  /// Either remove a pending task or send an RPC to kill a running task
  ///
  /// \param[in] task_spec The task to kill.
  /// \param[in] force_kill Whether to kill the worker executing the task.
  Status CancelTask(TaskSpecification task_spec, bool force_kill);

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

  /// Cancel a pending worker lease and retry until the cancellation succeeds
  /// (i.e., the raylet drops the request). This should be called when there
  /// are no more tasks queued with the given scheduling key and there is an
  /// in-flight lease request for that key.
  void CancelWorkerLeaseIfNeeded(const SchedulingKey &scheduling_key)
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

  /// Address of our RPC server.
  rpc::Address rpc_address_;

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

  /// A function to override actor creation. The callback will be called once the actor
  /// creation task has been accepted for submission, but the actor may not be created
  /// yet.
  std::function<Status(const TaskSpecification &task_spec,
                       const gcs::StatusCallback &callback)>
      actor_create_callback_;

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
  absl::flat_hash_map<SchedulingKey,
                      std::pair<std::shared_ptr<WorkerLeaseInterface>, TaskID>>
      pending_lease_requests_ GUARDED_BY(mu_);

  // Tasks that are queued for execution. We keep individual queues per
  // scheduling class to ensure fairness.
  // Invariant: if a queue is in this map, it has at least one task.
  absl::flat_hash_map<SchedulingKey, std::deque<TaskSpecification>> task_queues_
      GUARDED_BY(mu_);

  // Tasks that were cancelled while being resolved.
  absl::flat_hash_set<TaskID> cancelled_tasks_ GUARDED_BY(mu_);

  // Keeps track of where currently executing tasks are being run.
  absl::flat_hash_map<TaskID, rpc::WorkerAddress> executing_tasks_ GUARDED_BY(mu_);

  // Retries cancelation requests if they were not successful.
  absl::optional<boost::asio::steady_timer> cancel_retry_timer_;
};

};  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_TASK_H
