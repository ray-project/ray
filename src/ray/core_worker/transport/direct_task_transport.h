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

#pragma once

#include <google/protobuf/repeated_field.h>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace core {

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
// be aware of the actor and is not able to manage it.  It is also keyed on
// RuntimeEnvHash, because a worker can only run a task if the worker's RuntimeEnvHash
// matches the RuntimeEnvHash required by the task spec.
typedef int RuntimeEnvHash;
using SchedulingKey =
    std::tuple<SchedulingClass, std::vector<ObjectID>, ActorID, RuntimeEnvHash>;

// This class is thread-safe.
class CoreWorkerDirectTaskSubmitter {
 public:
  explicit CoreWorkerDirectTaskSubmitter(
      rpc::Address rpc_address,
      std::shared_ptr<WorkerLeaseInterface> lease_client,
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
      LeaseClientFactoryFn lease_client_factory,
      std::shared_ptr<LeasePolicyInterface> lease_policy,
      std::shared_ptr<CoreWorkerMemoryStore> store,
      std::shared_ptr<TaskFinisherInterface> task_finisher,
      NodeID local_raylet_id,
      WorkerType worker_type,
      int64_t lease_timeout_ms,
      std::shared_ptr<ActorCreatorInterface> actor_creator,
      const JobID &job_id,
      absl::optional<boost::asio::steady_timer> cancel_timer = absl::nullopt,
      uint64_t max_pending_lease_requests_per_scheduling_category =
          ::RayConfig::instance().max_pending_lease_requests_per_scheduling_category())
      : rpc_address_(rpc_address),
        local_lease_client_(lease_client),
        lease_client_factory_(lease_client_factory),
        lease_policy_(std::move(lease_policy)),
        resolver_(*store, *task_finisher, *actor_creator),
        task_finisher_(task_finisher),
        lease_timeout_ms_(lease_timeout_ms),
        local_raylet_id_(local_raylet_id),
        worker_type_(worker_type),
        actor_creator_(actor_creator),
        client_cache_(core_worker_client_pool),
        job_id_(job_id),
        max_pending_lease_requests_per_scheduling_category_(
            max_pending_lease_requests_per_scheduling_category),
        cancel_retry_timer_(std::move(cancel_timer)) {}

  /// Schedule a task for direct submission to a worker.
  ///
  /// \param[in] task_spec The task to schedule.
  Status SubmitTask(TaskSpecification task_spec);

  /// Either remove a pending task or send an RPC to kill a running task
  ///
  /// \param[in] task_spec The task to kill.
  /// \param[in] force_kill Whether to kill the worker executing the task.
  Status CancelTask(TaskSpecification task_spec, bool force_kill, bool recursive);

  Status CancelRemoteTask(const ObjectID &object_id,
                          const rpc::Address &worker_addr,
                          bool force_kill,
                          bool recursive);
  /// Check that the scheduling_key_entries_ hashmap is empty by calling the private
  /// CheckNoSchedulingKeyEntries function after acquiring the lock.
  bool CheckNoSchedulingKeyEntriesPublic() {
    absl::MutexLock lock(&mu_);
    return scheduling_key_entries_.empty();
  }

  int64_t GetNumTasksSubmitted() const { return num_tasks_submitted_; }

  int64_t GetNumLeasesRequested() {
    absl::MutexLock lock(&mu_);
    return num_leases_requested_;
  }

  /// Report worker backlog information to the local raylet.
  /// Since each worker only reports to its local rayet
  /// we avoid double counting backlogs in autoscaler.
  void ReportWorkerBacklog();

 private:
  /// Schedule more work onto an idle worker or return it back to the raylet if
  /// no more tasks are queued for submission. If an error was encountered
  /// processing the worker, we don't attempt to re-use the worker.
  ///
  /// \param[in] addr The address of the worker.
  /// \param[in] task_queue_key The scheduling class of the worker.
  /// \param[in] was_error Whether the task failed to be submitted.
  /// \param[in] worker_exiting Whether the worker is exiting.
  /// \param[in] assigned_resources Resource ids previously assigned to the worker.
  void OnWorkerIdle(
      const rpc::WorkerAddress &addr,
      const SchedulingKey &task_queue_key,
      bool was_error,
      bool worker_exiting,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Get an existing lease client or connect a new one. If a raylet_address is
  /// provided, this connects to a remote raylet. Else, this connects to the
  /// local raylet.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address *raylet_address) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Report worker backlog information to the local raylet
  void ReportWorkerBacklogInternal() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Report backlog if the backlog size is changed for this scheduling key
  /// since last report
  void ReportWorkerBacklogIfNeeded(const SchedulingKey &scheduling_key)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

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
  void AddWorkerLeaseClient(
      const rpc::WorkerAddress &addr,
      std::shared_ptr<WorkerLeaseInterface> lease_client,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
      const SchedulingKey &scheduling_key,
      const TaskID &task_id) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// This function takes care of returning a worker to the Raylet.
  /// \param[in] addr The address of the worker.
  /// \param[in] was_error Whether the task failed to be submitted.
  /// \param[in] worker_exiting Whether the worker is exiting.
  void ReturnWorker(const rpc::WorkerAddress addr,
                    bool was_error,
                    bool worker_exiting,
                    const SchedulingKey &scheduling_key) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Check that the scheduling_key_entries_ hashmap is empty.
  inline bool CheckNoSchedulingKeyEntries() const EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return scheduling_key_entries_.empty();
  }

  /// Push a task to a specific worker.
  void PushNormalTask(const rpc::WorkerAddress &addr,
                      rpc::CoreWorkerClientInterface &client,
                      const SchedulingKey &task_queue_key,
                      const TaskSpecification &task_spec,
                      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry>
                          &assigned_resources);

  /// Handles result from GetTaskFailureCause.
  void HandleGetTaskFailureCause(
      const Status &task_execution_status,
      const bool is_actor,
      const TaskID &task_id,
      const rpc::WorkerAddress &addr,
      const Status &get_task_failure_cause_reply_status,
      const rpc::GetTaskFailureCauseReply &get_task_failure_cause_reply);

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  // Client that can be used to lease and return workers from the local raylet.
  std::shared_ptr<WorkerLeaseInterface> local_lease_client_;

  /// Cache of gRPC clients to remote raylets.
  absl::flat_hash_map<NodeID, std::shared_ptr<WorkerLeaseInterface>> remote_lease_clients_
      GUARDED_BY(mu_);

  /// Factory for producing new clients to request leases from remote nodes.
  LeaseClientFactoryFn lease_client_factory_;

  /// Provider of worker leasing decisions for the first lease request (not on
  /// spillback).
  std::shared_ptr<LeasePolicyInterface> lease_policy_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  std::shared_ptr<TaskFinisherInterface> task_finisher_;

  /// The timeout for worker leases; after this duration, workers will be returned
  /// to the raylet.
  int64_t lease_timeout_ms_;

  /// The local raylet ID. Used to make sure that we use the local lease client
  /// if a remote raylet tells us to spill the task back to the local raylet.
  const NodeID local_raylet_id_;

  /// The type of this core worker process.
  const WorkerType worker_type_;

  /// Interface for actor creation.
  std::shared_ptr<ActorCreatorInterface> actor_creator_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  std::shared_ptr<rpc::CoreWorkerClientPool> client_cache_;

  /// The ID of the job.
  const JobID job_id_;

  // Max number of pending lease requests per SchedulingKey.
  const uint64_t max_pending_lease_requests_per_scheduling_category_;

  /// A LeaseEntry struct is used to condense the metadata about a single executor:
  /// (1) The lease client through which the worker should be returned
  /// (2) The expiration time of a worker's lease.
  /// (3) Whether the worker has assigned task to do.
  /// (5) The resources assigned to the worker
  /// (6) The SchedulingKey assigned to tasks that will be sent to the worker
  /// (7) The task id used to obtain the worker lease.
  struct LeaseEntry {
    std::shared_ptr<WorkerLeaseInterface> lease_client;
    int64_t lease_expiration_time;
    bool is_busy = false;
    google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources;
    SchedulingKey scheduling_key;
    TaskID task_id;

    LeaseEntry(
        std::shared_ptr<WorkerLeaseInterface> lease_client = nullptr,
        int64_t lease_expiration_time = 0,
        google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources =
            google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry>(),
        SchedulingKey scheduling_key =
            std::make_tuple(0, std::vector<ObjectID>(), ActorID::Nil(), 0),
        TaskID task_id = TaskID::Nil())
        : lease_client(lease_client),
          lease_expiration_time(lease_expiration_time),
          assigned_resources(assigned_resources),
          scheduling_key(scheduling_key),
          task_id(task_id) {}
  };

  // Map from worker address to a LeaseEntry struct containing the lease's metadata.
  absl::flat_hash_map<rpc::WorkerAddress, LeaseEntry> worker_to_lease_entry_
      GUARDED_BY(mu_);

  struct SchedulingKeyEntry {
    // Keep track of pending worker lease requests to the raylet.
    absl::flat_hash_map<TaskID, rpc::Address> pending_lease_requests;
    TaskSpecification resource_spec = TaskSpecification();
    // Tasks that are queued for execution. We keep an individual queue per
    // scheduling class to ensure fairness.
    std::deque<TaskSpecification> task_queue = std::deque<TaskSpecification>();
    // Keep track of the active workers, so that we can quickly check if one of them has
    // room for more tasks in flight
    absl::flat_hash_set<rpc::WorkerAddress> active_workers =
        absl::flat_hash_set<rpc::WorkerAddress>();
    // Keep track of how many workers have tasks to do.
    uint32_t num_busy_workers = 0;
    int64_t last_reported_backlog_size = 0;

    // Check whether it's safe to delete this SchedulingKeyEntry from the
    // scheduling_key_entries_ hashmap.
    inline bool CanDelete() const {
      if (pending_lease_requests.empty() && task_queue.empty() &&
          active_workers.size() == 0 && num_busy_workers == 0) {
        return true;
      }

      return false;
    }

    // Check whether all workers are busy.
    inline bool AllWorkersBusy() const {
      RAY_CHECK_LE(num_busy_workers, active_workers.size());
      return num_busy_workers == active_workers.size();
    }

    // Get the current backlog size for this scheduling key
    [[nodiscard]] inline int64_t BacklogSize() const {
      if (task_queue.size() < pending_lease_requests.size()) {
        // This can happen if worker is reused.
        return 0;
      }

      // Subtract tasks with pending lease requests so we don't double count them.
      return task_queue.size() - pending_lease_requests.size();
    }
  };

  // For each Scheduling Key, scheduling_key_entries_ contains a SchedulingKeyEntry struct
  // with the queue of tasks belonging to that SchedulingKey, together with the other
  // fields that are needed to orchestrate the execution of those tasks by the workers.
  absl::flat_hash_map<SchedulingKey, SchedulingKeyEntry> scheduling_key_entries_
      GUARDED_BY(mu_);

  // Tasks that were cancelled while being resolved.
  absl::flat_hash_set<TaskID> cancelled_tasks_ GUARDED_BY(mu_);

  // Keeps track of where currently executing tasks are being run.
  absl::flat_hash_map<TaskID, rpc::WorkerAddress> executing_tasks_ GUARDED_BY(mu_);

  // Retries cancelation requests if they were not successful.
  absl::optional<boost::asio::steady_timer> cancel_retry_timer_;

  int64_t num_tasks_submitted_ = 0;
  int64_t num_leases_requested_ GUARDED_BY(mu_) = 0;
};

}  // namespace core
}  // namespace ray
