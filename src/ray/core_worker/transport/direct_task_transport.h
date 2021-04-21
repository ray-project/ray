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
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
      LeaseClientFactoryFn lease_client_factory,
      std::shared_ptr<LeasePolicyInterface> lease_policy,
      std::shared_ptr<CoreWorkerMemoryStore> store,
      std::shared_ptr<TaskFinisherInterface> task_finisher, NodeID local_raylet_id,
      int64_t lease_timeout_ms, std::shared_ptr<ActorCreatorInterface> actor_creator,
      uint32_t max_tasks_in_flight_per_worker =
          RayConfig::instance().max_tasks_in_flight_per_worker(),
      bool work_stealing = RayConfig::instance().work_stealing(),
      absl::optional<boost::asio::steady_timer> cancel_timer = absl::nullopt)
      : rpc_address_(rpc_address),
        local_lease_client_(lease_client),
        lease_client_factory_(lease_client_factory),
        lease_policy_(std::move(lease_policy)),
        resolver_(store, task_finisher),
        task_finisher_(task_finisher),
        lease_timeout_ms_(lease_timeout_ms),
        local_raylet_id_(local_raylet_id),
        actor_creator_(std::move(actor_creator)),
        client_cache_(core_worker_client_pool),
        max_tasks_in_flight_per_worker_(max_tasks_in_flight_per_worker),
        work_stealing_(work_stealing),
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

  Status CancelRemoteTask(const ObjectID &object_id, const rpc::Address &worker_addr,
                          bool force_kill, bool recursive);
  /// Check that the scheduling_key_entries_ hashmap is empty by calling the private
  /// CheckNoSchedulingKeyEntries function after acquiring the lock.
  bool CheckNoSchedulingKeyEntriesPublic() {
    absl::MutexLock lock(&mu_);
    return scheduling_key_entries_.empty();
  }

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
  void AddWorkerLeaseClient(
      const rpc::WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
      const SchedulingKey &scheduling_key) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// This function takes care of returning a worker to the Raylet.
  /// \param[in] addr The address of the worker.
  /// \param[in] was_error Whether the task failed to be submitted.
  void ReturnWorker(const rpc::WorkerAddress addr, bool was_error,
                    const SchedulingKey &scheduling_key) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Check that the scheduling_key_entries_ hashmap is empty.
  inline bool CheckNoSchedulingKeyEntries() const EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return scheduling_key_entries_.empty();
  }

  /// Find the optimal victim (if there is any) for stealing work
  ///
  /// \param[in] scheduling_key The SchedulingKey of the thief.
  /// \param[in] victim_addr The pointer to a variable that the function will fill with
  /// the address of the victim, if one is found \param[out] A boolean indicating whether
  /// we found a suitable victim or not
  bool FindOptimalVictimForStealing(const SchedulingKey &scheduling_key,
                                    rpc::WorkerAddress thief_addr,
                                    rpc::Address *victim_raw_addr)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Look for workers with a surplus of tasks in flight, and, if it is possible,
  /// steal some of those tasks and submit them to the current worker. If no tasks
  /// are available for stealing, return the worker to the Raylet.
  ///
  /// \param[in] thief_addr The address of the worker that has finished its own work,
  ///                       and is ready for stealing.
  /// \param[in] was_error Whether the last task failed to be submitted to the worker.
  /// \param[in] scheduling_key The scheduling class of the worker.
  /// \param[in] assigned_resources Resource ids previously assigned to the worker.
  void StealTasksIfNeeded(
      const rpc::WorkerAddress &thief_addr, bool was_error,
      const SchedulingKey &scheduling_key,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources)
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

  /// Interface for actor creation.
  std::shared_ptr<ActorCreatorInterface> actor_creator_;

  // Protects task submission state below.
  absl::Mutex mu_;

  /// Cache of gRPC clients to other workers.
  std::shared_ptr<rpc::CoreWorkerClientPool> client_cache_;

  // max_tasks_in_flight_per_worker_ limits the number of tasks that can be pipelined to a
  // worker using a single lease.
  const uint32_t max_tasks_in_flight_per_worker_;

  // work_stealing_ indicates whether the work stealing mode is enabled
  const bool work_stealing_;

  /// A LeaseEntry struct is used to condense the metadata about a single executor:
  /// (1) The lease client through which the worker should be returned
  /// (2) The expiration time of a worker's lease.
  /// (3) The number of tasks that are currently in flight to the worker
  /// (4) A boolean that indicates whether we have launched a StealTasks request, and we
  /// are waiting for the stolen tasks (5) The resources assigned to the worker (6) The
  /// SchedulingKey assigned to tasks that will be sent to the worker
  struct LeaseEntry {
    std::shared_ptr<WorkerLeaseInterface> lease_client;
    int64_t lease_expiration_time;
    uint32_t tasks_in_flight;
    bool currently_stealing;
    google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources;
    SchedulingKey scheduling_key;

    LeaseEntry(
        std::shared_ptr<WorkerLeaseInterface> lease_client = nullptr,
        int64_t lease_expiration_time = 0, uint32_t tasks_in_flight = 0,
        bool currently_stealing = false, int64_t stolen_tasks_to_wait_for = 0,
        google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources =
            google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry>(),
        SchedulingKey scheduling_key = std::make_tuple(0, std::vector<ObjectID>(),
                                                       ActorID::Nil()))
        : lease_client(lease_client),
          lease_expiration_time(lease_expiration_time),
          tasks_in_flight(tasks_in_flight),
          currently_stealing(currently_stealing),
          assigned_resources(assigned_resources),
          scheduling_key(scheduling_key) {}

    // Check whether the pipeline to the worker associated with a LeaseEntry is full.
    inline bool PipelineToWorkerFull(uint32_t max_tasks_in_flight_per_worker) const {
      return tasks_in_flight == max_tasks_in_flight_per_worker;
    }

    // Check whether the worker is a thief who is in the process of stealing tasks.
    // Knowing whether a thief is currently stealing is important to prevent the thief
    // from initiating another StealTasks request or from being returned to the raylet
    // until stealing has completed.
    inline bool WorkerIsStealing() const { return currently_stealing; }

    // Once stealing has begun, updated the thief's currently_stealing flag to reflect the
    // new state.
    inline void SetWorkerIsStealing() {
      RAY_CHECK(!currently_stealing);
      currently_stealing = true;
    }

    // Once stealing has completed, updated the thief's currently_stealing flag to reflect
    // the new state.
    inline void SetWorkerDoneStealing() {
      RAY_CHECK(currently_stealing);
      currently_stealing = false;
    }
  };

  // Map from worker address to a LeaseEntry struct containing the lease's metadata.
  absl::flat_hash_map<rpc::WorkerAddress, LeaseEntry> worker_to_lease_entry_
      GUARDED_BY(mu_);

  struct SchedulingKeyEntry {
    // Keep track of pending worker lease requests to the raylet.
    std::pair<std::shared_ptr<WorkerLeaseInterface>, TaskID> pending_lease_request =
        std::make_pair(nullptr, TaskID::Nil());
    TaskSpecification resource_spec = TaskSpecification();
    // Tasks that are queued for execution. We keep an individual queue per
    // scheduling class to ensure fairness.
    std::deque<TaskSpecification> task_queue = std::deque<TaskSpecification>();
    // Keep track of the active workers, so that we can quickly check if one of them has
    // room for more tasks in flight
    absl::flat_hash_set<rpc::WorkerAddress> active_workers =
        absl::flat_hash_set<rpc::WorkerAddress>();
    // Keep track of how many tasks with this SchedulingKey are in flight, in total
    uint32_t total_tasks_in_flight = 0;

    // Check whether it's safe to delete this SchedulingKeyEntry from the
    // scheduling_key_entries_ hashmap.
    inline bool CanDelete() const {
      if (!pending_lease_request.first && task_queue.empty() &&
          active_workers.size() == 0 && total_tasks_in_flight == 0) {
        return true;
      }

      return false;
    }

    // Check whether the pipelines to the active workers associated with a
    // SchedulingKeyEntry are all full.
    inline bool AllPipelinesToWorkersFull(uint32_t max_tasks_in_flight_per_worker) const {
      return total_tasks_in_flight ==
             (active_workers.size() * max_tasks_in_flight_per_worker);
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
};

};  // namespace ray
