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

#include <deque>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "ray/common/id.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager_interface.h"
#include "ray/core_worker/task_submission/dependency_resolver.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"

namespace ray {
namespace core {

// The task queues are keyed on resource shape & function descriptor
// (encapsulated in SchedulingClass) to defer resource allocation decisions to the raylet
// and ensure fairness between different tasks, as well as plasma task dependencies as
// a performance optimization because the raylet will fetch plasma dependencies to the
// scheduled worker. It is also keyed on RuntimeEnvHash, because a worker can only run a
// task if the worker's RuntimeEnvHash matches the RuntimeEnvHash required by the task
// spec.
using RuntimeEnvHash = int;
using SchedulingKey = std::tuple<SchedulingClass, std::vector<ObjectID>, RuntimeEnvHash>;

// Interface that controls the max concurrent pending lease requests
// per scheduling category.
class LeaseRequestRateLimiter {
 public:
  virtual size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() = 0;
  virtual ~LeaseRequestRateLimiter() = default;
};

// Lease request rate-limiter with fixed number.
class StaticLeaseRequestRateLimiter : public LeaseRequestRateLimiter {
 public:
  explicit StaticLeaseRequestRateLimiter(size_t limit) : kLimit(limit) {}
  size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() override { return kLimit; }

 private:
  const size_t kLimit;
};

// Lease request rate-limiter based on cluster node size.
// It returns max(num_nodes_in_cluster, min_concurrent_lease_limit)
class ClusterSizeBasedLeaseRequestRateLimiter : public LeaseRequestRateLimiter {
 public:
  explicit ClusterSizeBasedLeaseRequestRateLimiter(size_t min_concurrent_lease_limit);
  size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() override;
  void OnNodeChanges(const rpc::GcsNodeAddressAndLiveness &data);

 private:
  const size_t min_concurrent_lease_cap_;
  std::atomic<size_t> num_alive_nodes_;
};

// This class is thread-safe.
class NormalTaskSubmitter {
 public:
  explicit NormalTaskSubmitter(
      rpc::Address rpc_address,
      std::shared_ptr<RayletClientInterface> local_raylet_client,
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
      std::shared_ptr<rpc::RayletClientPool> raylet_client_pool,
      std::unique_ptr<LeasePolicyInterface> lease_policy,
      std::shared_ptr<CoreWorkerMemoryStore> store,
      TaskManagerInterface &task_manager,
      NodeID local_node_id,
      WorkerType worker_type,
      int64_t lease_timeout_ms,
      std::shared_ptr<ActorCreatorInterface> actor_creator,
      const JobID &job_id,
      std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter,
      const TensorTransportGetter &tensor_transport_getter,
      boost::asio::steady_timer cancel_timer,
      ray::observability::MetricInterface &scheduler_placement_time_ms_histogram)
      : rpc_address_(std::move(rpc_address)),
        local_raylet_client_(std::move(local_raylet_client)),
        raylet_client_pool_(std::move(raylet_client_pool)),
        lease_policy_(std::move(lease_policy)),
        resolver_(*store, task_manager, *actor_creator, tensor_transport_getter),
        task_manager_(task_manager),
        lease_timeout_ms_(lease_timeout_ms),
        local_node_id_(local_node_id),
        worker_id_(WorkerID::FromBinary(rpc_address_.worker_id())),
        worker_type_(worker_type),
        core_worker_client_pool_(std::move(core_worker_client_pool)),
        job_id_(job_id),
        lease_request_rate_limiter_(std::move(lease_request_rate_limiter)),
        cancel_retry_timer_(std::move(cancel_timer)),
        scheduler_placement_time_ms_histogram_(scheduler_placement_time_ms_histogram) {}

  /// Schedule a task for direct submission to a worker.
  void SubmitTask(TaskSpecification task_spec);

  /// Either remove a pending task or send an RPC to kill a running task
  ///
  /// \param[in] task_spec The task to kill.
  /// \param[in] force_kill Whether to kill the worker executing the task.
  void CancelTask(TaskSpecification task_spec, bool force_kill, bool recursive);

  /// Request the owner of the object ID to cancel a request.
  /// It is used when a object ID is not owned by the current process.
  /// We cannot cancel the task in this case because we don't have enough
  /// information to cancel a task.
  void CancelRemoteTask(const ObjectID &object_id,
                        const rpc::Address &worker_addr,
                        bool force_kill,
                        bool recursive);

  /// Queue the streaming generator up for resubmission.
  /// \return true if the task is still executing and the submitter agrees to resubmit
  /// when it finishes. false if the user cancelled the task.
  bool QueueGeneratorForResubmit(const TaskSpecification &spec);

  /// Check that the scheduling_key_entries_ hashmap is empty by calling the private
  /// CheckNoSchedulingKeyEntries function after acquiring the lock.
  bool CheckNoSchedulingKeyEntriesPublic() {
    absl::MutexLock lock(&mu_);
    return scheduling_key_entries_.empty();
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
  /// \param[in] error_detail The reason why it was errored.
  /// It is unused if was_error is false.
  /// \param[in] worker_exiting Whether the worker is exiting.
  /// \param[in] assigned_resources Resource ids previously assigned to the worker.
  void OnWorkerIdle(
      const rpc::Address &addr,
      const SchedulingKey &task_queue_key,
      bool was_error,
      const std::string &error_detail,
      bool worker_exiting,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Report worker backlog information to the local raylet
  void ReportWorkerBacklogInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Report backlog if the backlog size is changed for this scheduling key
  /// since last report
  void ReportWorkerBacklogIfNeeded(const SchedulingKey &scheduling_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Request a new worker from the raylet if no such requests are currently in
  /// flight and there are tasks queued. If a raylet address is provided, then
  /// the worker should be requested from the raylet at that address. Else, the
  /// worker should be requested from the local raylet.
  void RequestNewWorkerIfNeeded(const SchedulingKey &task_queue_key,
                                const rpc::Address *raylet_address = nullptr)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Cancel a pending worker lease and retry until the cancellation succeeds
  /// (i.e., the raylet drops the request). This should be called when there
  /// are no more tasks queued with the given scheduling key and there is an
  /// in-flight lease request for that key.
  void CancelWorkerLeaseIfNeeded(const SchedulingKey &scheduling_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Set up client state for newly granted worker lease.
  void AddWorkerLeaseClient(
      const rpc::Address &worker_address,
      const rpc::Address &raylet_address,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
      const SchedulingKey &scheduling_key,
      const LeaseID &lease_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// This function takes care of returning a worker to the Raylet.
  /// \param[in] addr The address of the worker.
  /// \param[in] was_error Whether the task failed to be submitted.
  /// \param[in] error_detail The reason why it was errored.
  /// it is unused if was_error is false.
  /// \param[in] worker_exiting Whether the worker is exiting.
  void ReturnWorkerLease(const rpc::Address &addr,
                         bool was_error,
                         const std::string &error_detail,
                         bool worker_exiting,
                         const SchedulingKey &scheduling_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Check that the scheduling_key_entries_ hashmap is empty.
  bool CheckNoSchedulingKeyEntries() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return scheduling_key_entries_.empty();
  }

  /// Push a task to a specific worker.
  void PushNormalTask(const rpc::Address &addr,
                      std::shared_ptr<rpc::CoreWorkerClientInterface> client,
                      const SchedulingKey &task_queue_key,
                      TaskSpecification task_spec,
                      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry>
                          &assigned_resources);

  /// Handles result from GetWorkerFailureCause.
  /// \return true if the task executing on the worker should be retried, false otherwise.
  bool HandleGetWorkerFailureCause(
      const Status &task_execution_status,
      const TaskID &task_id,
      const rpc::Address &addr,
      const Status &get_worker_failure_cause_reply_status,
      const rpc::GetWorkerFailureCauseReply &get_worker_failure_cause_reply);

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  /// Client that can be used to lease and return workers from the local raylet.
  std::shared_ptr<RayletClientInterface> local_raylet_client_;

  /// Raylet client pool for producing new clients to request leases from remote nodes.
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;

  /// Provider of worker leasing decisions for the first lease request (not on
  /// spillback).
  std::unique_ptr<LeasePolicyInterface> lease_policy_;

  /// Resolve local and remote dependencies;
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  TaskManagerInterface &task_manager_;

  /// The timeout for worker leases; after this duration, workers will be returned
  /// to the raylet.
  int64_t lease_timeout_ms_;

  /// The local node ID. Used to make sure that we use the local lease client
  /// if a remote raylet tells us to spill the task back to the local raylet.
  const NodeID local_node_id_;

  /// The local worker ID.
  const WorkerID worker_id_;

  /// The type of this core worker process.
  const WorkerType worker_type_;

  // Protects task submission state below.
  absl::Mutex mu_;

  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;

  /// The ID of the job.
  const JobID job_id_;

  /// A LeaseEntry struct is used to condense the metadata about a single executor:
  /// (1) The address of the raylet that leased the worker.
  /// (2) The expiration time of a worker's lease.
  /// (3) Whether the worker has assigned task to do.
  /// (4) The resources assigned to the worker
  /// (5) The SchedulingKey assigned to tasks that will be sent to the worker
  /// (6) The task id used to obtain the worker lease.
  struct LeaseEntry {
    rpc::Address addr;
    int64_t lease_expiration_time;
    google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources;
    SchedulingKey scheduling_key;
    LeaseID lease_id;
    bool is_busy = false;
  };

  // Map from worker address to a LeaseEntry struct containing the lease's metadata.
  absl::flat_hash_map<rpc::Address, LeaseEntry> worker_to_lease_entry_
      ABSL_GUARDED_BY(mu_);

  struct SchedulingKeyEntry {
    // Keep track of pending worker lease requests to the raylet.
    absl::flat_hash_map<LeaseID, rpc::Address> pending_lease_requests;

    LeaseSpecification lease_spec;
    // Tasks that are queued for execution. We keep an individual queue per
    // scheduling class to ensure fairness.
    std::deque<TaskSpecification> task_queue;
    // Keep track of the active workers, so that we can quickly check if one of them has
    // room for more tasks in flight
    absl::flat_hash_set<rpc::Address> active_workers;
    // Keep track of how many workers have tasks to do.
    uint32_t num_busy_workers = 0;
    int64_t last_reported_backlog_size = 0;

    // Check whether it's safe to delete this SchedulingKeyEntry from the
    // scheduling_key_entries_ hashmap.
    bool CanDelete() const {
      if (pending_lease_requests.empty() && task_queue.empty() &&
          active_workers.size() == 0 && num_busy_workers == 0) {
        return true;
      }

      return false;
    }

    // Check whether all workers are busy.
    bool AllWorkersBusy() const {
      RAY_CHECK_LE(num_busy_workers, active_workers.size());
      return num_busy_workers == active_workers.size();
    }

    // Get the current backlog size for this scheduling key
    int64_t BacklogSize() const {
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
      ABSL_GUARDED_BY(mu_);

  // Tasks that were cancelled while being resolved.
  absl::flat_hash_set<TaskID> cancelled_tasks_ ABSL_GUARDED_BY(mu_);

  // Keeps track of where currently executing tasks are being run.
  absl::flat_hash_map<TaskID, rpc::Address> executing_tasks_ ABSL_GUARDED_BY(mu_);

  // Generators that are currently running and need to be resubmitted.
  absl::flat_hash_set<TaskID> generators_to_resubmit_ ABSL_GUARDED_BY(mu_);

  // Tasks that have failed but we are waiting for their error cause to decide if they
  // should be retried or permanently failed.
  absl::flat_hash_set<TaskID> failed_tasks_pending_failure_cause_ ABSL_GUARDED_BY(mu_);

  // Ratelimiter controls the num of pending lease requests.
  std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter_;

  // Retries cancelation requests if they were not successful.
  boost::asio::steady_timer cancel_retry_timer_ ABSL_GUARDED_BY(mu_);

  ray::observability::MetricInterface &scheduler_placement_time_ms_histogram_;
};

}  // namespace core
}  // namespace ray
