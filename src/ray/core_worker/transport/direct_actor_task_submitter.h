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

#include <boost/asio/thread_pool.hpp>
#include <boost/thread.hpp>
#include <list>
#include <queue>
#include <set>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace core {

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

// Interface for testing.
class CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  virtual void AddActorQueueIfNotExists(const ActorID &actor_id) = 0;
  virtual void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                            int64_t num_restarts) = 0;
  virtual void DisconnectActor(
      const ActorID &actor_id, int64_t num_restarts, bool dead,
      const std::shared_ptr<rpc::ActorDeathCause> &death_cause = nullptr) = 0;
  virtual void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) = 0;

  virtual void CheckTimeoutTasks() = 0;

  virtual ~CoreWorkerDirectActorTaskSubmitterInterface() {}
};

// This class is thread-safe.
class CoreWorkerDirectActorTaskSubmitter
    : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      rpc::CoreWorkerClientPool &core_worker_client_pool, CoreWorkerMemoryStore &store,
      TaskFinisherInterface &task_finisher, ActorCreatorInterface &actor_creator,
      std::function<void(const ActorID &, int64_t)> warn_excess_queueing)
      : core_worker_client_pool_(core_worker_client_pool),
        resolver_(store, task_finisher, actor_creator),
        task_finisher_(task_finisher),
        warn_excess_queueing_(warn_excess_queueing) {
    next_queueing_warn_threshold_ =
        ::RayConfig::instance().actor_excess_queueing_warn_threshold();
  }

  /// Add an actor queue. This should be called whenever a reference to an
  /// actor is created in the language frontend.
  /// TODO(swang): Remove the actor queue once it is sure that this worker will
  /// not receive another reference to the same actor.
  ///
  /// \param[in] actor_id The actor for whom to add a queue.
  void AddActorQueueIfNotExists(const ActorID &actor_id);

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status::Invalid if the task is not yet supported.
  Status SubmitTask(TaskSpecification task_spec);

  /// Tell this actor to exit immediately.
  ///
  /// \param[in] actor_id The actor_id of the actor to kill.
  /// \param[in] force_kill Whether to force kill the actor, or let the actor
  /// try a clean exit.
  /// \param[in] no_restart If set to true, the killed actor will not be
  /// restarted anymore.
  void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Create connection to actor and send all pending tasks.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] address The new address of the actor.
  /// \param[in] num_restarts How many times this actor has been restarted
  /// before. If we've already seen a later incarnation of the actor, we will
  /// ignore the command to connect.
  void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                    int64_t num_restarts);

  /// Disconnect from a failed actor.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] num_restarts How many times this actor has been restarted
  /// before. If we've already seen a later incarnation of the actor, we will
  /// ignore the command to connect.
  /// \param[in] dead Whether the actor is permanently dead. In this case, all
  /// pending tasks for the actor should be failed.
  /// \param[in] death_cause Context about why this actor is dead.
  void DisconnectActor(
      const ActorID &actor_id, int64_t num_restarts, bool dead,
      const std::shared_ptr<rpc::ActorDeathCause> &death_cause = nullptr);

  /// Set the timerstamp for the caller.
  void SetCallerCreationTimestamp(int64_t timestamp);

  /// Check timeout tasks that are waiting for Death info.
  void CheckTimeoutTasks();

 private:
  struct ClientQueue {
    /// The current state of the actor. If this is ALIVE, then we should have
    /// an RPC client to the actor. If this is DEAD, then all tasks in the
    /// queue will be marked failed and all other ClientQueue state is ignored.
    rpc::ActorTableData::ActorState state = rpc::ActorTableData::DEPENDENCIES_UNREADY;
    /// Only applies when state=DEAD.
    std::shared_ptr<rpc::ActorDeathCause> death_cause = nullptr;
    /// How many times this actor has been restarted before. Starts at -1 to
    /// indicate that the actor is not yet created. This is used to drop stale
    /// messages from the GCS.
    int64_t num_restarts = -1;
    /// The RPC client. We use shared_ptr to enable shared_from_this for
    /// pending client callbacks.
    std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client = nullptr;
    /// The intended worker ID of the actor.
    std::string worker_id = "";

    /// The actor's pending requests, ordered by the task number (see below
    /// diagram) in the request. The bool indicates whether the dependencies
    /// for that task have been resolved yet. A task will be sent after its
    /// dependencies have been resolved and its task number matches
    /// next_send_position.
    std::map<uint64_t, std::pair<TaskSpecification, bool>> requests;

    /// Diagram of the sequence numbers assigned to actor tasks during actor
    /// crash and restart:
    ///
    /// The actor starts, and 10 tasks are submitted. We have sent 6 tasks
    /// (0-5) so far, and have received a successful reply for 4 tasks (0-3).
    /// 0 1 2 3 4 5 6 7 8 9
    ///             ^ next_send_position
    ///         ^ next_task_reply_position
    /// ^ caller_starts_at
    ///
    /// Suppose the actor crashes and recovers. Then, caller_starts_at is reset
    /// to the current next_task_reply_position. caller_starts_at is then subtracted
    /// from each task's counter, so the recovered actor will receive the
    /// sequence numbers 0, 1, 2 (and so on) for tasks 4, 5, 6, respectively.
    /// Therefore, the recovered actor will restart execution from task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///             ^ next_send_position
    ///         ^ next_task_reply_position
    ///         ^ caller_starts_at
    ///
    /// New actor tasks will continue to be sent even while tasks are being
    /// resubmitted, but the receiving worker will only execute them after the
    /// resent tasks. For example, this diagram shows what happens if task 6 is
    /// sent for the first time, tasks 4 and 5 have been resent, and we have
    /// received a successful reply for task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///               ^ next_send_position
    ///           ^ next_task_reply_position
    ///         ^ caller_starts_at
    ///
    /// The send position of the next task to send to this actor. This sequence
    /// number increases monotonically.
    uint64_t next_send_position = 0;
    /// The offset at which the the actor should start its counter for this
    /// caller. This is used for actors that can be restarted, so that the new
    /// instance of the actor knows from which task to start executing.
    uint64_t caller_starts_at = 0;
    /// Out of the tasks sent by this worker to the actor, the number of tasks
    /// that we will never send to the actor again. This is used to reset
    /// caller_starts_at if the actor dies and is restarted. We only include
    /// tasks that will not be sent again, to support automatic task retry on
    /// actor failure. This value only tracks consecutive tasks that are completed.
    /// Tasks completed out of order will be cached in out_of_completed_tasks first.
    uint64_t next_task_reply_position = 0;

    /// The temporary container for tasks completed out of order. It can happen in
    /// async or threaded actor mode. This map is used to store the seqno and task
    /// spec for (1) increment next_task_reply_position later when the in order tasks are
    /// returned (2) resend the tasks to restarted actor so retried tasks can maintain
    /// ordering.
    // NOTE(simon): consider absl::btree_set for performance, but it requires updating
    // abseil.
    std::map<uint64_t, TaskSpecification> out_of_order_completed_tasks;

    /// Tasks that can't be sent because 1) the callee actor is dead. 2) network error.
    /// For 1) the task will wait for the DEAD state notification, then mark task as
    /// failed using the death_info in notification. For 2) we'll never receive a DEAD
    /// notification, in this case we'll wait for a fixed timeout value and then mark it
    /// as failed.
    /// pair key: timestamp in ms when this task should be considered as timeout.
    /// pair value: task specification
    std::deque<std::pair<int64_t, TaskSpecification>> wait_for_death_info_tasks;

    /// A force-kill request that should be sent to the actor once an RPC
    /// client to the actor is available.
    absl::optional<rpc::KillActorRequest> pending_force_kill;

    /// Stores all callbacks of inflight tasks. Note that this doesn't include tasks
    /// without replies.
    std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
        inflight_task_callbacks;
  };

  /// Push a task to a remote actor via the given client.
  /// Note, this function doesn't return any error status code. If an error occurs while
  /// sending the request, this task will be treated as failed.
  ///
  /// \param[in] queue The actor queue. Contains the RPC client state.
  /// \param[in] task_spec The task to send.
  /// \param[in] skip_queue Whether to skip the task queue. This will send the
  /// task for execution immediately.
  /// \return Void.
  void PushActorTask(ClientQueue &queue, const TaskSpecification &task_spec,
                     bool skip_queue) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Send all pending tasks for an actor.
  ///
  /// \param[in] actor_id Actor ID.
  /// \return Void.
  void SendPendingTasks(const ActorID &actor_id) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Resend all previously-received, out-of-order, received tasks for an actor.
  /// When sending these tasks, the tasks will have the flag skip_execution=true.
  ///
  /// \param[in] actor_id Actor ID.
  /// \return Void.
  void ResendOutOfOrderTasks(const ActorID &actor_id) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Disconnect the RPC client for an actor.
  void DisconnectRpcClient(ClientQueue &queue) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Fail all in-flight tasks.
  void FailInflightTasks(
      const std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
          &inflight_task_callbacks) LOCKS_EXCLUDED(mu_);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id) const;

  /// Pool for producing new core worker clients.
  rpc::CoreWorkerClientPool &core_worker_client_pool_;

  /// Mutex to protect the various maps below.
  mutable absl::Mutex mu_;

  absl::flat_hash_map<ActorID, ClientQueue> client_queues_ GUARDED_BY(mu_);

  /// Resolve direct call object dependencies.
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  TaskFinisherInterface &task_finisher_;

  /// Used to warn of excessive queueing.
  std::function<void(const ActorID &, int64_t num_queued)> warn_excess_queueing_;

  /// Warn the next time the number of queued task submissions to an actor
  /// exceeds this quantity. This threshold is doubled each time it is hit.
  int64_t next_queueing_warn_threshold_;

  friend class CoreWorkerTest;
};

}  // namespace core
}  // namespace ray
