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
#include "ray/core_worker/transport/actor_submit_queue.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/out_of_order_actor_submit_queue.h"
#include "ray/core_worker/transport/sequential_actor_submit_queue.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace core {

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

// Interface for testing.
class CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  virtual void AddActorQueueIfNotExists(const ActorID &actor_id,
                                        int32_t max_pending_calls,
                                        bool execute_out_of_order = false) = 0;
  virtual void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                            int64_t num_restarts) = 0;
  virtual void DisconnectActor(const ActorID &actor_id, int64_t num_restarts, bool dead,
                               const rpc::ActorDeathCause &death_cause) = 0;
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
      std::function<void(const ActorID &, int64_t)> warn_excess_queueing,
      instrumented_io_context &io_service)
      : core_worker_client_pool_(core_worker_client_pool),
        resolver_(store, task_finisher, actor_creator),
        task_finisher_(task_finisher),
        warn_excess_queueing_(warn_excess_queueing),
        io_service_(io_service) {
    next_queueing_warn_threshold_ =
        ::RayConfig::instance().actor_excess_queueing_warn_threshold();
  }

  /// Add an actor queue. This should be called whenever a reference to an
  /// actor is created in the language frontend.
  /// TODO(swang): Remove the actor queue once it is sure that this worker will
  /// not receive another reference to the same actor.
  ///
  /// \param[in] actor_id The actor for whom to add a queue.
  /// \param[in] max_pending_calls The max pending calls for the actor to be added.
  void AddActorQueueIfNotExists(const ActorID &actor_id, int32_t max_pending_calls,
                                bool execute_out_of_order = false);

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] task_spec The task spec to submit.
  ///
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
  void DisconnectActor(const ActorID &actor_id, int64_t num_restarts, bool dead,
                       const rpc::ActorDeathCause &death_cause);

  /// Set the timerstamp for the caller.
  void SetCallerCreationTimestamp(int64_t timestamp);

  /// Check timeout tasks that are waiting for Death info.
  void CheckTimeoutTasks();

  /// If the the number of tasks in requests is greater than or equal to
  /// max_pending_calls.
  ///
  /// \param[in] actor_id Actor id.
  /// \return Whether the corresponding client queue is full or not.
  bool PendingTasksFull(const ActorID &actor_id) const;

  /// Returns debug string for class.
  ///
  /// \param[in] actor_id The actor whose debug string to return.
  /// \return string.
  std::string DebugString(const ActorID &actor_id) const;

 private:
  struct ClientQueue {
    ClientQueue(ActorID actor_id, bool execute_out_of_order, int32_t max_pending_calls)
        : max_pending_calls(max_pending_calls) {
      if (execute_out_of_order) {
        actor_submit_queue = std::make_unique<OutofOrderActorSubmitQueue>(actor_id);
      } else {
        actor_submit_queue = std::make_unique<SequentialActorSubmitQueue>(actor_id);
      }
    }

    /// The current state of the actor. If this is ALIVE, then we should have
    /// an RPC client to the actor. If this is DEAD, then all tasks in the
    /// queue will be marked failed and all other ClientQueue state is ignored.
    rpc::ActorTableData::ActorState state = rpc::ActorTableData::DEPENDENCIES_UNREADY;
    /// The reason why this actor is dead.
    /// If the context is not set, it means the actor is not dead.
    rpc::ActorDeathCause death_cause;
    /// How many times this actor has been restarted before. Starts at -1 to
    /// indicate that the actor is not yet created. This is used to drop stale
    /// messages from the GCS.
    int64_t num_restarts = -1;
    /// The RPC client. We use shared_ptr to enable shared_from_this for
    /// pending client callbacks.
    std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client = nullptr;
    /// The intended worker ID of the actor.
    std::string worker_id = "";

    /// The queue that orders actor requests.
    std::unique_ptr<IActorSubmitQueue> actor_submit_queue;

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

    /// The max number limit of task capacity used for back pressure.
    /// If the number of tasks in requests >= max_pending_calls, it can't continue to
    /// push task to ClientQueue.
    const int32_t max_pending_calls;

    /// The current task number in this client queue.
    int32_t cur_pending_calls = 0;

    /// Returns debug string for class.
    ///
    /// \return string.
    std::string DebugString() const {
      std::ostringstream stream;
      stream << "max_pending_calls=" << max_pending_calls
             << " cur_pending_calls=" << cur_pending_calls;
      return stream.str();
    }
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

  /// The event loop where the actor task events are handled.
  instrumented_io_context &io_service_;

  friend class CoreWorkerTest;
};

}  // namespace core
}  // namespace ray
