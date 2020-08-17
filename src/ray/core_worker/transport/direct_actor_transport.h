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
#include "ray/core_worker/context.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace {}  // namespace

namespace ray {

/// The max time to wait for out-of-order tasks.
const int kMaxReorderWaitSeconds = 30;

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

// Interface for testing.
class CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  virtual void AddActorQueueIfNotExists(const ActorID &actor_id) = 0;
  virtual void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                            int64_t num_restarts) = 0;
  virtual void DisconnectActor(const ActorID &actor_id, int64_t num_restarts,
                               bool dead = false) = 0;
  virtual void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) = 0;

  virtual ~CoreWorkerDirectActorTaskSubmitterInterface() {}
};

// This class is thread-safe.
class CoreWorkerDirectActorTaskSubmitter
    : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  CoreWorkerDirectActorTaskSubmitter(rpc::ClientFactoryFn client_factory,
                                     std::shared_ptr<CoreWorkerMemoryStore> store,
                                     std::shared_ptr<TaskFinisherInterface> task_finisher)
      : client_factory_(client_factory),
        resolver_(store, task_finisher),
        task_finisher_(task_finisher) {}

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
  void DisconnectActor(const ActorID &actor_id, int64_t num_restarts, bool dead = false);

  /// Set the timerstamp for the caller.
  void SetCallerCreationTimestamp(int64_t timestamp);

 private:
  struct ClientQueue {
    /// The current state of the actor. If this is ALIVE, then we should have
    /// an RPC client to the actor. If this is DEAD, then all tasks in the
    /// queue will be marked failed and all other ClientQueue state is ignored.
    rpc::ActorTableData::ActorState state = rpc::ActorTableData::DEPENDENCIES_UNREADY;
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
    ///         ^ num_completed_tasks
    /// ^ caller_starts_at
    ///
    /// Suppose the actor crashes and recovers. Then, caller_starts_at is reset
    /// to the current num_completed_tasks. caller_starts_at is then subtracted
    /// from each task's counter, so the recovered actor will receive the
    /// sequence numbers 0, 1, 2 (and so on) for tasks 4, 5, 6, respectively.
    /// Therefore, the recovered actor will restart execution from task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///             ^ next_send_position
    ///         ^ num_completed_tasks
    ///         ^ caller_starts_at
    ///
    /// New actor tasks will continue to be sent even while tasks are being
    /// resubmitted, but the receiving worker will only execute them after the
    /// resent tasks. For example, this diagram shows what happens if task 6 is
    /// sent for the first time, tasks 4 and 5 have been resent, and we have
    /// received a successful reply for task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///               ^ next_send_position
    ///           ^ num_completed_tasks
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
    /// actor failure.
    uint64_t num_completed_tasks = 0;

    /// A force-kill request that should be sent to the actor once an RPC
    /// client to the actor is available.
    absl::optional<rpc::KillActorRequest> pending_force_kill;
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
  void PushActorTask(const ClientQueue &queue, const TaskSpecification &task_spec,
                     bool skip_queue) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Send all pending tasks for an actor.
  /// Note that this function doesn't take lock, the caller is expected to hold
  /// `mutex_` before calling this function.
  ///
  /// \param[in] actor_id Actor ID.
  /// \return Void.
  void SendPendingTasks(const ActorID &actor_id) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Disconnect the RPC client for an actor.
  void DisconnectRpcClient(ClientQueue &queue) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id) const;

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Mutex to protect the various maps below.
  mutable absl::Mutex mu_;

  absl::flat_hash_map<ActorID, ClientQueue> client_queues_ GUARDED_BY(mu_);

  /// Resolve direct call object dependencies;
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  std::shared_ptr<TaskFinisherInterface> task_finisher_;

  friend class CoreWorkerTest;
};

/// Object dependency and RPC state of an inbound request.
class InboundRequest {
 public:
  InboundRequest(){};
  InboundRequest(std::function<void()> accept_callback,
                 std::function<void()> reject_callback, bool has_dependencies)
      : accept_callback_(accept_callback),
        reject_callback_(reject_callback),
        has_pending_dependencies_(has_dependencies) {}

  void Accept() { accept_callback_(); }
  void Cancel() { reject_callback_(); }
  bool CanExecute() const { return !has_pending_dependencies_; }
  void MarkDependenciesSatisfied() { has_pending_dependencies_ = false; }

 private:
  std::function<void()> accept_callback_;
  std::function<void()> reject_callback_;
  bool has_pending_dependencies_;
};

/// Waits for an object dependency to become available. Abstract for testing.
class DependencyWaiter {
 public:
  /// Calls `callback` once the specified objects become available.
  virtual void Wait(const std::vector<rpc::ObjectReference> &dependencies,
                    std::function<void()> on_dependencies_available) = 0;

  virtual ~DependencyWaiter(){};
};

class DependencyWaiterImpl : public DependencyWaiter {
 public:
  DependencyWaiterImpl(DependencyWaiterInterface &dependency_client)
      : dependency_client_(dependency_client) {}

  void Wait(const std::vector<rpc::ObjectReference> &dependencies,
            std::function<void()> on_dependencies_available) override {
    auto tag = next_request_id_++;
    requests_[tag] = on_dependencies_available;
    RAY_CHECK_OK(dependency_client_.WaitForDirectActorCallArgs(dependencies, tag));
  }

  /// Fulfills the callback stored by Wait().
  void OnWaitComplete(int64_t tag) {
    auto it = requests_.find(tag);
    RAY_CHECK(it != requests_.end());
    it->second();
    requests_.erase(it);
  }

 private:
  int64_t next_request_id_ = 0;
  std::unordered_map<int64_t, std::function<void()>> requests_;
  DependencyWaiterInterface &dependency_client_;
};

/// Wraps a thread-pool to block posts until the pool has free slots. This is used
/// by the SchedulingQueue to provide backpressure to clients.
class BoundedExecutor {
 public:
  BoundedExecutor(int max_concurrency)
      : num_running_(0), max_concurrency_(max_concurrency), pool_(max_concurrency){};

  /// Posts work to the pool, blocking if no free threads are available.
  void PostBlocking(std::function<void()> fn) {
    mu_.LockWhen(absl::Condition(this, &BoundedExecutor::ThreadsAvailable));
    num_running_ += 1;
    mu_.Unlock();
    boost::asio::post(pool_, [this, fn]() {
      fn();
      absl::MutexLock lock(&mu_);
      num_running_ -= 1;
    });
  }

 private:
  bool ThreadsAvailable() EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return num_running_ < max_concurrency_;
  }

  /// Protects access to the counters below.
  absl::Mutex mu_;
  /// The number of currently running tasks.
  int num_running_ GUARDED_BY(mu_);
  /// The max number of concurrently running tasks allowed.
  const int max_concurrency_;
  /// The underlying thread pool for running tasks.
  boost::asio::thread_pool pool_;
};

/// Used to ensure serial order of task execution per actor handle.
/// See direct_actor.proto for a description of the ordering protocol.
class SchedulingQueue {
 public:
  SchedulingQueue(boost::asio::io_service &main_io_service, DependencyWaiter &waiter,
                  WorkerContext &worker_context,
                  int64_t reorder_wait_seconds = kMaxReorderWaitSeconds)
      : worker_context_(worker_context),
        reorder_wait_seconds_(reorder_wait_seconds),
        wait_timer_(main_io_service),
        main_thread_id_(boost::this_thread::get_id()),
        waiter_(waiter) {}

  void Add(int64_t seq_no, int64_t client_processed_up_to,
           std::function<void()> accept_request, std::function<void()> reject_request,
           const std::vector<rpc::ObjectReference> &dependencies = {}) {
    if (seq_no == -1) {
      accept_request();  // A seq_no of -1 means no ordering constraint.
      return;
    }
    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
    if (client_processed_up_to >= next_seq_no_) {
      RAY_LOG(ERROR) << "client skipping requests " << next_seq_no_ << " to "
                     << client_processed_up_to;
      next_seq_no_ = client_processed_up_to + 1;
    }
    RAY_LOG(DEBUG) << "Enqueue " << seq_no << " cur seqno " << next_seq_no_;
    pending_tasks_[seq_no] =
        InboundRequest(accept_request, reject_request, dependencies.size() > 0);
    if (dependencies.size() > 0) {
      waiter_.Wait(dependencies, [seq_no, this]() {
        RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
        auto it = pending_tasks_.find(seq_no);
        if (it != pending_tasks_.end()) {
          it->second.MarkDependenciesSatisfied();
          ScheduleRequests();
        }
      });
    }
    ScheduleRequests();
  }

 private:
  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() {
    // Only call SetMaxActorConcurrency to configure threadpool size when the
    // actor is not async actor. Async actor is single threaded.
    int max_concurrency = worker_context_.CurrentActorMaxConcurrency();
    if (worker_context_.CurrentActorIsAsync()) {
      // If this is an async actor, initialize the fiber state once.
      if (!is_asyncio_) {
        RAY_LOG(DEBUG) << "Setting direct actor as async, creating new fiber thread.";
        fiber_state_.reset(new FiberState(max_concurrency));
        is_asyncio_ = true;
      }
    } else {
      // If this is a concurrency actor (not async), initialize the thread pool once.
      if (max_concurrency != 1 && !pool_) {
        RAY_LOG(INFO) << "Creating new thread pool of size " << max_concurrency;
        pool_.reset(new BoundedExecutor(max_concurrency));
      }
    }

    // Cancel any stale requests that the client doesn't need any longer.
    while (!pending_tasks_.empty() && pending_tasks_.begin()->first < next_seq_no_) {
      auto head = pending_tasks_.begin();
      RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                     << pending_tasks_.begin()->first << " < " << next_seq_no_;
      head->second.Cancel();
      pending_tasks_.erase(head);
    }

    // Process as many in-order requests as we can.
    while (!pending_tasks_.empty() && pending_tasks_.begin()->first == next_seq_no_ &&
           pending_tasks_.begin()->second.CanExecute()) {
      auto head = pending_tasks_.begin();
      auto request = head->second;

      if (is_asyncio_) {
        // Process async actor task.
        fiber_state_->EnqueueFiber([request]() mutable { request.Accept(); });
      } else if (pool_) {
        // Process concurrent actor task.
        pool_->PostBlocking([request]() mutable { request.Accept(); });
      } else {
        // Process normal actor task.
        request.Accept();
      }
      pending_tasks_.erase(head);
      next_seq_no_++;
    }

    if (pending_tasks_.empty() || !pending_tasks_.begin()->second.CanExecute()) {
      // No timeout for object dependency waits.
      wait_timer_.cancel();
    } else {
      // Set a timeout on the queued tasks to avoid an infinite wait on failure.
      wait_timer_.expires_from_now(boost::posix_time::seconds(reorder_wait_seconds_));
      RAY_LOG(DEBUG) << "waiting for " << next_seq_no_ << " queue size "
                     << pending_tasks_.size();
      wait_timer_.async_wait([this](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted) {
          return;  // time deadline was adjusted
        }
        OnSequencingWaitTimeout();
      });
    }
  }

  /// Called when we time out waiting for an earlier task to show up.
  void OnSequencingWaitTimeout() {
    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
    RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                   << ", cancelling all queued tasks";
    while (!pending_tasks_.empty()) {
      auto head = pending_tasks_.begin();
      head->second.Cancel();
      next_seq_no_ = std::max(next_seq_no_, head->first + 1);
      pending_tasks_.erase(head);
    }
  }

  // Worker context.
  WorkerContext &worker_context_;
  /// Max time in seconds to wait for dependencies to show up.
  const int64_t reorder_wait_seconds_ = 0;
  /// Sorted map of (accept, rej) task callbacks keyed by their sequence number.
  std::map<int64_t, InboundRequest> pending_tasks_;
  /// The next sequence number we are waiting for to arrive.
  int64_t next_seq_no_ = 0;
  /// Timer for waiting on dependencies. Note that this is set on the task main
  /// io service, which is fine since it only ever fires if no tasks are running.
  boost::asio::deadline_timer wait_timer_;
  /// The id of the thread that constructed this scheduling queue.
  boost::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  /// If concurrent calls are allowed, holds the pool for executing these tasks.
  std::unique_ptr<BoundedExecutor> pool_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool is_asyncio_ = false;
  /// If use_asyncio_ is true, fiber_state_ contains the running state required
  /// to enable continuation and work together with python asyncio.
  std::unique_ptr<FiberState> fiber_state_;
  friend class SchedulingQueueTest;
};

class CoreWorkerDirectTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           const std::shared_ptr<ResourceMappingType> resource_ids,
                           std::vector<std::shared_ptr<RayObject>> *return_objects,
                           ReferenceCounter::ReferenceTableProto *borrower_refs)>;

  using OnTaskDone = std::function<ray::Status()>;

  CoreWorkerDirectTaskReceiver(WorkerContext &worker_context,
                               boost::asio::io_service &main_io_service,
                               const TaskHandler &task_handler,
                               const OnTaskDone &task_done)
      : worker_context_(worker_context),
        task_handler_(task_handler),
        task_main_io_service_(main_io_service),
        task_done_(task_done) {}

  /// Initialize this receiver. This must be called prior to use.
  void Init(rpc::ClientFactoryFn client_factory, rpc::Address rpc_address,
            std::shared_ptr<DependencyWaiter> dependency_waiter);

  /// Handle a `PushTask` request.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback);

 private:
  // Worker context.
  WorkerContext &worker_context_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The IO event loop for running tasks on.
  boost::asio::io_service &task_main_io_service_;
  /// The callback function to be invoked when finishing a task.
  OnTaskDone task_done_;
  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;
  /// Address of our RPC server.
  rpc::Address rpc_address_;
  /// Shared waiter for dependencies required by incoming tasks.
  std::shared_ptr<DependencyWaiter> waiter_;
  /// Queue of pending requests per actor handle.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  std::unordered_map<WorkerID, SchedulingQueue> scheduling_queue_;
};

}  // namespace ray
