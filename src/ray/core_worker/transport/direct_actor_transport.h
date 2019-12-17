#ifndef RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
#define RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H

#include <boost/asio/thread_pool.hpp>
#include <boost/fiber/all.hpp>
#include <boost/thread.hpp>
#include <list>
#include <queue>
#include <set>
#include <utility>
#include "absl/container/flat_hash_map.h"

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
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

// This class is thread-safe.
class CoreWorkerDirectActorTaskSubmitter {
 public:
  CoreWorkerDirectActorTaskSubmitter(rpc::ClientFactoryFn client_factory,
                                     std::shared_ptr<CoreWorkerMemoryStore> store,
                                     std::shared_ptr<TaskFinisherInterface> task_finisher)
      : client_factory_(client_factory),
        resolver_(store),
        task_finisher_(task_finisher) {}

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status::Invalid if the task is not yet supported.
  Status SubmitTask(TaskSpecification task_spec);

  /// Create connection to actor and send all pending tasks.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] address The new address of the actor.
  void ConnectActor(const ActorID &actor_id, const rpc::Address &address);

  /// Disconnect from a failed actor.
  ///
  /// \param[in] actor_id Actor ID.
  void DisconnectActor(const ActorID &actor_id, bool dead = false);

 private:
  /// Push a task to a remote actor via the given client.
  /// Note, this function doesn't return any error status code. If an error occurs while
  /// sending the request, this task will be treated as failed.
  ///
  /// \param[in] client The RPC client to send tasks to an actor.
  /// \param[in] request The request to send.
  /// \param[in] actor_id Actor ID.
  /// \param[in] task_id The ID of a task.
  /// \param[in] num_returns Number of return objects.
  /// \return Void.
  void PushActorTask(rpc::CoreWorkerClientInterface &client,
                     std::unique_ptr<rpc::PushTaskRequest> request,
                     const ActorID &actor_id, const TaskID &task_id, int num_returns)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Send all pending tasks for an actor.
  /// Note that this function doesn't take lock, the caller is expected to hold
  /// `mutex_` before calling this function.
  ///
  /// \param[in] actor_id Actor ID.
  /// \return Void.
  void SendPendingTasks(const ActorID &actor_id) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id) const;

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Mutex to proect the various maps below.
  mutable absl::Mutex mu_;

  /// Map from actor id to rpc client. This only includes actors that we send tasks to.
  /// We use shared_ptr to enable shared_from_this for pending client callbacks.
  ///
  /// TODO(zhijunfu): this will be moved into `actor_states_` later when we can
  /// subscribe updates for a specific actor.
  absl::flat_hash_map<ActorID, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      rpc_clients_ GUARDED_BY(mu_);

  /// Map from actor ids to worker ids. TODO(ekl) consider unifying this with the
  /// rpc_clients_ map.
  absl::flat_hash_map<ActorID, std::string> worker_ids_ GUARDED_BY(mu_);

  /// Map from actor id to the actor's pending requests. Each actor's requests
  /// are ordered by the task number in the request.
  absl::flat_hash_map<ActorID, std::map<int64_t, std::unique_ptr<rpc::PushTaskRequest>>>
      pending_requests_ GUARDED_BY(mu_);

  /// Map from actor id to the send position of the next task to queue for send
  /// for that actor. This is always greater than or equal to next_send_position_.
  absl::flat_hash_map<ActorID, int64_t> next_send_position_to_assign_ GUARDED_BY(mu_);

  /// Map from actor id to the send position of the next task to send to that actor.
  /// Note that this differs from the PushTaskRequest's sequence number in that it
  /// increases monotonically in this process independently of CallerId changes.
  absl::flat_hash_map<ActorID, int64_t> next_send_position_ GUARDED_BY(mu_);

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
  virtual void Wait(const std::vector<ObjectID> &dependencies,
                    std::function<void()> on_dependencies_available) = 0;
};

class DependencyWaiterImpl : public DependencyWaiter {
 public:
  DependencyWaiterImpl(raylet::RayletClient &raylet_client)
      : raylet_client_(raylet_client) {}

  void Wait(const std::vector<ObjectID> &dependencies,
            std::function<void()> on_dependencies_available) override {
    auto tag = next_request_id_++;
    requests_[tag] = on_dependencies_available;
    raylet_client_.WaitForDirectActorCallArgs(dependencies, tag);
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
  raylet::RayletClient &raylet_client_;
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

/// Used by async actor mode. The fiber event will be used
/// from python to switch control among different coroutines.
/// Taken from boost::fiber examples
/// https://github.com/boostorg/fiber/blob/7be4f860e733a92d2fa80a848dd110df009a20e1/examples/wait_stuff.cpp#L115-L142
class FiberEvent {
 public:
  // Block the fiber until the event is notified.
  void Wait() {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  // Notify the event and unblock all waiters.
  void Notify() {
    {
      std::unique_lock<boost::fibers::mutex> lock(mutex_);
      ready_ = true;
    }
    cond_.notify_one();
  }

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  bool ready_ = false;
};

/// Used by async actor mode. The FiberRateLimiter is a barrier that
/// allows at most num fibers running at once. It implements the
/// semaphore data structure.
class FiberRateLimiter {
 public:
  FiberRateLimiter(int num) : num_(num) {}

  // Enter the semaphore. Wait fo the value to be > 0 and decrement the value.
  void Acquire() {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return num_ > 0; });
    num_ -= 1;
  }

  // Exit the semaphore. Increment the value and notify other waiter.
  void Release() {
    {
      std::unique_lock<boost::fibers::mutex> lock(mutex_);
      num_ += 1;
    }
    // TODO(simon): This not does guarantee to wake up the first queued fiber.
    // This could be a problem for certain workloads because there is no guarantee
    // on task ordering .
    cond_.notify_one();
  }

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  int num_;
};

/// Used to ensure serial order of task execution per actor handle.
/// See direct_actor.proto for a description of the ordering protocol.
class SchedulingQueue {
 public:
  SchedulingQueue(boost::asio::io_service &main_io_service, DependencyWaiter &waiter,
                  std::shared_ptr<BoundedExecutor> pool = nullptr,
                  bool use_asyncio = false,
                  std::shared_ptr<FiberRateLimiter> fiber_rate_limiter = nullptr,
                  int64_t reorder_wait_seconds = kMaxReorderWaitSeconds)
      : wait_timer_(main_io_service),
        waiter_(waiter),
        reorder_wait_seconds_(reorder_wait_seconds),
        main_thread_id_(boost::this_thread::get_id()),
        pool_(pool),
        use_asyncio_(use_asyncio),
        fiber_rate_limiter_(fiber_rate_limiter) {}

  void Add(int64_t seq_no, int64_t client_processed_up_to,
           std::function<void()> accept_request, std::function<void()> reject_request,
           const std::vector<ObjectID> &dependencies = {}) {
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

      if (use_asyncio_) {
        boost::fibers::fiber([request, this]() mutable {
          fiber_rate_limiter_->Acquire();
          request.Accept();
          fiber_rate_limiter_->Release();
        })
            .detach();
      } else if (pool_ != nullptr) {
        pool_->PostBlocking([request]() mutable { request.Accept(); });
      } else {
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
      pending_tasks_.erase(head);
      next_seq_no_ = std::max(next_seq_no_, head->first + 1);
    }
  }

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
  std::shared_ptr<BoundedExecutor> pool_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool use_asyncio_;
  /// If use_asyncio_ is true, fiber_rate_limiter_ limits the max number of async
  /// tasks running at once.
  std::shared_ptr<FiberRateLimiter> fiber_rate_limiter_;

  friend class SchedulingQueueTest;
};

class CoreWorkerDirectTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           const std::shared_ptr<ResourceMappingType> resource_ids,
                           std::vector<std::shared_ptr<RayObject>> *return_objects)>;

  CoreWorkerDirectTaskReceiver(WorkerContext &worker_context,
                               boost::asio::io_service &main_io_service,
                               const TaskHandler &task_handler,
                               const std::function<void()> &exit_handler)
      : worker_context_(worker_context),
        task_handler_(task_handler),
        exit_handler_(exit_handler),
        task_main_io_service_(main_io_service) {}

  ~CoreWorkerDirectTaskReceiver() {
    fiber_shutdown_event_.Notify();
    // Only join the fiber thread if it was spawned in the first place.
    if (fiber_runner_thread_.joinable()) {
      fiber_runner_thread_.join();
    }
  }

  /// Initialize this receiver. This must be called prior to use.
  void Init(raylet::RayletClient &client, rpc::ClientFactoryFn client_factory,
            rpc::Address rpc_address);

  /// Handle a `PushTask` request.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback);

  /// Handle a `DirectActorCallArgWaitComplete` request.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleDirectActorCallArgWaitComplete(
      const rpc::DirectActorCallArgWaitCompleteRequest &request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback);

  /// Set the max concurrency at runtime. It cannot be changed once set.
  void SetMaxActorConcurrency(int max_concurrency);

  void SetActorAsAsync();

 private:
  // Worker context.
  WorkerContext &worker_context_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The callback function to exit the worker.
  std::function<void()> exit_handler_;
  /// The IO event loop for running tasks on.
  boost::asio::io_service &task_main_io_service_;
  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;
  /// Address of our RPC server.
  rpc::Address rpc_address_;
  /// Shared waiter for dependencies required by incoming tasks.
  std::unique_ptr<DependencyWaiterImpl> waiter_;
  /// Queue of pending requests per actor handle.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  std::unordered_map<TaskID, std::unique_ptr<SchedulingQueue>> scheduling_queue_;
  /// The max number of concurrent calls to allow.
  int max_concurrency_ = 1;
  /// Whether we are shutting down and not running further tasks.
  bool exiting_ = false;
  /// If concurrent calls are allowed, holds the pool for executing these tasks.
  std::shared_ptr<BoundedExecutor> pool_;
  /// Whether this actor use asyncio for concurrency.
  /// TODO(simon) group all asyncio related fields into a separate struct.
  bool is_asyncio_ = false;
  /// The thread that runs all asyncio fibers. is_asyncio_ must be true.
  std::thread fiber_runner_thread_;
  /// The fiber event used to block fiber_runner_thread_ from shutdown.
  /// is_asyncio_ must be true.
  FiberEvent fiber_shutdown_event_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once.
  std::shared_ptr<FiberRateLimiter> fiber_rate_limiter_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
