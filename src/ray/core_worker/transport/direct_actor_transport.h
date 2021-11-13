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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_task_submitter.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace core {

/// The max time to wait for out-of-order tasks.
const int kMaxReorderWaitSeconds = 30;

/// The class that manages fiber states for Python asyncio actors.
///
/// We'll create one fiber state for every concurrency group. And
/// create one default fiber state for default concurrency group if
/// necessary.
class FiberStateManager final {
 public:
  explicit FiberStateManager(const std::vector<ConcurrencyGroup> &concurrency_groups = {},
                             const int32_t default_group_max_concurrency = 1000) {
    for (auto &group : concurrency_groups) {
      const auto name = group.name;
      const auto max_concurrency = group.max_concurrency;
      auto fiber = std::make_shared<FiberState>(max_concurrency);
      auto &fds = group.function_descriptors;
      for (auto fd : fds) {
        functions_to_fiber_index_[fd->ToString()] = fiber;
      }
      name_to_fiber_index_[name] = fiber;
    }
    /// Create default fiber state for default concurrency group.
    if (default_group_max_concurrency >= 1) {
      default_fiber_ = std::make_shared<FiberState>(default_group_max_concurrency);
    }
  }

  /// Get the corresponding fiber state by the give concurrency group or function
  /// descriptor.
  ///
  /// Return the corresponding fiber state of the concurrency group
  /// if concurrency_group_name is given.
  /// Otherwise return the corresponding fiber state by the given function descriptor.
  std::shared_ptr<FiberState> GetFiber(const std::string &concurrency_group_name,
                                       ray::FunctionDescriptor fd) {
    if (!concurrency_group_name.empty()) {
      auto it = name_to_fiber_index_.find(concurrency_group_name);
      RAY_CHECK(it != name_to_fiber_index_.end())
          << "Failed to look up the fiber state of the given concurrency group "
          << concurrency_group_name << " . It might be that you didn't define "
          << "the concurrency group " << concurrency_group_name;
      return it->second;
    }

    /// Code path of that this task wasn't specified in a concurrency group addtionally.
    /// Use the predefined concurrency group.
    if (functions_to_fiber_index_.find(fd->ToString()) !=
        functions_to_fiber_index_.end()) {
      return functions_to_fiber_index_[fd->ToString()];
    }
    return default_fiber_;
  }

 private:
  // Map from the name to their corresponding fibers.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberState>> name_to_fiber_index_;

  // Map from the FunctionDescriptors to their corresponding fibers.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberState>> functions_to_fiber_index_;

  // The fiber for default concurrency group. It's nullptr if its max concurrency
  // is 1.
  std::shared_ptr<FiberState> default_fiber_ = nullptr;
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

  /// Stop the thread pool.
  void Stop() { pool_.stop(); }

  /// Join the thread pool.
  void Join() { pool_.join(); }

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

/// A manager that manages a set of thread pool. which will perform
/// the methods defined in one concurrency group.
class PoolManager final {
 public:
  explicit PoolManager(const std::vector<ConcurrencyGroup> &concurrency_groups = {},
                       const int32_t default_group_max_concurrency = 1) {
    for (auto &group : concurrency_groups) {
      const auto name = group.name;
      const auto max_concurrency = group.max_concurrency;
      auto pool = std::make_shared<BoundedExecutor>(max_concurrency);
      auto &fds = group.function_descriptors;
      for (auto fd : fds) {
        functions_to_thread_pool_index_[fd->ToString()] = pool;
      }
      name_to_thread_pool_index_[name] = pool;
    }
    // If max concurrency of default group is 1, the tasks of default group
    // will be performed in main thread instead of any executor pool.
    if (default_group_max_concurrency > 1) {
      default_thread_pool_ =
          std::make_shared<BoundedExecutor>(default_group_max_concurrency);
    }
  }

  std::shared_ptr<BoundedExecutor> GetPool(const std::string &concurrency_group_name,
                                           ray::FunctionDescriptor fd) {
    if (!concurrency_group_name.empty()) {
      auto it = name_to_thread_pool_index_.find(concurrency_group_name);
      /// TODO(qwang): Fail the user task.
      RAY_CHECK(it != name_to_thread_pool_index_.end());
      return it->second;
    }

    /// Code path of that this task wasn't specified in a concurrency group addtionally.
    /// Use the predefined concurrency group.
    if (functions_to_thread_pool_index_.find(fd->ToString()) !=
        functions_to_thread_pool_index_.end()) {
      return functions_to_thread_pool_index_[fd->ToString()];
    }
    return default_thread_pool_;
  }

  /// Stop and join the thread pools that the pool manager owns.
  void Stop() {
    if (default_thread_pool_) {
      RAY_LOG(DEBUG) << "Default pool is stopping.";
      default_thread_pool_->Stop();
      RAY_LOG(INFO) << "Default pool is joining. If the 'Default pool is joined.' "
                       "message is not printed after this, the worker is probably "
                       "hanging because the actor task is running an infinite loop.";
      default_thread_pool_->Join();
      RAY_LOG(INFO) << "Default pool is joined.";
    }

    for (const auto &it : name_to_thread_pool_index_) {
      it.second->Stop();
    }
    for (const auto &it : name_to_thread_pool_index_) {
      it.second->Join();
    }
  }

 private:
  // Map from the name to their corresponding thread pools.
  std::unordered_map<std::string, std::shared_ptr<BoundedExecutor>>
      name_to_thread_pool_index_;

  // Map from the FunctionDescriptors to their corresponding thread pools.
  std::unordered_map<std::string, std::shared_ptr<BoundedExecutor>>
      functions_to_thread_pool_index_;

  // The thread pool for default concurrency group. It's nullptr if its max concurrency
  // is 1.
  std::shared_ptr<BoundedExecutor> default_thread_pool_ = nullptr;
};

/// Object dependency and RPC state of an inbound request.
class InboundRequest {
 public:
  InboundRequest(){};

  InboundRequest(std::function<void(rpc::SendReplyCallback)> accept_callback,
                 std::function<void(rpc::SendReplyCallback)> reject_callback,
                 std::function<void(rpc::SendReplyCallback)> steal_callback,
                 rpc::SendReplyCallback send_reply_callback, TaskID task_id,
                 bool has_dependencies, const std::string &concurrency_group_name,
                 const ray::FunctionDescriptor &function_descriptor)
      : accept_callback_(std::move(accept_callback)),
        reject_callback_(std::move(reject_callback)),
        steal_callback_(std::move(steal_callback)),
        send_reply_callback_(std::move(send_reply_callback)),
        task_id_(task_id),
        concurrency_group_name_(concurrency_group_name),
        function_descriptor_(function_descriptor),
        has_pending_dependencies_(has_dependencies) {}

  void Accept() { accept_callback_(std::move(send_reply_callback_)); }
  void Cancel() { reject_callback_(std::move(send_reply_callback_)); }
  void Steal(rpc::StealTasksReply *reply) {
    reply->add_stolen_tasks_ids(task_id_.Binary());
    RAY_CHECK(TaskID::FromBinary(reply->stolen_tasks_ids(reply->stolen_tasks_ids_size() -
                                                         1)) == task_id_);
    steal_callback_(std::move(send_reply_callback_));
  }

  bool CanExecute() const { return !has_pending_dependencies_; }
  ray::TaskID TaskID() const { return task_id_; }
  const std::string &ConcurrencyGroupName() const { return concurrency_group_name_; }
  const ray::FunctionDescriptor &FunctionDescriptor() const {
    return function_descriptor_;
  }
  void MarkDependenciesSatisfied() { has_pending_dependencies_ = false; }

 private:
  std::function<void(rpc::SendReplyCallback)> accept_callback_;
  std::function<void(rpc::SendReplyCallback)> reject_callback_;
  std::function<void(rpc::SendReplyCallback)> steal_callback_;
  rpc::SendReplyCallback send_reply_callback_;

  ray::TaskID task_id_;
  std::string concurrency_group_name_;
  ray::FunctionDescriptor function_descriptor_;
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

/// Used to implement task queueing at the worker. Abstraction to provide a common
/// interface for actor tasks as well as normal ones.
class SchedulingQueue {
 public:
  virtual void Add(int64_t seq_no, int64_t client_processed_up_to,
                   std::function<void(rpc::SendReplyCallback)> accept_request,
                   std::function<void(rpc::SendReplyCallback)> reject_request,
                   rpc::SendReplyCallback send_reply_callback,
                   const std::string &concurrency_group_name,
                   const ray::FunctionDescriptor &function_descriptor,
                   std::function<void(rpc::SendReplyCallback)> steal_request = nullptr,
                   TaskID task_id = TaskID::Nil(),
                   const std::vector<rpc::ObjectReference> &dependencies = {}) = 0;
  virtual void ScheduleRequests() = 0;
  virtual bool TaskQueueEmpty() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t Steal(rpc::StealTasksReply *reply) = 0;
  virtual void Stop() = 0;
  virtual bool CancelTaskIfFound(TaskID task_id) = 0;
  virtual ~SchedulingQueue(){};
};

/// Used to ensure serial order of task execution per actor handle.
/// See direct_actor.proto for a description of the ordering protocol.
class ActorSchedulingQueue : public SchedulingQueue {
 public:
  ActorSchedulingQueue(
      instrumented_io_context &main_io_service, DependencyWaiter &waiter,
      std::shared_ptr<PoolManager> pool_manager = std::make_shared<PoolManager>(),
      bool is_asyncio = false, int fiber_max_concurrency = 1,
      const std::vector<ConcurrencyGroup> &concurrency_groups = {},
      int64_t reorder_wait_seconds = kMaxReorderWaitSeconds)
      : reorder_wait_seconds_(reorder_wait_seconds),
        wait_timer_(main_io_service),
        main_thread_id_(boost::this_thread::get_id()),
        waiter_(waiter),
        pool_manager_(pool_manager),
        is_asyncio_(is_asyncio) {
    if (is_asyncio_) {
      std::stringstream ss;
      ss << "Setting actor as asyncio with max_concurrency=" << fiber_max_concurrency
         << ", and defined concurrency groups are:" << std::endl;
      for (const auto &concurrency_group : concurrency_groups) {
        ss << "\t" << concurrency_group.name << " : "
           << concurrency_group.max_concurrency;
      }
      RAY_LOG(INFO) << ss.str();
      fiber_state_manager_ =
          std::make_unique<FiberStateManager>(concurrency_groups, fiber_max_concurrency);
    }
  }

  virtual ~ActorSchedulingQueue() = default;

  void Stop() {
    if (pool_manager_) {
      pool_manager_->Stop();
    }
  }

  bool TaskQueueEmpty() const {
    RAY_CHECK(false) << "TaskQueueEmpty() not implemented for actor queues";
    // The return instruction will never be executed, but we need to include it
    // nonetheless because this is a non-void function.
    return false;
  }

  size_t Size() const {
    RAY_CHECK(false) << "Size() not implemented for actor queues";
    // The return instruction will never be executed, but we need to include it
    // nonetheless because this is a non-void function.
    return 0;
  }

  /// Add a new actor task's callbacks to the worker queue.
  void Add(int64_t seq_no, int64_t client_processed_up_to,
           std::function<void(rpc::SendReplyCallback)> accept_request,
           std::function<void(rpc::SendReplyCallback)> reject_request,
           rpc::SendReplyCallback send_reply_callback,
           const std::string &concurrency_group_name,
           const ray::FunctionDescriptor &function_descriptor,
           std::function<void(rpc::SendReplyCallback)> steal_request = nullptr,
           TaskID task_id = TaskID::Nil(),
           const std::vector<rpc::ObjectReference> &dependencies = {}) {
    // A seq_no of -1 means no ordering constraint. Actor tasks must be executed in order.
    RAY_CHECK(seq_no != -1);

    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
    if (client_processed_up_to >= next_seq_no_) {
      RAY_LOG(ERROR) << "client skipping requests " << next_seq_no_ << " to "
                     << client_processed_up_to;
      next_seq_no_ = client_processed_up_to + 1;
    }
    RAY_LOG(DEBUG) << "Enqueue " << seq_no << " cur seqno " << next_seq_no_;

    pending_actor_tasks_[seq_no] = InboundRequest(
        std::move(accept_request), std::move(reject_request), std::move(steal_request),
        std::move(send_reply_callback), task_id, dependencies.size() > 0,
        concurrency_group_name, function_descriptor);

    if (dependencies.size() > 0) {
      waiter_.Wait(dependencies, [seq_no, this]() {
        RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
        auto it = pending_actor_tasks_.find(seq_no);
        if (it != pending_actor_tasks_.end()) {
          it->second.MarkDependenciesSatisfied();
          ScheduleRequests();
        }
      });
    }
    ScheduleRequests();
  }

  size_t Steal(rpc::StealTasksReply *reply) {
    RAY_CHECK(false) << "Cannot steal actor tasks";
    // The return instruction will never be executed, but we need to include it
    // nonetheless because this is a non-void function.
    return 0;
  }

  // We don't allow the cancellation of actor tasks, so invoking CancelTaskIfFound
  // results in a fatal error.
  bool CancelTaskIfFound(TaskID task_id) {
    RAY_CHECK(false) << "Cannot cancel actor tasks";
    // The return instruction will never be executed, but we need to include it
    // nonetheless because this is a non-void function.
    return false;
  }

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() {
    // Cancel any stale requests that the client doesn't need any longer.
    while (!pending_actor_tasks_.empty() &&
           pending_actor_tasks_.begin()->first < next_seq_no_) {
      auto head = pending_actor_tasks_.begin();
      RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                     << pending_actor_tasks_.begin()->first << " < " << next_seq_no_;
      head->second.Cancel();
      pending_actor_tasks_.erase(head);
    }

    // Process as many in-order requests as we can.
    while (!pending_actor_tasks_.empty() &&
           pending_actor_tasks_.begin()->first == next_seq_no_ &&
           pending_actor_tasks_.begin()->second.CanExecute()) {
      auto head = pending_actor_tasks_.begin();
      auto request = head->second;

      if (is_asyncio_) {
        // Process async actor task.
        auto fiber = fiber_state_manager_->GetFiber(request.ConcurrencyGroupName(),
                                                    request.FunctionDescriptor());
        fiber->EnqueueFiber([request]() mutable { request.Accept(); });
      } else {
        // Process actor tasks.
        RAY_CHECK(pool_manager_ != nullptr);
        auto pool = pool_manager_->GetPool(request.ConcurrencyGroupName(),
                                           request.FunctionDescriptor());
        if (pool == nullptr) {
          request.Accept();
        } else {
          pool->PostBlocking([request]() mutable { request.Accept(); });
        }
      }
      pending_actor_tasks_.erase(head);
      next_seq_no_++;
    }

    if (pending_actor_tasks_.empty() ||
        !pending_actor_tasks_.begin()->second.CanExecute()) {
      // No timeout for object dependency waits.
      wait_timer_.cancel();
    } else {
      // Set a timeout on the queued tasks to avoid an infinite wait on failure.
      wait_timer_.expires_from_now(boost::posix_time::seconds(reorder_wait_seconds_));
      RAY_LOG(DEBUG) << "waiting for " << next_seq_no_ << " queue size "
                     << pending_actor_tasks_.size();
      wait_timer_.async_wait([this](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted) {
          return;  // time deadline was adjusted
        }
        OnSequencingWaitTimeout();
      });
    }
  }

 private:
  /// Called when we time out waiting for an earlier task to show up.
  void OnSequencingWaitTimeout() {
    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
    RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                   << ", cancelling all queued tasks";
    while (!pending_actor_tasks_.empty()) {
      auto head = pending_actor_tasks_.begin();
      head->second.Cancel();
      next_seq_no_ = std::max(next_seq_no_, head->first + 1);
      pending_actor_tasks_.erase(head);
    }
  }

  /// Max time in seconds to wait for dependencies to show up.
  const int64_t reorder_wait_seconds_ = 0;
  /// Sorted map of (accept, rej) task callbacks keyed by their sequence number.
  std::map<int64_t, InboundRequest> pending_actor_tasks_;
  /// The next sequence number we are waiting for to arrive.
  int64_t next_seq_no_ = 0;
  /// Timer for waiting on dependencies. Note that this is set on the task main
  /// io service, which is fine since it only ever fires if no tasks are running.
  boost::asio::deadline_timer wait_timer_;
  /// The id of the thread that constructed this scheduling queue.
  boost::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<PoolManager> pool_manager_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool is_asyncio_ = false;
  /// Manage the running fiber states of actors in this worker. It works with
  /// python asyncio if this is an asyncio actor.
  std::unique_ptr<FiberStateManager> fiber_state_manager_;

  friend class SchedulingQueueTest;
};

/// Used to implement the non-actor task queue. These tasks do not have ordering
/// constraints.
class NormalSchedulingQueue : public SchedulingQueue {
 public:
  NormalSchedulingQueue(){};

  void Stop() {
    // No-op
  }

  bool TaskQueueEmpty() const {
    absl::MutexLock lock(&mu_);
    return pending_normal_tasks_.empty();
  }

  // Returns the current size of the task queue.
  size_t Size() const {
    absl::MutexLock lock(&mu_);
    return pending_normal_tasks_.size();
  }

  /// Add a new task's callbacks to the worker queue.
  void Add(
      int64_t seq_no, int64_t client_processed_up_to,
      std::function<void(rpc::SendReplyCallback)> accept_request,
      std::function<void(rpc::SendReplyCallback)> reject_request,
      rpc::SendReplyCallback send_reply_callback,
      const std::string &concurrency_group_name = "",
      const FunctionDescriptor &function_descriptor = FunctionDescriptorBuilder::Empty(),
      std::function<void(rpc::SendReplyCallback)> steal_request = nullptr,
      TaskID task_id = TaskID::Nil(),

      const std::vector<rpc::ObjectReference> &dependencies = {}) {
    absl::MutexLock lock(&mu_);
    // Normal tasks should not have ordering constraints.
    RAY_CHECK(seq_no == -1);
    // Create a InboundRequest object for the new task, and add it to the queue.

    pending_normal_tasks_.push_back(InboundRequest(
        std::move(accept_request), std::move(reject_request), std::move(steal_request),
        std::move(send_reply_callback), task_id, dependencies.size() > 0,
        /*concurrency_group_name=*/"", function_descriptor));
  }

  /// Steal up to max_tasks tasks by removing them from the queue and responding to the
  /// owner.
  size_t Steal(rpc::StealTasksReply *reply) {
    size_t tasks_stolen = 0;

    absl::MutexLock lock(&mu_);

    if (pending_normal_tasks_.size() <= 1) {
      RAY_LOG(DEBUG) << "We don't have enough tasks to steal, so we return early!";
      return tasks_stolen;
    }

    size_t half = pending_normal_tasks_.size() / 2;

    for (tasks_stolen = 0; tasks_stolen < half; tasks_stolen++) {
      RAY_CHECK(!pending_normal_tasks_.empty());
      InboundRequest tail = pending_normal_tasks_.back();
      pending_normal_tasks_.pop_back();
      int stolen_task_ids = reply->stolen_tasks_ids_size();
      tail.Steal(reply);
      RAY_CHECK(reply->stolen_tasks_ids_size() == stolen_task_ids + 1);
    }

    return tasks_stolen;
  }

  // Search for an InboundRequest associated with the task that we are trying to cancel.
  // If found, remove the InboundRequest from the queue and return true. Otherwise,
  // return false.
  bool CancelTaskIfFound(TaskID task_id) {
    absl::MutexLock lock(&mu_);
    for (std::deque<InboundRequest>::reverse_iterator it = pending_normal_tasks_.rbegin();
         it != pending_normal_tasks_.rend(); ++it) {
      if (it->TaskID() == task_id) {
        pending_normal_tasks_.erase(std::next(it).base());
        return true;
      }
    }
    return false;
  }

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() {
    while (true) {
      InboundRequest head;
      {
        absl::MutexLock lock(&mu_);
        if (!pending_normal_tasks_.empty()) {
          head = pending_normal_tasks_.front();
          pending_normal_tasks_.pop_front();
        } else {
          return;
        }
      }
      head.Accept();
    }
  }

 private:
  /// Protects access to the dequeue below.
  mutable absl::Mutex mu_;
  /// Queue with (accept, rej) callbacks for non-actor tasks
  std::deque<InboundRequest> pending_normal_tasks_ GUARDED_BY(mu_);
  friend class SchedulingQueueTest;
};

class CoreWorkerDirectTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           const std::shared_ptr<ResourceMappingType> resource_ids,
                           std::vector<std::shared_ptr<RayObject>> *return_objects,
                           ReferenceCounter::ReferenceTableProto *borrower_refs,
                           bool *is_application_level_error)>;

  using OnTaskDone = std::function<Status()>;

  CoreWorkerDirectTaskReceiver(WorkerContext &worker_context,
                               instrumented_io_context &main_io_service,
                               const TaskHandler &task_handler,
                               const OnTaskDone &task_done)
      : worker_context_(worker_context),
        task_handler_(task_handler),
        task_main_io_service_(main_io_service),
        task_done_(task_done),
        pool_manager_(std::make_shared<PoolManager>()) {}

  /// Initialize this receiver. This must be called prior to use.
  void Init(std::shared_ptr<rpc::CoreWorkerClientPool>, rpc::Address rpc_address,
            std::shared_ptr<DependencyWaiter> dependency_waiter);

  /// Handle a `PushTask` request. If it's an actor request, this function will enqueue
  /// the task and then start scheduling the requests to begin the execution. If it's a
  /// non-actor request, this function will just enqueue the task.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                  rpc::SendReplyCallback send_reply_callback);

  /// Pop tasks from the queue and execute them sequentially
  void RunNormalTasksFromQueue();

  /// Handle a `StealTask` request.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleStealTasks(const rpc::StealTasksRequest &request,
                        rpc::StealTasksReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

  bool CancelQueuedNormalTask(TaskID task_id);

  void Stop();

 protected:
  /// Cache the concurrency groups of actors.
  absl::flat_hash_map<ActorID, std::vector<ConcurrencyGroup>> concurrency_groups_cache_;

 private:
  // Worker context.
  WorkerContext &worker_context_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The IO event loop for running tasks on.
  instrumented_io_context &task_main_io_service_;
  /// The callback function to be invoked when finishing a task.
  OnTaskDone task_done_;
  /// Shared pool for producing new core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool_;
  /// Address of our RPC server.
  rpc::Address rpc_address_;
  /// Shared waiter for dependencies required by incoming tasks.
  std::shared_ptr<DependencyWaiter> waiter_;
  /// Queue of pending requests per actor handle.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  std::unordered_map<WorkerID, std::unique_ptr<SchedulingQueue>> actor_scheduling_queues_;
  // Queue of pending normal (non-actor) tasks.
  std::unique_ptr<SchedulingQueue> normal_scheduling_queue_ =
      std::unique_ptr<SchedulingQueue>(new NormalSchedulingQueue());
  /// The max number of concurrent calls to allow for fiber mode.
  /// 0 indicates that the value is not set yet.
  int fiber_max_concurrency_ = 0;

  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<PoolManager> pool_manager_;
  /// Whether this actor use asyncio for concurrency.
  bool is_asyncio_ = false;

  /// Set the max concurrency for fiber actor.
  /// This should be called once for the actor creation task.
  void SetMaxActorConcurrency(bool is_asyncio, int fiber_max_concurrency);
};

}  // namespace core
}  // namespace ray
