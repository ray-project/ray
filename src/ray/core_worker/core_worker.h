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

#include <gtest/gtest_prod.h>

#include <deque>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/buffer.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/generator_waiter.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/profile_event.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/shutdown_coordinator.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/task_execution/task_receiver.h"
#include "ray/core_worker/task_submission/normal_task_submitter.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/util/shared_lru.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray::core {

JobID GetProcessJobID(const CoreWorkerOptions &options);

/// Tracks stats for inbound tasks (tasks this worker is executing).
/// The counters are keyed by the function name in task spec.
class TaskCounter {
  /// A task can only be one of the following state. Received state in particular
  /// covers from the point of RPC call to beginning execution.
  enum class TaskStatusType { kPending, kRunning, kFinished };

 public:
  explicit TaskCounter(ray::observability::MetricInterface &task_by_state_gauge,
                       ray::observability::MetricInterface &actor_by_state_gauge);

  void BecomeActor(const std::string &actor_name) {
    absl::MutexLock l(&mu_);
    actor_name_ = actor_name;
  }

  void SetJobId(const JobID &job_id) {
    absl::MutexLock l(&mu_);
    job_id_ = job_id.Hex();
  }

  bool IsActor() ABSL_EXCLUSIVE_LOCKS_REQUIRED(&mu_) { return !actor_name_.empty(); }

  void RecordMetrics();

  void IncPending(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Increment({func_name, TaskStatusType::kPending, is_retry});
  }

  void MovePendingToRunning(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Swap({func_name, TaskStatusType::kPending, is_retry},
                  {func_name, TaskStatusType::kRunning, is_retry});
    num_tasks_running_++;
  }

  void MoveRunningToFinished(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Swap({func_name, TaskStatusType::kRunning, is_retry},
                  {func_name, TaskStatusType::kFinished, is_retry});
    num_tasks_running_--;
    RAY_CHECK_GE(num_tasks_running_, 0);
  }

  void SetMetricStatus(const std::string &func_name,
                       rpc::TaskStatus status,
                       bool is_retry);

  void UnsetMetricStatus(const std::string &func_name,
                         rpc::TaskStatus status,
                         bool is_retry);

 private:
  mutable absl::Mutex mu_;
  // Tracks all tasks submitted to this worker by state, is_retry.
  CounterMap<std::tuple<std::string, TaskStatusType, bool>> counter_ ABSL_GUARDED_BY(mu_);

  // Additionally tracks the sub-states of RUNNING_IN_RAY_GET/WAIT. The counters here
  // overlap with those of counter_.
  CounterMap<std::pair<std::string, bool>> running_in_get_counter_ ABSL_GUARDED_BY(mu_);
  CounterMap<std::pair<std::string, bool>> running_in_wait_counter_ ABSL_GUARDED_BY(mu_);
  CounterMap<std::pair<std::string, bool>> pending_getting_and_pinning_args_fetch_counter_
      ABSL_GUARDED_BY(mu_);

  std::string job_id_ ABSL_GUARDED_BY(mu_);
  // Used for actor state tracking.
  std::string actor_name_ ABSL_GUARDED_BY(mu_);
  int64_t num_tasks_running_ ABSL_GUARDED_BY(mu_) = 0;

  // Metric to track the number of tasks by state.
  // Expected tags:
  // - State: the task state, as described by rpc::TaskState proto in common.proto
  // - Name: the name of the function called
  // - IsRetry: whether the task is a retry
  // - Source: component reporting, e.g., "core_worker", "executor", or "pull_manager"
  ray::observability::MetricInterface &task_by_state_gauge_;
  ray::observability::MetricInterface &actor_by_state_gauge_;
};

struct TaskToRetry {
  /// Time when the task should be retried.
  int64_t execution_time_ms{};

  /// The details of the task.
  TaskSpecification task_spec;
};

/// Sorts TaskToRetry in descending order of the execution time.
/// Priority queue naturally sorts elements in descending order,
/// in order to have the tasks ordered by execution time in
/// ascending order we use a comparator that sorts elements in
/// descending order. Per docs "Priority queues are a type of container
/// adaptors, specifically designed such that its first element is always
/// the greatest of the elements it contains".
class TaskToRetryDescComparator {
 public:
  bool operator()(const TaskToRetry &left, const TaskToRetry &right) {
    return left.execution_time_ms > right.execution_time_ms;
  }
};

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// All member variables are injected either from CoreWorkerProcess or test code

  CoreWorker(CoreWorkerOptions options,
             std::unique_ptr<WorkerContext> worker_context,
             instrumented_io_context &io_service,
             std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
             std::shared_ptr<rpc::RayletClientPool> raylet_client_pool,
             std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
             std::unique_ptr<rpc::GrpcServer> core_worker_server,
             rpc::Address rpc_address,
             std::shared_ptr<gcs::GcsClient> gcs_client,
             std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client,
             std::shared_ptr<ray::RayletClientInterface> local_raylet_rpc_client,
             boost::thread &io_thread,
             std::shared_ptr<ReferenceCounterInterface> reference_counter,
             std::shared_ptr<CoreWorkerMemoryStore> memory_store,
             std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider,
             std::shared_ptr<experimental::MutableObjectProviderInterface>
                 experimental_mutable_object_provider,
             std::unique_ptr<FutureResolver> future_resolver,
             std::shared_ptr<TaskManager> task_manager,
             std::shared_ptr<ActorCreatorInterface> actor_creator,
             std::unique_ptr<ActorTaskSubmitter> actor_task_submitter,
             std::unique_ptr<pubsub::PublisherInterface> object_info_publisher,
             std::unique_ptr<pubsub::SubscriberInterface> object_info_subscriber,
             std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter,
             std::unique_ptr<NormalTaskSubmitter> normal_task_submitter,
             std::unique_ptr<ObjectRecoveryManager> object_recovery_manager,
             std::unique_ptr<ActorManager> actor_manager,
             instrumented_io_context &task_execution_service,
             std::unique_ptr<worker::TaskEventBuffer> task_event_buffer,
             uint32_t pid,
             ray::observability::MetricInterface &task_by_state_counter,
             ray::observability::MetricInterface &actor_by_state_counter);

  CoreWorker(CoreWorker const &) = delete;

  /// Core worker's deallocation lifecycle
  ///
  /// Shutdown API must be called before deallocating a core worker.
  /// Otherwise, it can have various destruction order related memory corruption.
  ///
  /// If the core worker is initiated at a driver, the driver is responsible for calling
  /// the shutdown API before terminating. If the core worker is initiated at a worker,
  /// shutdown must be called before terminating the task execution loop.
  ~CoreWorker();

  void operator=(CoreWorker const &other) = delete;

  ///
  /// Public methods used by `CoreWorkerProcess` and `CoreWorker` itself.
  ///

  /// Get the Plasma Store Usage.
  ///
  /// \param output[out] memory usage from the plasma store
  /// \return error status if unable to get response from the plasma store
  Status GetPlasmaUsage(std::string &output);

  /// Gracefully disconnect the worker from Raylet.
  /// Once the method is returned, it is guaranteed that raylet is
  /// notified that this worker is disconnected from a raylet.
  ///
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  /// \param creation_task_exception_pb_bytes It is given when the worker is
  /// disconnected because the actor is failed due to its exception in its init method.
  void Disconnect(const rpc::WorkerExitType &exit_type,
                  const std::string &exit_detail,
                  const std::shared_ptr<LocalMemoryBuffer>
                      &creation_task_exception_pb_bytes = nullptr);

  /// Shut down the worker completely.
  ///
  /// This must be called before deallocating a worker / driver's core worker for memory
  /// safety.
  ///
  void Shutdown();

  /// Start receiving and executing tasks.
  void RunTaskExecutionLoop();

  const WorkerID &GetWorkerID() const;

  WorkerType GetWorkerType() const { return options_.worker_type; }

  Language GetLanguage() const { return options_.language; }

  WorkerContext &GetWorkerContext() { return *worker_context_; }

  const TaskID &GetCurrentTaskId() const { return worker_context_->GetCurrentTaskID(); }

  const std::string GetCurrentTaskName() const {
    return worker_context_->GetCurrentTask() != nullptr
               ? worker_context_->GetCurrentTask()->GetName()
               : "";
  }

  const std::string GetCurrentTaskFunctionName() const {
    return (worker_context_->GetCurrentTask() != nullptr &&
            worker_context_->GetCurrentTask()->FunctionDescriptor() != nullptr)
               ? worker_context_->GetCurrentTask()->FunctionDescriptor()->CallSiteString()
               : "";
  }

  /// Controls the is debugger paused flag.
  ///
  /// \param task_id The task id of the task to update.
  /// \param is_debugger_paused The new value of the flag.
  void UpdateTaskIsDebuggerPaused(const TaskID &task_id, const bool is_debugger_paused);

  int64_t GetCurrentTaskAttemptNumber() const {
    return worker_context_->GetCurrentTask() != nullptr
               ? worker_context_->GetCurrentTask()->AttemptNumber()
               : 0;
  }

  JobID GetCurrentJobId() const { return worker_context_->GetCurrentJobID(); }

  int64_t GetTaskDepth() const { return worker_context_->GetTaskDepth(); }

  NodeID GetCurrentNodeId() const { return NodeID::FromBinary(rpc_address_.node_id()); }

  /// Read the next index of a ObjectRefStream of generator_id.
  /// This API always return immediately.
  ///
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  /// \param[out] object_ref_out The ObjectReference
  /// that the caller can convert to its own ObjectRef.
  /// The current process is always the owner of the
  /// generated ObjectReference. It will be Nil() if there's
  /// no next item.
  /// \return Status ObjectRefEndOfStream if the stream reaches to EoF.
  /// OK otherwise.
  Status TryReadObjectRefStream(const ObjectID &generator_id,
                                rpc::ObjectReference *object_ref_out);

  /// Return True if there's no more object to read. False otherwise.
  bool StreamingGeneratorIsFinished(const ObjectID &generator_id) const;

  /// Read the next index of a ObjectRefStream of generator_id without
  /// consuming an index.
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  /// \return A object reference of the next index and if the object is already ready
  /// (meaning if the object's value if retrievable).
  /// It should not be nil.
  std::pair<rpc::ObjectReference, bool> PeekObjectRefStream(const ObjectID &generator_id);

  /// Asynchronously delete the ObjectRefStream that was created upon the
  /// initial task submission. This method triggers a timer. On each interval,
  /// we check whether the generator ref and all dynamic return refs have been
  /// removed in the ref counter. If so, we remove the stream and task
  /// metadata, because we know that the streaming task can never be
  /// re-executed.
  ///
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  void AsyncDelObjectRefStream(const ObjectID &generator_id);

  // Attempt to delete ObjectRefStreams that were unable to be deleted when
  // AsyncDelObjectRefStream was called (stored in generator_ids_pending_deletion_).
  // This function is called periodically on the io_service_.
  void TryDelPendingObjectRefStreams();

  PlacementGroupID GetCurrentPlacementGroupId() const {
    return worker_context_->GetCurrentPlacementGroupId();
  }

  bool ShouldCaptureChildTasksInPlacementGroup() const {
    return worker_context_->ShouldCaptureChildTasksInPlacementGroup();
  }

  bool GetCurrentTaskRetryExceptions() const {
    if (options_.is_local_mode) {
      return false;
    }
    return worker_context_->GetCurrentTask()->ShouldRetryExceptions();
  }

  void SetWebuiDisplay(const std::string &key, const std::string &message);

  /// Sets the actor's repr name.
  ///
  /// This is set explicitly rather than included as part of actor creation task spec
  /// because it's only available after running the creation task as it might depend on
  /// fields to be be initialized during actor creation task. The repr name will be
  /// included as part of actor creation task reply (PushTaskReply) to GCS.
  ///
  /// \param repr_name Actor repr name.
  void SetActorReprName(const std::string &repr_name);

  void SetCallerCreationTimestamp();

  /// Increase the reference count for this object ID.
  /// Increase the local reference count for this object ID. Should be called
  /// by the language frontend when a new reference is created.
  ///
  /// \param[in] object_id The object ID to increase the reference count for.
  void AddLocalReference(const ObjectID &object_id) {
    AddLocalReference(object_id, CurrentCallSite());
  }

  /// Decrease the reference count for this object ID. Should be called
  /// by the language frontend when a reference is destroyed.
  ///
  /// \param[in] object_id The object ID to decrease the reference count for.
  void RemoveLocalReference(const ObjectID &object_id) {
    std::vector<ObjectID> deleted;
    reference_counter_->RemoveLocalReference(object_id, &deleted);
    // TODO(ilr): better way of keeping an object from being deleted
    // TODO(sang): This seems bad... We should delete the memory store
    // properly from reference counter.
    if (!options_.is_local_mode) {
      memory_store_->Delete(deleted);
    }
  }

  int GetMemoryStoreSize() { return memory_store_->Size(); }

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const;

  /// Return all pending children task ids for a given parent task id.
  /// The parent task id should exist in the current worker.
  /// For debugging and testing only.
  std::vector<TaskID> GetPendingChildrenTasks(const TaskID &task_id) const;

  /// Get the RPC address of this worker.
  ///
  /// \param[out] The RPC address of this worker.
  const rpc::Address &GetRpcAddress() const;

  /// Get the RPC address of the worker that owns the given object.
  ///
  /// \param[in] object_id The object ID. The object must either be owned by
  /// us, or the caller previously added the ownership information (via
  /// RegisterOwnershipInfoAndResolveFuture).
  /// \param[out] The RPC address of the worker that owns this object.
  Status GetOwnerAddress(const ObjectID &object_id, rpc::Address *owner_address) const;

  /// Get the RPC address of the worker that owns the given object. If the
  /// object has no owner, then we terminate the process.
  ///
  /// \param[in] object_id The object ID. The object must either be owned by
  /// us, or the caller previously added the ownership information (via
  /// RegisterOwnershipInfoAndResolveFuture).
  /// \param[out] The RPC address of the worker that owns this object.
  rpc::Address GetOwnerAddressOrDie(const ObjectID &object_id) const;

  /// Get the RPC address of the worker that owns the given object.
  ///
  /// \param[in] object_id The object ID. The object must either be owned by
  /// us, or the caller previously added the ownership information (via
  /// RegisterOwnershipInfoAndResolveFuture).
  /// \param[out] The RPC address of the worker that owns this object.
  std::vector<rpc::ObjectReference> GetObjectRefs(
      const std::vector<ObjectID> &object_ids) const;

  /// Get the owner information of an object. This should be
  /// called when serializing an object ID, and the returned information should
  /// be stored with the serialized object ID.
  ///
  /// This can only be called on object IDs that we created via task
  /// submission, ray.put, or object IDs that we deserialized. It cannot be
  /// called on object IDs that were created randomly, e.g.,
  /// ObjectID::FromRandom.
  ///
  /// Postcondition: Get(object_id) is valid.
  ///
  /// \param[in] object_id The object ID to serialize.
  /// appended to the serialized object ID.
  /// \param[out] owner_address The address of the object's owner. This should
  /// be appended to the serialized object ID.
  /// \param[out] serialized_object_status The serialized object status protobuf.
  Status GetOwnershipInfo(const ObjectID &object_id,
                          rpc::Address *owner_address,
                          std::string *serialized_object_status);

  /// Add a reference to an ObjectID that was deserialized by the language
  /// frontend. This will also start the process to resolve the future.
  /// Specifically, we will periodically contact the owner, until we learn that
  /// the object has been created or the owner is no longer reachable. This
  /// will then unblock any Gets or submissions of tasks dependent on the
  /// object.
  ///
  /// \param[in] object_id The object ID to deserialize.
  /// \param[in] outer_object_id The object ID that contained object_id, if
  /// any. This may be nil if the object ID was inlined directly in a task spec
  /// or if it was passed out-of-band by the application (deserialized from a
  /// byte string).
  /// \param[in] owner_address The address of the object's owner.
  /// \param[in] serialized_object_status The serialized object status protobuf.
  void RegisterOwnershipInfoAndResolveFuture(const ObjectID &object_id,
                                             const ObjectID &outer_object_id,
                                             const rpc::Address &owner_address,
                                             const std::string &serialized_object_status);

  ///
  /// Public methods related to storing and retrieving objects.
  ///

  /// Put an object into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object,
             const std::vector<ObjectID> &contained_object_ids,
             ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[in] object_id Object ID specified by the user.
  /// \param[in] pin_object Whether or not to tell the raylet to pin this object.
  /// \return Status.
  Status Put(const RayObject &object,
             const std::vector<ObjectID> &contained_object_ids,
             const ObjectID &object_id,
             bool pin_object = false);

  /// Create and return a buffer in the object store that can be directly written
  /// into. After writing to the buffer, the caller must call `SealOwned()` to
  /// finalize the object. The `CreateOwnedAndIncrementLocalRef()` and
  /// `SealOwned()` combination is an alternative interface to `Put()` that
  /// allows frontends to avoid an extra copy when possible.
  ///
  /// Note that this call also initializes the local reference count for the
  /// object to 1 so that the ref is considered in scope. The caller must
  /// ensure that they decrement the ref count once the returned ObjectRef has
  /// gone out of scope.
  ///
  /// \param[in] is_experimental_mutable_object Whether this object is an
  /// experimental mutable object. If true, then the returned object buffer
  /// will not be available to read until the caller Seals and then writes
  /// again.
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[out] object_id Object ID generated for the put.
  /// \param[out] data Buffer for the user to write the object into.
  /// \param[in] owner_address The address of object's owner. If not provided,
  /// defaults to this worker.
  /// \param[in] inline_small_object Whether to inline create this object if it's
  /// small.
  /// \param[in] tensor_transport The tensor transport to use for the object.
  /// \return Status.
  Status CreateOwnedAndIncrementLocalRef(
      bool is_experimental_mutable_object,
      const std::shared_ptr<Buffer> &metadata,
      const size_t data_size,
      const std::vector<ObjectID> &contained_object_ids,
      ObjectID *object_id,
      std::shared_ptr<Buffer> *data,
      const std::unique_ptr<rpc::Address> &owner_address = nullptr,
      bool inline_small_object = true,
      rpc::TensorTransport tensor_transport = rpc::TensorTransport::OBJECT_STORE);

  /// Create and return a buffer in the object store that can be directly written
  /// into, for an object ID that already exists. After writing to the buffer, the
  /// caller must call `SealExisting()` to finalize the object. The `CreateExisting()`
  /// and `SealExisting()` combination is an alternative interface to `Put()` that
  /// allows frontends to avoid an extra copy when possible.
  ///
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] object_id Object ID specified by the user.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[out] data Buffer for the user to write the object into.
  /// \return Status.
  Status CreateExisting(const std::shared_ptr<Buffer> &metadata,
                        const size_t data_size,
                        const ObjectID &object_id,
                        const rpc::Address &owner_address,
                        std::shared_ptr<Buffer> *data,
                        bool created_by_worker);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `CreateOwned()` call and then writing into the returned buffer.
  ///
  /// If the object seal fails, then the initial local reference that was added
  /// in CreateOwnedAndIncrementLocalRef will be deleted and the object will be
  /// released by the ref counter.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \param[in] pin_object Whether or not to pin the object at the local raylet.
  /// \param[in] The address of object's owner. If not provided,
  /// defaults to this worker.
  /// \return Status.
  Status SealOwned(const ObjectID &object_id,
                   bool pin_object,
                   const std::unique_ptr<rpc::Address> &owner_address = nullptr);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `CreateExisting()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \param[in] pin_object Whether or not to pin the object at the local raylet.
  /// \param[in] generator_id For dynamically created objects, this is the ID
  /// of the object that wraps the dynamically created ObjectRefs in a
  /// generator. We use this to notify the owner of the dynamically created
  /// objects.
  /// \param[in] owner_address Address of the owner of the object who will be contacted by
  /// the raylet if the object is pinned. If not provided, defaults to this worker.
  /// \return Status.
  Status SealExisting(const ObjectID &object_id,
                      bool pin_object,
                      const ObjectID &generator_id = ObjectID::Nil(),
                      const std::unique_ptr<rpc::Address> &owner_address = nullptr);

  /// Experimental method for mutable objects. Acquires a write lock on the
  /// object that prevents readers from reading until we are done writing. Does
  /// not protect against concurrent writers.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] metadata The metadata of the object. This overwrites the
  /// current metadata.
  /// \param[in] data_size The size of the object to write. This overwrites the
  /// current data size.
  /// \param[in] num_readers The number of readers that must read and release
  /// the object before the caller can write again.
  /// \param[in] timeout_ms The timeout in milliseconds to acquire the write lock.
  /// If this is 0, the method will try to acquire the write lock once immediately,
  /// and return either OK or TimedOut without blocking. If this is -1, the method
  /// will block indefinitely until the write lock is acquired.
  /// \param[out] data The mutable object buffer in plasma that can be written to.
  Status ExperimentalChannelWriteAcquire(const ObjectID &object_id,
                                         const std::shared_ptr<Buffer> &metadata,
                                         uint64_t data_size,
                                         int64_t num_readers,
                                         int64_t timeout_ms,
                                         std::shared_ptr<Buffer> *data);

  /// Experimental method for mutable objects. Releases a write lock on the
  /// object, allowing readers to read. This is the equivalent of "Seal" for
  /// normal objects.
  ///
  /// \param[in] object_id The ID of the object.
  Status ExperimentalChannelWriteRelease(const ObjectID &object_id);

  /// Experimental method for mutable objects. Sets the error bit, causing all
  /// future readers and writers to raise an error on acquire.
  ///
  /// \param[in] object_id The ID of the object.
  Status ExperimentalChannelSetError(const ObjectID &object_id);

  /// Experimental method for mutable objects. Registers a writer channel.
  ///
  /// The API is not idempotent.
  ///
  /// \param[in] writer_object_id The ID of the object.
  /// \param[in] remote_reader_node_ids The list of remote reader's node ids.
  void ExperimentalRegisterMutableObjectWriter(
      const ObjectID &writer_object_id,
      const std::vector<NodeID> &remote_reader_node_ids);

  /// Experimental method for mutable objects. Registers a reader channel.
  ///
  /// The API is not idempotent.
  ///
  /// \param[in] object_id The ID of the object.
  Status ExperimentalRegisterMutableObjectReader(const ObjectID &object_id);

  /// Experimental method for mutable objects. Registers a mapping from a mutable object
  /// that is written to on this node to the corresponding mutable object that is read on
  /// the node that `remote_reader_actors` is on.
  ///
  /// \param[in] writer_object_id The ID of the object that is written on this node.
  /// \param[in] remote_reader_ref_info The remote reader reference info. There's
  /// 1 reader reference per node.
  Status ExperimentalRegisterMutableObjectReaderRemote(
      const ObjectID &writer_object_id,
      const std::vector<ray::experimental::ReaderRefInfo> &remote_reader_ref_info);

  /// Get a list of objects from the object store. Objects that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids,
             const int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> &results);

  /// Get objects directly from the local plasma store, without waiting for the
  /// objects to be fetched from another node. This should only be used
  /// internally, never by user code.
  /// NOTE: Caller of this method should guarantee that the object already exists in the
  /// plasma store, thus it doesn't need to fetch from other nodes.
  ///
  /// \param[in] ids The IDs of the objects to get.
  /// \param[out] results The results will be stored here. A nullptr will be
  /// added for objects that were not in the local store.
  /// \return Status OK if all objects were found. Returns ObjectNotFound error
  /// if at least one object was not in the local store.
  Status GetIfLocal(const std::vector<ObjectID> &ids,
                    std::vector<std::shared_ptr<RayObject>> *results);

  /// Return whether or not the object store contains the given object.
  ///
  /// \param[in] object_id ID of the objects to check for.
  /// \param[out] has_object Whether or not the object is present.
  /// \param[out] is_in_plasma Whether or not the object is in Plasma.
  /// \return Status.
  Status Contains(const ObjectID &object_id,
                  bool *has_object,
                  bool *is_in_plasma = nullptr);

  /// Wait for a list of objects to appear in the object store.
  /// Duplicate object ids are supported, and `num_objects` includes duplicate ids in this
  /// case.
  /// TODO(zhijunfu): it is probably more clear in semantics to just fail when there
  /// are duplicates, and require it to be handled at application level.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_objects Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A vector of booleans that indicates each object has appeared or
  /// not.
  /// \return Status.
  Status Wait(const std::vector<ObjectID> &object_ids,
              const int num_objects,
              const int64_t timeout_ms,
              std::vector<bool> *results,
              bool fetch_local);

  /// Delete a list of objects from the plasma object store.
  ///
  /// This calls DeleteImpl() locally for objects we own, and DeleteImpl() remotely
  /// for objects we do not own.
  ///
  /// If IOError is returned from DeleteImpl() when deleting objects locally, we will
  /// return an UnexpectedSystemExit status instead. This is to make sure the tasks
  /// that calls this function in application code can properly retry when hitting the
  /// IOError.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only);

  /// Delete a list of objects from the plasma object store; called by Delete().
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \return Status.
  Status DeleteImpl(const std::vector<ObjectID> &object_ids, bool local_only);

  /// Get the locations of a list objects from the local core worker. Locations that
  /// failed to be retrieved will be returned as nullopt. No RPCs are made in this
  /// method.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[out] results Result list of object locations.
  /// \return Status.
  Status GetLocalObjectLocations(const std::vector<ObjectID> &object_ids,
                                 std::vector<std::optional<ObjectLocation>> *results);

  /// Return the locally submitted ongoing retry tasks triggered by lineage
  /// reconstruction. Key is the lineage reconstruction task info.
  /// Value is the number of ongoing lineage reconstruction tasks of this type.
  std::unordered_map<rpc::LineageReconstructionTask, uint64_t>
  GetLocalOngoingLineageReconstructionTasks() const;

  /// Get the locations of a list objects. Locations that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// Note: this returns shared_ptr while `GetLocalObjectLocations` returns optional.
  /// This is just an optimization in the implementation because this method needs to
  /// track RPCs out-of-order. They don't have any semantic differences.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of object locations.
  /// \return Status.
  Status GetLocationFromOwner(const std::vector<ObjectID> &object_ids,
                              int64_t timeout_ms,
                              std::vector<std::shared_ptr<ObjectLocation>> *results);

  /// Trigger garbage collection on each worker in the cluster.
  void TriggerGlobalGC();

  /// Report the task caller at caller_address that the intermediate
  /// task return. It means if this API is used, the caller will be notified
  /// the task return before the current task is terminated. The caller must
  /// implement HandleReportGeneratorItemReturns API endpoint
  /// to handle the intermediate result report.
  /// This API makes sense only for a generator task
  /// (task that can return multiple intermediate
  /// result before the task terminates).
  ///
  /// NOTE: The API doesn't guarantee the ordering of the report. The
  /// caller is supposed to reorder the report based on the item_index.
  ///
  /// \param[in] returned_object A intermediate ray object to report
  /// to the caller before the task terminates. This object must have been
  /// created dynamically from this worker via AllocateReturnObject.
  /// If the Object ID is nil, it means it is the end of the task return.
  /// In this case, the caller is responsible for setting finished = true,
  /// otherwise it will panic.
  /// \param[in] generator_id The return object ref ID from a current generator
  /// task.
  /// \param[in] caller_address The address of the caller of the current task
  /// that created a generator_id.
  /// \param[in] item_index The index of the task return. It is used to reorder the
  /// report from the caller side.
  /// \param[in] attempt_number The number of time the current task is retried.
  /// 0 means it is the first attempt.
  /// \param[in] waiter The class to pause the thread if generator backpressure limit
  /// is reached.
  Status ReportGeneratorItemReturns(
      const std::pair<ObjectID, std::shared_ptr<RayObject>> &returned_object,
      const ObjectID &generator_id,
      const rpc::Address &caller_address,
      int64_t item_index,
      uint64_t attempt_number,
      const std::shared_ptr<GeneratorBackpressureWaiter> &waiter);

  /// Implements gRPC server handler.
  /// If an executor can generator task return before the task is finished,
  /// it invokes this endpoint via ReportGeneratorItemReturns RPC.
  void HandleReportGeneratorItemReturns(rpc::ReportGeneratorItemReturnsRequest request,
                                        rpc::ReportGeneratorItemReturnsReply *reply,
                                        rpc::SendReplyCallback send_reply_callback);

  ///
  /// Public methods related to task submission.
  ///

  /// Push an error to the relevant driver.
  ///
  /// \param[in] The ID of the job_id that the error is for.
  /// \param[in] The type of the error.
  /// \param[in] The error message.
  /// \param[in] The timestamp of the error.
  /// \return Status.
  Status PushError(const JobID &job_id,
                   const std::string &type,
                   const std::string &error_message,
                   double timestamp);

  // Prestart workers. The workers:
  // - uses current language.
  // - uses current JobID.
  // - does NOT support root_detached_actor_id.
  // - uses provided runtime_env_info applied to the job runtime env, as if it's a task
  // request.
  //
  // This API is async. It provides no guarantee that the workers are actually started.
  void PrestartWorkers(const std::string &serialized_runtime_env_info,
                       uint64_t keep_alive_duration_secs,
                       size_t num_workers);

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[in] max_retries max number of retry when the task fails.
  /// \param[in] retry_exceptions whether a user exception/error is eligible to retry.
  /// \param[in] scheduling_strategy Strategy about how to schedule the task.
  /// \param[in] debugger_breakpoint breakpoint to drop into for the debugger after this
  /// task starts executing, or "" if we do not want to drop into the debugger.
  /// should capture parent's placement group implicitly.
  /// \param[in] serialized_retry_exception_allowlist A serialized exception list
  /// that serves as an allowlist of frontend-language exceptions/errors that should be
  /// retried. Default is an empty string, which will be treated as an allow-all in the
  /// language worker.
  /// \param[in] current_task_id The current task_id that submits the task.
  /// If Nil() is given, it will be automatically propagated from worker_context.
  /// This is used when worker_context cannot reliably obtain the current task_id
  /// i.e., Python async actors.
  /// \param[in] call_site The stacktrace of the task invocation, or actor
  /// creation. This is only used for observability.
  /// \return ObjectRefs returned by this task.
  std::vector<rpc::ObjectReference> SubmitTask(
      const RayFunction &function,
      const std::vector<std::unique_ptr<TaskArg>> &args,
      const TaskOptions &task_options,
      int max_retries,
      bool retry_exceptions,
      const rpc::SchedulingStrategy &scheduling_strategy,
      const std::string &debugger_breakpoint,
      const std::string &serialized_retry_exception_allowlist = "",
      const std::string &call_site = "",
      const TaskID current_task_id = TaskID::Nil());

  /// Create an actor.
  ///
  /// NOTE: RAY CHECK fails if an actor handle with the same actor id has already been
  /// added, or if the scheduling strategy for actor creation is not set.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[in] extension_data Extension data of the actor handle,
  /// see `ActorHandle` in `core_worker.proto`.
  /// \param[in] call_site The stacktrace of the actor creation. This is
  /// only used for observability.
  /// \param[out] actor_id ID of the created actor. This can be used to submit
  /// tasks on the actor.
  /// \return Status error if actor creation fails, likely due to raylet failure.
  Status CreateActor(const RayFunction &function,
                     const std::vector<std::unique_ptr<TaskArg>> &args,
                     const ActorCreationOptions &actor_creation_options,
                     const std::string &extension_data,
                     const std::string &call_site,
                     ActorID *actor_id);

  /// Create a placement group.
  ///
  /// \param[in] function The remote function that generates the placement group object.
  /// \param[in] placement_group_creation_options Options for this placement group
  /// creation task.
  /// \param[out] placement_group_id ID of the created placement group.
  /// This can be used to schedule actor in node
  /// \return Status error if placement group
  /// creation fails, likely due to raylet failure.
  Status CreatePlacementGroup(
      const PlacementGroupCreationOptions &placement_group_creation_options,
      PlacementGroupID *placement_group_id);

  /// Remove a placement group. Note that this operation is synchronous.
  ///
  /// \param[in] placement_group_id The id of a placement group to remove.
  /// \return Status OK if succeed. TimedOut if request to GCS server times out.
  /// NotFound if placement group is already removed or doesn't exist.
  Status RemovePlacementGroup(const PlacementGroupID &placement_group_id);

  /// Wait for a placement group until ready asynchronously.
  /// Returns once the placement group is created or the timeout expires.
  ///
  /// \param placement_group The id of a placement group to wait for.
  /// \param timeout_seconds Timeout in seconds.
  /// \return Status OK if the placement group is created. TimedOut if request to GCS
  /// server times out. NotFound if placement group is already removed or doesn't exist.
  Status WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                 int64_t timeout_seconds);

  /// Submit an actor task.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[in] max_retries max number of retry when the task fails.
  /// \param[in] serialized_retry_exception_allowlist A serialized exception list
  /// that serves as an allowlist of frontend-language exceptions/errors that should be
  /// retried. Empty string means an allow-all in the language worker.
  /// \param[in] call_site The stacktrace of the task invocation. This is
  /// only used for observability.
  /// \param[out] task_returns The object returned by this task
  /// param[in] current_task_id The current task_id that submits the task.
  /// If Nil() is given, it will be automatically propagated from worker_context.
  /// This is used when worker_context cannot reliably obtain the curernt task_id
  /// i.e., Python async actors.
  ///
  /// \return Status of this submission
  Status SubmitActorTask(const ActorID &actor_id,
                         const RayFunction &function,
                         const std::vector<std::unique_ptr<TaskArg>> &args,
                         const TaskOptions &task_options,
                         int max_retries,
                         bool retry_exceptions,
                         const std::string &serialized_retry_exception_allowlist,
                         const std::string &call_site,
                         std::vector<rpc::ObjectReference> &task_returns,
                         const TaskID current_task_id = TaskID::Nil());

  /// Tell an actor to exit immediately, without completing outstanding work.
  ///
  /// \param[in] actor_id ID of the actor to kill.
  /// \param[in] force_kill Whether to force kill an actor by killing the worker.
  /// \param[in] no_restart If set to true, the killed actor will not be
  /// restarted anymore.
  /// \param[out] Status
  Status KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Stops the task associated with the given Object ID.
  ///
  /// \param[in] object_id of the task to kill (must be a Non-Actor task)
  /// \param[in] force_kill Whether to force kill a task by killing the worker.
  /// \param[in] recursive Whether to cancel tasks submitted by the task to cancel.
  /// \param[out] Status
  Status CancelTask(const ObjectID &object_id, bool force_kill, bool recursive);

  /// Check if a task has been canceled.
  ///
  /// \param[in] task_id The task ID to check.
  /// \return Whether the task has been canceled.
  bool IsTaskCanceled(const TaskID &task_id) const;

  /// Decrease the reference count for this actor. Should be called by the
  /// language frontend when a reference to the ActorHandle destroyed.
  ///
  /// \param[in] actor_id The actor ID to decrease the reference count for.
  void RemoveActorHandleReference(const ActorID &actor_id);

  /// Get the local actor state. nullopt if the state is unknown.
  std::optional<rpc::ActorTableData::ActorState> GetLocalActorState(
      const ActorID &actor_id) const;

  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \param[in] add_local_ref Whether to add a local reference for this actor
  /// handle. Handles that were created out-of-band (i.e. via getting actor by
  /// name or getting a handle to self) should not add a local reference
  /// because the distributed reference counting protocol does not ensure that
  /// the owner will learn of this reference.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized,
                                            const ObjectID &outer_object_id,
                                            bool add_local_ref);

  /// Serialize an actor handle.
  ///
  /// This should be called when passing an actor handle to another task or
  /// actor.
  ///
  /// \param[in] actor_id The ID of the actor handle to serialize.
  /// \param[out] The serialized handle.
  /// \param[out] The ID used to track references to the actor handle. If the
  /// serialized actor handle in the language frontend is stored inside an
  /// object, then this must be recorded in the worker's ReferenceCounter.
  /// \return Status::Invalid if we don't have the specified handle.
  Status SerializeActorHandle(const ActorID &actor_id,
                              std::string *output,
                              ObjectID *actor_handle_id) const;

  ///
  /// Public methods related to task execution. Should not be used by driver processes.
  ///

  ActorID GetActorId() const {
    absl::MutexLock lock(&mutex_);
    return actor_id_;
  }

  std::string GetActorName() const;

  // Get the resource IDs available to this worker (as assigned by the raylet).
  ResourceMappingType GetResourceIDs() const;

  /// Create a profile event and push it the TaskEventBuffer when the event is destructed.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(const std::string &event_name);

 public:
  friend class CoreWorkerProcessImpl;

  /// Allocate the return object for an executing task. The caller should write into the
  /// data buffer of the allocated buffer, then call SealReturnObject() to seal it.
  /// To avoid deadlock, the caller should allocate and seal a single object at a time.
  ///
  /// \param[in] object_id Object ID of the return value.
  /// \param[in] data_size Size of the return value.
  /// \param[in] metadata Metadata buffer of the return value.
  /// \param[in] caller_address The address of the caller of the method.
  /// \param[in] contained_object_id ID serialized within each return object.
  /// \param[in][out] task_output_inlined_bytes Store the total size of all inlined
  /// objects of a task. It is used to decide if the current object should be inlined. If
  /// the current object is inlined, the task_output_inlined_bytes will be updated.
  /// \param[out] return_object RayObject containing buffers to write results into.
  /// \return Status.
  Status AllocateReturnObject(const ObjectID &object_id,
                              const size_t &data_size,
                              const std::shared_ptr<Buffer> &metadata,
                              const std::vector<ObjectID> &contained_object_id,
                              const rpc::Address &caller_address,
                              int64_t *task_output_inlined_bytes,
                              std::shared_ptr<RayObject> *return_object);

  /// Seal a return object for an executing task. The caller should already have
  /// written into the data buffer.
  ///
  /// \param[in] return_id Object ID of the return value.
  /// \param[in] return_object RayObject containing the buffer written info.
  /// \return Status.
  /// \param[in] generator_id For dynamically created objects, this is the ID
  /// of the object that wraps the dynamically created ObjectRefs in a
  /// generator. We use this to notify the owner of the dynamically created
  /// objects.
  Status SealReturnObject(const ObjectID &return_id,
                          const std::shared_ptr<RayObject> &return_object,
                          const ObjectID &generator_id,
                          const rpc::Address &caller_address);

  /// Pin the local copy of the return object, if one exists.
  ///
  /// \param[in] return_id ObjectID of the return value.
  /// \param[out] return_object The object that was pinned.
  /// \return success if the object still existed and was pinned. Note that
  /// pinning is done asynchronously.
  /// \param[in] generator_id For dynamically created objects, this is the ID
  /// of the object that wraps the dynamically created ObjectRefs in a
  /// generator. We use this to notify the owner of the dynamically created
  /// objects.
  /// \param[in] caller_address The address of the caller who is also the owner
  bool PinExistingReturnObject(const ObjectID &return_id,
                               std::shared_ptr<RayObject> *return_object,
                               const ObjectID &generator_id,
                               const rpc::Address &caller_address);

  /// Dynamically allocate an object.
  ///
  /// This should be used during task execution, if the task wants to return an
  /// object to the task caller and have the resulting ObjectRef be owned by
  /// the caller. This is in contrast to static allocation, where the caller
  /// decides at task invocation time how many returns the task should have.
  ///
  /// NOTE: Normally task_id and put_index it not necessary to be specified
  /// because we can obtain them from the global worker context. However,
  /// when the async actor uses this API, it cannot find the correct
  /// worker context due to the implementation limitation.
  /// In this case, the caller is responsible for providing the correct
  /// task ID and index.
  /// See https://github.com/ray-project/ray/issues/10324 for the further details.
  ///
  /// \param[in] owner_address The address of the owner who will own this
  /// dynamically generated object.
  /// \param[in] task_id The task id of the dynamically generated return ID.
  /// If Nil() is specified, it will deduce the Task ID from the current
  /// worker context.
  /// \param[in] put_index The equivalent of the return value of
  /// WorkerContext::GetNextPutIndex.
  /// If std::nullopt is specified, it will deduce the put index from the
  /// current worker context.
  ObjectID AllocateDynamicReturnId(const rpc::Address &owner_address,
                                   const TaskID &task_id = TaskID::Nil(),
                                   std::optional<ObjectIDIndexType> put_index = -1);

  /// Get a handle to an actor.
  ///
  /// NOTE: This function should be called ONLY WHEN we know actor handle exists.
  /// NOTE: The actor_handle obtained by this function should not be stored anywhere
  /// because this method returns the raw pointer to what a unique pointer points to.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \return Status::Invalid if we don't have this actor handle.
  std::shared_ptr<const ActorHandle> GetActorHandle(const ActorID &actor_id) const;

  /// Get a handle to a named actor.
  ///
  /// NOTE: The actor_handle obtained by this function should not be stored anywhere.
  ///
  /// \param[in] name The name of the actor whose handle to get.
  /// \param[in] ray_namespace The namespace of the requested actor.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return The shared_ptr to the actor handle if found, nullptr otherwise.
  /// The second pair contains the status of getting a named actor handle.
  std::pair<std::shared_ptr<const ActorHandle>, Status> GetNamedActorHandle(
      const std::string &name, const std::string &ray_namespace);

  /// Returns a list of the named actors currently in the system.
  ///
  /// Each actor is returned as a pair of <namespace, name>.
  /// This includes actors that are pending placement or being restarted.
  ///
  /// \param all_namespaces Whether or not to include actors from all namespaces.
  /// \return The list of <namespace, name> pairs and a status.
  std::pair<std::vector<std::pair<std::string, std::string>>, Status> ListNamedActors(
      bool all_namespaces);

  /// Get the expected return ids of the next task.
  std::vector<ObjectID> GetCurrentReturnIds(int num_returns,
                                            const ActorID &callee_actor_id);

  int64_t GetLocalMemoryStoreBytesUsed() const;

  /// The following methods are handlers for the core worker's gRPC server, which follow
  /// a macro-generated call convention. These are executed on the io_service_ and
  /// post work to the appropriate event loop.
  ///

  /// Implements gRPC server handler.
  void HandlePushTask(rpc::PushTaskRequest request,
                      rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleActorCallArgWaitComplete(rpc::ActorCallArgWaitCompleteRequest request,
                                      rpc::ActorCallArgWaitCompleteReply *reply,
                                      rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleRayletNotifyGCSRestart(rpc::RayletNotifyGCSRestartRequest request,
                                    rpc::RayletNotifyGCSRestartReply *reply,
                                    rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
                             rpc::GetObjectStatusReply *reply,
                             rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleWaitForActorRefDeleted(rpc::WaitForActorRefDeletedRequest request,
                                    rpc::WaitForActorRefDeletedReply *reply,
                                    rpc::SendReplyCallback send_reply_callback);

  // Implements gRPC server handler.
  void HandlePubsubLongPolling(rpc::PubsubLongPollingRequest request,
                               rpc::PubsubLongPollingReply *reply,
                               rpc::SendReplyCallback send_reply_callback);

  // Implements gRPC server handler.
  void HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request,
                                rpc::PubsubCommandBatchReply *reply,
                                rpc::SendReplyCallback send_reply_callback);

  // Implements gRPC server handler.
  void HandleUpdateObjectLocationBatch(rpc::UpdateObjectLocationBatchRequest request,
                                       rpc::UpdateObjectLocationBatchReply *reply,
                                       rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleGetObjectLocationsOwner(rpc::GetObjectLocationsOwnerRequest request,
                                     rpc::GetObjectLocationsOwnerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleKillActor(rpc::KillActorRequest request,
                       rpc::KillActorReply *reply,
                       rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleCancelTask(rpc::CancelTaskRequest request,
                        rpc::CancelTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandleRequestOwnerToCancelTask(rpc::RequestOwnerToCancelTaskRequest request,
                                      rpc::RequestOwnerToCancelTaskReply *reply,
                                      rpc::SendReplyCallback send_reply_callback);

  /// Implements gRPC server handler.
  void HandlePlasmaObjectReady(rpc::PlasmaObjectReadyRequest request,
                               rpc::PlasmaObjectReadyReply *reply,
                               rpc::SendReplyCallback send_reply_callback);

  /// Creates a new mutable object.
  void HandleRegisterMutableObjectReader(rpc::RegisterMutableObjectReaderRequest request,
                                         rpc::RegisterMutableObjectReaderReply *reply,
                                         rpc::SendReplyCallback send_reply_callback);

  /// Get statistics from core worker.
  void HandleGetCoreWorkerStats(rpc::GetCoreWorkerStatsRequest request,
                                rpc::GetCoreWorkerStatsReply *reply,
                                rpc::SendReplyCallback send_reply_callback);

  /// Trigger local GC on this worker.
  void HandleLocalGC(rpc::LocalGCRequest request,
                     rpc::LocalGCReply *reply,
                     rpc::SendReplyCallback send_reply_callback);

  /// Delete objects explicitly.
  void HandleDeleteObjects(rpc::DeleteObjectsRequest request,
                           rpc::DeleteObjectsReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  // Spill objects to external storage.
  void HandleSpillObjects(rpc::SpillObjectsRequest request,
                          rpc::SpillObjectsReply *reply,
                          rpc::SendReplyCallback send_reply_callback);

  // Restore objects from external storage.
  void HandleRestoreSpilledObjects(rpc::RestoreSpilledObjectsRequest request,
                                   rpc::RestoreSpilledObjectsReply *reply,
                                   rpc::SendReplyCallback send_reply_callback);

  // Delete objects from external storage.
  void HandleDeleteSpilledObjects(rpc::DeleteSpilledObjectsRequest request,
                                  rpc::DeleteSpilledObjectsReply *reply,
                                  rpc::SendReplyCallback send_reply_callback);

  // Make the this worker exit.
  // This request fails if the core worker owns any object.
  void HandleExit(rpc::ExitRequest request,
                  rpc::ExitReply *reply,
                  rpc::SendReplyCallback send_reply_callback);

  // Set local worker as the owner of object.
  // Request by borrower's worker, execute by owner's worker.
  void HandleAssignObjectOwner(rpc::AssignObjectOwnerRequest request,
                               rpc::AssignObjectOwnerReply *reply,
                               rpc::SendReplyCallback send_reply_callback);

  // Get the number of pending tasks.
  void HandleNumPendingTasks(rpc::NumPendingTasksRequest request,
                             rpc::NumPendingTasksReply *reply,
                             rpc::SendReplyCallback send_reply_callback);

  ///
  /// Public methods related to async actor call. This should only be used when
  /// the actor is (1) direct actor and (2) using async mode.
  ///

  /// Block current fiber until event is triggered.
  void YieldCurrentFiber(FiberEvent &event);

  /// The callback expected to be implemented by the client.
  using SetResultCallback =
      std::function<void(std::shared_ptr<RayObject>, ObjectID object_id, void *)>;

  using OnCanceledCallback = std::function<void(bool, bool)>;

  /// Perform async get from the object store.
  ///
  /// \param[in] object_id The id to call get on.
  /// \param[in] success_callback The callback to use the result object.
  /// \param[in] python_user_callback The user-provided Python callback object that
  /// will be called inside of `success_callback`.
  void GetAsync(const ObjectID &object_id,
                SetResultCallback success_callback,
                void *python_user_callback);

  // Get serialized job configuration.
  rpc::JobConfig GetJobConfig() const;

  /// Return true if the core worker is in the exit process.
  bool IsExiting() const;

  /// Mark this worker is exiting.
  void SetIsExiting();

  /// Add task log info for a task when it starts executing.
  ///
  /// It's an no-op in local mode.
  ///
  /// \param stdout_path Path to stdout log file.
  /// \param stderr_path Path to stderr log file.
  /// \param stdout_start_offset Start offset of the stdout for this task.
  /// \param stderr_start_offset Start offset of the stderr for this task.
  void RecordTaskLogStart(const TaskID &task_id,
                          int32_t attempt_number,
                          const std::string &stdout_path,
                          const std::string &stderr_path,
                          int64_t stdout_start_offset,
                          int64_t stderr_start_offset) const;

  /// Add task log info for a task when it finishes executing.
  ///
  /// It's an no-op in local mode.
  ///
  /// \param stdout_end_offset End offset of the stdout for this task.
  /// \param stderr_end_offset End offset of the stderr for this task.
  void RecordTaskLogEnd(const TaskID &task_id,
                        int32_t attempt_number,
                        int64_t stdout_end_offset,
                        int64_t stderr_end_offset) const;

  /// (WORKER mode only) Gracefully exit the worker. `Graceful` means the worker will
  /// exit when it drains all tasks and cleans all owned objects.
  /// After this method is called, all the tasks in the queue will not be
  /// executed.
  ///
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  /// \param creation_task_exception_pb_bytes It is given when the worker is
  /// disconnected because the actor is failed due to its exception in its init method.
  void Exit(const rpc::WorkerExitType exit_type,
            const std::string &detail,
            const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes =
                nullptr);

  void AsyncRetryTask(TaskSpecification &spec, uint32_t delay_ms);

 private:
  static nlohmann::json OverrideRuntimeEnv(const nlohmann::json &child,
                                           const std::shared_ptr<nlohmann::json> &parent);

  /// The following tests will use `OverrideRuntimeEnv` function.
  FRIEND_TEST(TestOverrideRuntimeEnv, TestOverrideEnvVars);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestPyModulesInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestOverridePyModules);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestWorkingDirInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestWorkingDirOverride);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestCondaInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestCondaOverride);

  /// Used to lazily subscribe to node_changes only if the worker takes any owner actions.
  void SubscribeToNodeChanges();

  std::shared_ptr<rpc::RuntimeEnvInfo> OverrideTaskOrActorRuntimeEnvInfo(
      const std::string &serialized_runtime_env_info) const;

  // Used as the factory function for [OverrideTaskOrActorRuntimeEnvInfo] to create in LRU
  // cache.
  std::shared_ptr<rpc::RuntimeEnvInfo> OverrideTaskOrActorRuntimeEnvInfoImpl(
      const std::string &serialized_runtime_env_info) const;

  void BuildCommonTaskSpec(
      TaskSpecBuilder &builder,
      const JobID &job_id,
      const TaskID &task_id,
      const std::string &name,
      const TaskID &current_task_id,
      uint64_t task_index,
      const TaskID &caller_id,
      const rpc::Address &address,
      const RayFunction &function,
      const std::vector<std::unique_ptr<TaskArg>> &args,
      int64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      const std::string &debugger_breakpoint,
      int64_t depth,
      const std::string &serialized_runtime_env_info,
      const std::string &call_site,
      const TaskID &main_thread_current_task_id,
      const std::string &concurrency_group_name = "",
      bool include_job_config = false,
      int64_t generator_backpressure_num_objects = -1,
      bool enable_task_events = true,
      const std::unordered_map<std::string, std::string> &labels = {},
      const LabelSelector &label_selector = {},
      const std::vector<FallbackOption> &fallback_strategy = {},
      const rpc::TensorTransport &tensor_transport = rpc::TensorTransport::OBJECT_STORE);
  void SetCurrentTaskId(const TaskID &task_id,
                        uint64_t attempt_number,
                        const std::string &task_name);

  void SetActorId(const ActorID &actor_id);

  /// Forcefully exit the worker. `Force` means it will exit actor without draining
  /// or cleaning any resources.
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  void ForceExit(const rpc::WorkerExitType exit_type, const std::string &detail);

  /// Forcefully kill child processes. User code running in actors or tasks
  /// can spawn processes that don't get terminated. If those processes
  /// own resources (such as GPU memory), then those resources will become
  /// unavailable until the process is killed.
  /// This is called during shutdown of the process.
  void KillChildProcs();

  /// Register this worker or driver to GCS.
  void RegisterToGcs(int64_t worker_launch_time_ms, int64_t worker_launched_time_ms);

  /// (WORKER mode only) Check if the raylet has failed. If so, shutdown.
  void ExitIfParentRayletDies();

  /// Heartbeat for internal bookkeeping.
  void InternalHeartbeat();

  /// Record metric for executed and owned tasks. Will be run periodically.
  void RecordMetrics();

  /// Check if there is an owner of the object from the ReferenceCounter.
  bool HasOwner(const ObjectID &object_id) const;

  /// Helper method to fill in object status reply given an object.
  void PopulateObjectStatus(const ObjectID &object_id,
                            const std::shared_ptr<RayObject> &obj,
                            rpc::GetObjectStatusReply *reply);

  ///
  /// Private methods related to task submission.
  ///

  /// Increase the local reference count for this object ID. Should be called
  /// by the language frontend when a new reference is created.
  ///
  /// \param[in] object_id The object ID to increase the reference count for.
  /// \param[in] call_site The call site from the language frontend.
  void AddLocalReference(const ObjectID &object_id, const std::string &call_site) {
    reference_counter_->AddLocalReference(object_id, call_site);
  }

  /// Stops the children tasks from the given TaskID
  ///
  /// \param[in] task_id of the parent task
  /// \param[in] force_kill Whether to force kill a task by killing the worker.
  Status CancelChildren(const TaskID &task_id, bool force_kill);

  ///
  /// Private methods related to task execution. Should not be used by driver processes.
  ///

  /// Execute a task.
  ///
  /// \param spec[in] task_spec Task specification.
  /// \param spec[in] resource_ids Resource IDs of resources assigned to this
  ///                 worker. If nullopt, reuse the previously assigned
  ///                 resources.
  /// \param results[out] return_objects Result objects that should be returned
  /// to the caller.
  /// \param results[out] dynamic_return_objects Result objects whose
  /// ObjectRefs were dynamically allocated during task execution by using a
  /// generator. The language-level ObjectRefs should be returned inside the
  /// statically allocated return_objects.
  /// \param results[out] borrowed_refs Refs that this task (or a nested task)
  ///                     was or is still borrowing. This includes all
  ///                     objects whose IDs we passed to the task in its
  ///                     arguments and recursively, any object IDs that were
  ///                     contained in those objects.
  /// \param results[out] is_retryable_error Whether the task failed with a retryable
  ///                     error.
  /// \param results[out] application_error The error message if the
  ///                     task failed during execution or cancelled.
  /// \return Status.
  Status ExecuteTask(
      const TaskSpecification &task_spec,
      std::optional<ResourceMappingType> resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
          *dynamic_return_objects,
      std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
      ReferenceCounterInterface::ReferenceTableProto *borrowed_refs,
      bool *is_retryable_error,
      std::string *application_error);

  /// Put an object in the local plasma store.
  ///
  /// Return status semantics:
  /// - Status::OK(): The object was created (or already existed) and bookkeeping was
  ///   updated. Note: an internal ObjectExists from the plasma provider is treated
  ///   as OK and does not surface here.
  /// - Status::ObjectStoreFull(): The local plasma store is out of memory (or out of
  ///   disk when spilling). The error message contains context and a short memory
  ///   report.
  /// - Status::IOError(): IPC/connection failures while talking to the plasma store
  ///   (e.g., broken pipe/connection reset during shutdown, store not reachable).
  ///
  /// Call sites that run during shutdown may choose to tolerate IOError specifically,
  /// but should treat all other statuses as real failures.
  Status PutInLocalPlasmaStore(const RayObject &object,
                               const ObjectID &object_id,
                               bool pin_object);

  /// Execute a local mode task (runs normal ExecuteTask)
  ///
  /// \param spec[in] task_spec Task specification.
  std::vector<rpc::ObjectReference> ExecuteTaskLocalMode(
      const TaskSpecification &task_spec, const ActorID &actor_id = ActorID::Nil());

  /// KillActor API for a local mode.
  Status KillActorLocalMode(const ActorID &actor_id);

  /// Get a handle to a named actor for local mode.
  std::pair<std::shared_ptr<const ActorHandle>, Status> GetNamedActorHandleLocalMode(
      const std::string &name);

  /// Get all named actors in local mode.
  std::pair<std::vector<std::pair<std::string, std::string>>, Status>
  ListNamedActorsLocalMode();

  /// Get the values of the task arguments for the executor. Values are
  /// retrieved from the local plasma store or, if the value is inlined, from
  /// the task spec.
  ///
  /// This also pins all plasma arguments and ObjectIDs that were contained in
  /// an inlined argument by adding a local reference in the reference counter.
  /// This is to ensure that we have the address of the object's owner, which
  /// is needed to retrieve the value. It also ensures that when the task
  /// completes, we can retrieve any metadata about objects that are still
  /// being borrowed by this process. The IDs should be unpinned once the task
  /// completes.
  ///
  /// \param spec[in] task Task specification.
  /// \param args[out] args Argument data as RayObjects.
  /// \param args[out] arg_reference_ids ObjectIDs corresponding to each by
  ///                  reference argument. The length of this vector will be
  ///                  the same as args, and by value arguments will have
  ///                  ObjectID::Nil().
  ///                  // TODO(edoakes): this is a bit of a hack that's necessary because
  ///                  we have separate serialization paths for by-value and by-reference
  ///                  arguments in Python. This should ideally be handled better there.
  /// \param args[out] pinned_ids ObjectIDs that should be unpinned once the
  ///                  task completes execution.  This vector will be populated
  ///                  with all argument IDs that were passed by reference and
  ///                  any ObjectIDs that were included in the task spec's
  ///                  inlined arguments.
  /// \return Error if the values could not be retrieved.
  Status GetAndPinArgsForExecutor(const TaskSpecification &task,
                                  std::vector<std::shared_ptr<RayObject>> *args,
                                  std::vector<rpc::ObjectReference> *arg_refs,
                                  std::vector<ObjectID> *pinned_ids);

  /// Process a subscribe message for wait for object eviction.
  /// The object eviction message will be published once the object
  /// needs to be evicted.
  void ProcessSubscribeForObjectEviction(
      const rpc::WorkerObjectEvictionSubMessage &message);

  /// Process a subscribe message for wait for ref removed.
  /// It is used for the ref counting protocol. When the borrower
  /// stops using the reference, the message will be published to the owner.
  void ProcessSubscribeForRefRemoved(const rpc::WorkerRefRemovedSubMessage &message);

  /// Process a subscribe message for object locations.
  /// Since core worker owns the object directory, there are various raylets
  /// that subscribe this object directory.
  void ProcessSubscribeObjectLocations(
      const rpc::WorkerObjectLocationsSubMessage &message);

  using Commands = ::google::protobuf::RepeatedPtrField<rpc::Command>;

  /// Process the subscribe message received from the subscriber.
  void ProcessSubscribeMessage(const rpc::SubMessage &sub_message,
                               rpc::ChannelType channel_type,
                               const std::string &key_id,
                               const NodeID &subscriber_id);

  /// A single endpoint to process different types of pubsub commands.
  /// Pubsub commands are coming as a batch and contain various subscribe / unbsubscribe
  /// messages.
  void ProcessPubsubCommands(const Commands &commands, const NodeID &subscriber_id);

  void AddSpilledObjectLocationOwner(const ObjectID &object_id,
                                     const std::string &spilled_url,
                                     const NodeID &spilled_node_id,
                                     const std::optional<ObjectID> &generator_id);

  void AddObjectLocationOwner(const ObjectID &object_id, const NodeID &node_id);

  void RemoveObjectLocationOwner(const ObjectID &object_id, const NodeID &node_id);

  /// Returns whether the message was sent to the wrong worker. The right error reply
  /// is sent automatically. Messages end up on the wrong worker when a worker dies
  /// and a new one takes its place with the same place. In this situation, we want
  /// the new worker to reject messages meant for the old one.
  bool HandleWrongRecipient(const WorkerID &intended_worker_id,
                            const rpc::SendReplyCallback &send_reply_callback) {
    if (intended_worker_id != worker_context_->GetWorkerID()) {
      std::ostringstream stream;
      stream << "Mismatched WorkerID: ignoring RPC for previous worker "
             << intended_worker_id
             << ", current worker ID: " << worker_context_->GetWorkerID();
      auto msg = stream.str();
      RAY_LOG(ERROR) << msg;
      send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
      return true;
    } else {
      return false;
    }
  }

  const CoreWorkerOptions options_;

  /// Callback to get the current language (e.g., Python) call site.
  std::function<void(std::string *)> get_call_site_;

  // Convenience method to get the current language call site.
  std::string CurrentCallSite() {
    std::string call_site;
    if (get_call_site_ != nullptr) {
      get_call_site_(&call_site);
    }
    return call_site;
  }

  Status WaitForActorRegistered(const std::vector<ObjectID> &ids);

  /// Cancel a normal task (non-actor-task) queued or running in the current worker.
  ///
  /// \param intended_task_id The ID of a task to cancel.
  /// \param force_kill If true, kill the worker.
  /// \param recursive If true, cancel all children tasks of the intended_task_id.
  /// \param on_canceled Callback called after a task is canceled.
  /// It has two inputs, which corresponds to requested_task_running (if task is still
  /// running after a cancelation attempt is done) and attempt_succeeded (if task
  /// is canceled, and a caller doesn't have to retry).
  void CancelTaskOnExecutor(TaskID intended_task_id,
                            bool force_kill,
                            bool recursive,
                            const OnCanceledCallback &on_canceled);

  // Attempt to cancel the actor task.
  //
  // The callback will be called with `success` to indicate if the cancellation succeeded.
  // If not, the caller must retry the cancellation request until either it succeeds or
  // the task finishes executing.
  //
  // A task can be in one of three states locally:
  //
  // 1) Not present in the local task receiver.
  //    This means it wasn't received yet or it already finished executing.
  // 2) Queued in the local task receiver, but not executing yet.
  // 3) Executing.
  //
  // We first check if the task is present in the local receiver. If not, we
  // do nothing and return success=false.
  //
  // If the task *is* present in the local receiver, we attempt to cancel it.
  // The task may already be running, in which case we cancel it during execution.
  //
  // NOTE: only async actor tasks can be cancelled during execution. For non-async
  // actor tasks that are already executing, we will return success=true to prevent the
  // client from retrying infinitely.
  void CancelActorTaskOnExecutor(WorkerID caller_worker_id,
                                 TaskID intended_task_id,
                                 bool force_kill,
                                 bool recursive,
                                 OnCanceledCallback on_canceled);

  /// Helper for Get.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status GetObjects(const std::vector<ObjectID> &ids,
                    const int64_t timeout_ms,
                    std::vector<std::shared_ptr<RayObject>> &results);

  /// Helper to compute idleness from precomputed counters.
  ///
  /// We consider the worker to be idle if it doesn't have object references and it
  /// doesn't have any object pinning RPCs in flight and it doesn't have pending tasks.
  bool IsIdle(size_t num_objects_with_references,
              int64_t pins_in_flight,
              size_t num_pending_tasks) const;

  /// Convenience overload that fetches counters and evaluates idleness.
  bool IsIdle() const;

  /// Get the caller ID used to submit tasks from this worker to an actor.
  ///
  /// \return The caller ID. For non-actor tasks, this is the current task ID.
  /// For actors, this is the current actor ID. To make sure that all caller
  /// IDs have the same type, we embed the actor ID in a TaskID with the rest
  /// of the bytes zeroed out.
  TaskID GetCallerId() const ABSL_LOCKS_EXCLUDED(mutex_);

  /// Helper for Get, used only to read experimental mutable objects.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Time out in milliseconds to get the objects.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status GetExperimentalMutableObjects(const std::vector<ObjectID> &ids,
                                       int64_t timeout_ms,
                                       std::vector<std::shared_ptr<RayObject>> &results);

  /// Sends AnnounceWorkerPort to the GCS. Called in ctor and also in ConnectToRaylet.
  void ConnectToRayletInternal();

  // Fallback for when GetAsync cannot directly get the requested object.
  void PlasmaCallback(const SetResultCallback &success,
                      const std::shared_ptr<RayObject> &ray_object,
                      ObjectID object_id,
                      void *py_future);

  /// Shared state of the worker. Includes process-level and thread-level state.
  /// TODO(edoakes): we should move process-level state into this class and make
  /// this a ThreadContext.
  std::unique_ptr<WorkerContext> worker_context_;

  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_ ABSL_GUARDED_BY(mutex_);

  std::string main_thread_task_name_ ABSL_GUARDED_BY(mutex_);

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  instrumented_io_context &io_service_;

  /// Shared core worker client pool.
  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;

  // Shared raylet client pool.
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;

  std::shared_ptr<PeriodicalRunnerInterface> periodical_runner_;

  /// RPC server used to receive tasks to execute.
  std::unique_ptr<rpc::GrpcServer> core_worker_server_;

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  /// Whether or not this worker is connected to the raylet and GCS.
  bool connected_ = false;

  // Client to the GCS shared by core worker interfaces.
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  // Client to the local Raylet that goes over a local socket.
  std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client_;

  // Client to the local Raylet that goes over a gRPC connection.
  std::shared_ptr<RayletClientInterface> local_raylet_rpc_client_;

  // Thread that runs a boost::asio service to process IO events.
  boost::thread &io_thread_;

  // Keeps track of object ID reference counts.
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;

  ///
  /// Fields related to storing and retrieving objects.
  ///

  /// In-memory store for return objects.
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  /// Plasma store interface.
  std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider_;

  /// Manages mutable objects that must be transferred across nodes.
  std::shared_ptr<experimental::MutableObjectProviderInterface>
      experimental_mutable_object_provider_;

  std::unique_ptr<FutureResolver> future_resolver_;

  ///
  /// Fields related to task submission.
  ///

  // Tracks the currently pending tasks.
  std::shared_ptr<TaskManager> task_manager_;

  // A class for actor creation.
  std::shared_ptr<ActorCreatorInterface> actor_creator_;

  // Interface to submit tasks directly to other actors.
  std::unique_ptr<ActorTaskSubmitter> actor_task_submitter_;

  // A class to publish object status from other raylets/workers.
  std::unique_ptr<pubsub::PublisherInterface> object_info_publisher_;

  // A class to subscribe object status from other raylets/workers.
  std::unique_ptr<pubsub::SubscriberInterface> object_info_subscriber_;

  // Rate limit the concurrent pending lease requests for submitting
  // tasks.
  std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter_;

  // Interface to submit non-actor tasks directly to leased workers.
  std::unique_ptr<NormalTaskSubmitter> normal_task_submitter_;

  /// Manages recovery of objects stored in remote plasma nodes.
  std::unique_ptr<ObjectRecoveryManager> object_recovery_manager_;

  ///
  /// Fields related to actor handles.
  ///

  /// Interface to manage actor handles.
  std::unique_ptr<ActorManager> actor_manager_;

  ///
  /// Fields related to task execution.
  ///

  /// Protects around accesses to fields below. This should only ever be held
  /// for short-running periods of time.
  mutable absl::Mutex mutex_;

  /// Our actor ID. If this is nil, then we execute only stateless tasks.
  ActorID actor_id_ ABSL_GUARDED_BY(mutex_);

  /// Set of currently-running tasks. For single-threaded, non-async actors this will
  /// contain at most one task ID.
  ///
  /// We have to track this separately because we cannot access the thread-local worker
  /// contexts from GetCoreWorkerStats().
  absl::flat_hash_map<TaskID, TaskSpecification> running_tasks_ ABSL_GUARDED_BY(mutex_);

  /// Tracks which tasks have been marked as canceled. For single-threaded, non-async
  /// actors this will contain at most one task ID.
  ///
  /// We have to track this separately because cancellation requests come from submitter
  /// thread than the thread executing the task, so we cannot get the cancellation status
  /// from the thread-local WorkerThreadContext.
  absl::flat_hash_set<TaskID> canceled_tasks_ ABSL_GUARDED_BY(mutex_);

  /// Key value pairs to be displayed on Web UI.
  std::unordered_map<std::string, std::string> webui_display_ ABSL_GUARDED_BY(mutex_);

  /// Actor repr name if overrides by the user, empty string if not.
  std::string actor_repr_name_ ABSL_GUARDED_BY(mutex_);

  /// Number of tasks that have been pushed to the actor but not executed.
  std::atomic<int64_t> task_queue_length_;

  /// Number of executed tasks.
  std::atomic<int64_t> num_executed_tasks_;

  // Number of in flight argument pinning requests used for metric reporting only
  std::atomic<int64_t> num_get_pin_args_in_flight_;

  // Number of failed argument pinning requests used for metric reporting only
  std::atomic<int64_t> num_failed_get_pin_args_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker. This is set on task assignment.
  ResourceMappingType resource_ids_ ABSL_GUARDED_BY(mutex_);

  /// Used to notify the task receiver when the arguments of a queued
  /// actor task are ready.
  std::unique_ptr<DependencyWaiterImpl> task_argument_waiter_;

  // Interface that receives tasks from direct actor calls.
  std::unique_ptr<TaskReceiver> task_receiver_;

  /// Event loop where tasks are processed.
  /// task_execution_service_ should be destructed first to avoid
  /// issues like https://github.com/ray-project/ray/issues/18857
  instrumented_io_context &task_execution_service_;

  // Queue of tasks to resubmit when the specified time passes.
  std::priority_queue<TaskToRetry, std::deque<TaskToRetry>, TaskToRetryDescComparator>
      to_resubmit_ ABSL_GUARDED_BY(mutex_);

  /// Map of named actor registry. It doesn't need to hold a lock because
  /// local mode is single-threaded.
  absl::flat_hash_map<std::string, ActorID> local_mode_named_actor_registry_;

  // Guard for `async_plasma_callbacks_` map.
  mutable absl::Mutex plasma_mutex_;

  // Callbacks for when when a plasma object becomes ready.
  absl::flat_hash_map<ObjectID, std::vector<std::function<void(void)>>>
      async_plasma_callbacks_ ABSL_GUARDED_BY(plasma_mutex_);

  /// The detail reason why the core worker has exited.
  /// If this value is set, it means the exit process has begun.
  std::optional<std::string> exiting_detail_ ABSL_GUARDED_BY(mutex_);

  /// Unified shutdown coordinator that manages all shutdown operations.
  /// Implements a thread-safe, single state machine that coordinates
  /// all shutdown entry points.
  std::unique_ptr<ShutdownCoordinator> shutdown_coordinator_;

  int64_t max_direct_call_object_size_;

  TaskCounter task_counter_;

  /// Used to guarantee that submitting actor task is thread safe.
  /// NOTE(MissiontoMars,scv119): In particular, without this mutex,
  /// the checking and increasing of backpressure pending calls counter
  /// is not atomic, which may lead to under counting or over counting.
  absl::Mutex actor_task_mutex_;

  /// A shared pointer between various components that emitting task state events.
  /// e.g. CoreWorker, TaskManager.
  std::unique_ptr<worker::TaskEventBuffer> task_event_buffer_ = nullptr;

  /// Worker's PID
  uint32_t pid_;

  /// Callback to cleanup actor instance before shutdown
  std::function<void()> actor_shutdown_callback_;

  // Guards generator_ids_pending_deletion_.
  absl::Mutex generator_ids_pending_deletion_mutex_;

  // A set of generator IDs that have gone out of scope but couldn't be deleted from
  // the task manager yet (e.g., due to lineage references). We will periodically
  // attempt to delete them in the background until it succeeds.
  // This field is accessed on the destruction path of an ObjectRefGenerator as well as
  // by a background thread attempting later deletion, so it must be guarded by a lock.
  absl::flat_hash_set<ObjectID> generator_ids_pending_deletion_
      ABSL_GUARDED_BY(generator_ids_pending_deletion_mutex_);

  /// TODO(hjiang):
  /// 1. Cached job runtime env info, it's not implemented at first place since
  /// `OverrideRuntimeEnv` mutates parent job runtime env info.
  /// 2. Cleanup cache on job change.
  ///
  /// Maps serialized runtime env info to **immutable** deserialized protobuf.
  mutable utils::container::ThreadSafeSharedLruCache<std::string, rpc::RuntimeEnvInfo>
      runtime_env_json_serialization_cache_;

  /// Used to ensure we only subscribe to node changes once.
  std::once_flag subscribe_to_node_changes_flag_;

  // Grant CoreWorkerShutdownExecutor access to CoreWorker internals for orchestrating
  // the shutdown procedure without exposing additional public APIs.
  friend class CoreWorkerShutdownExecutor;

  /// Used to block in certain spots if the GCS node address and liveness cache is needed.
  std::mutex gcs_client_node_cache_populated_mutex_;
  std::condition_variable gcs_client_node_cache_populated_cv_;
  bool gcs_client_node_cache_populated_ = false;

  /// Callback to free an RDT object when it is out of scope.
  std::function<void(const ObjectID &)> free_actor_object_callback_;
};
}  // namespace ray::core
