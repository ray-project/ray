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

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/buffer.h"
#include "ray/common/placement_group.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/profile_event.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_server.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/pubsub.pb.h"

/// The set of gRPC handlers and their associated level of concurrency. If you want to
/// add a new call to the worker gRPC server, do the following:
/// 1) Add the rpc to the CoreWorkerService in core_worker.proto, e.g., "ExampleCall"
/// 2) Add a new macro to RAY_CORE_WORKER_DECLARE_RPC_HANDLERS
///    in core_worker_server.h,
//     e.g. "DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(ExampleCall)"
/// 3) Add a new macro to RAY_CORE_WORKER_RPC_HANDLERS in core_worker_server.h, e.g.
///    "RPC_SERVICE_HANDLER(CoreWorkerService, ExampleCall, 1)"
/// 4) Add a method to the CoreWorker class below: "CoreWorker::HandleExampleCall"

namespace ray {
namespace core {

JobID GetProcessJobID(const CoreWorkerOptions &options);

/// Tracks stats for inbound tasks (tasks this worker is executing).
/// The counters are keyed by the function name in task spec.
class TaskCounter {
  /// A task can only be one of the following state. Received state in particular
  /// covers from the point of RPC call to beginning execution.
  enum TaskStatusType { kPending, kRunning, kFinished };

 public:
  TaskCounter() {
    counter_.SetOnChangeCallback(
        [this](const std::tuple<std::string, TaskStatusType, bool> &key)
            EXCLUSIVE_LOCKS_REQUIRED(&mu_) mutable {
              if (std::get<1>(key) != kRunning) {
                return;
              }
              auto func_name = std::get<0>(key);
              auto is_retry = std::get<2>(key);
              int64_t running_total = counter_.Get(key);
              int64_t num_in_get = running_in_get_counter_.Get({func_name, is_retry});
              int64_t num_in_wait = running_in_wait_counter_.Get({func_name, is_retry});
              auto is_retry_label = is_retry ? "1" : "0";
              // RUNNING_IN_RAY_GET/WAIT are sub-states of RUNNING, so we need to subtract
              // them out to avoid double-counting.
              ray::stats::STATS_tasks.Record(
                  running_total - num_in_get - num_in_wait,
                  {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING)},
                   {"Name", func_name},
                   {"IsRetry", is_retry_label},
                   {"Source", "executor"}});
              // Negate the metrics recorded from the submitter process for these tasks.
              ray::stats::STATS_tasks.Record(
                  -running_total,
                  {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::SUBMITTED_TO_WORKER)},
                   {"Name", func_name},
                   {"IsRetry", is_retry_label},
                   {"Source", "executor"}});
              // Record sub-state for get.
              ray::stats::STATS_tasks.Record(
                  num_in_get,
                  {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_GET)},
                   {"Name", func_name},
                   {"IsRetry", is_retry_label},
                   {"Source", "executor"}});
              // Record sub-state for wait.
              ray::stats::STATS_tasks.Record(
                  num_in_wait,
                  {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_WAIT)},
                   {"Name", func_name},
                   {"IsRetry", is_retry_label},
                   {"Source", "executor"}});
            });
  }

  void BecomeActor(const std::string &actor_name) {
    absl::MutexLock l(&mu_);
    actor_name_ = actor_name;
  }

  bool IsActor() EXCLUSIVE_LOCKS_REQUIRED(&mu_) { return actor_name_.size() > 0; }

  void RecordMetrics() {
    absl::MutexLock l(&mu_);
    counter_.FlushOnChangeCallbacks();
    if (IsActor()) {
      float running = 0.0;
      float in_get = 0.0;
      float in_wait = 0.0;
      if (running_in_wait_counter_.Total() > 0) {
        in_wait = 1.0;
      } else if (running_in_get_counter_.Total() > 0) {
        in_get = 1.0;
      } else if (num_tasks_running_ > 0) {
        running = 1.0;
      }
      ray::stats::STATS_actors.Record(
          -(running + in_get + in_wait),
          {{"State", "ALIVE"}, {"Name", actor_name_}, {"Source", "executor"}});
      ray::stats::STATS_actors.Record(
          running,
          {{"State", "RUNNING_TASK"}, {"Name", actor_name_}, {"Source", "executor"}});
      ray::stats::STATS_actors.Record(in_get,
                                      {{"State", "RUNNING_IN_RAY_GET"},
                                       {"Name", actor_name_},
                                       {"Source", "executor"}});
      ray::stats::STATS_actors.Record(in_wait,
                                      {{"State", "RUNNING_IN_RAY_WAIT"},
                                       {"Name", actor_name_},
                                       {"Source", "executor"}});
    }
  }

  void IncPending(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Increment({func_name, kPending, is_retry});
  }

  void MovePendingToRunning(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Swap({func_name, kPending, is_retry}, {func_name, kRunning, is_retry});
    num_tasks_running_++;
  }

  void MoveRunningToFinished(const std::string &func_name, bool is_retry) {
    absl::MutexLock l(&mu_);
    counter_.Swap({func_name, kRunning, is_retry}, {func_name, kFinished, is_retry});
    num_tasks_running_--;
    RAY_CHECK(num_tasks_running_ >= 0);
  }

  void SetMetricStatus(const std::string &func_name,
                       rpc::TaskStatus status,
                       bool is_retry) {
    absl::MutexLock l(&mu_);
    if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
      running_in_get_counter_.Increment({func_name, is_retry});
    } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
      running_in_wait_counter_.Increment({func_name, is_retry});
    } else {
      RAY_CHECK(false) << "Unexpected status " << rpc::TaskStatus_Name(status);
    }
  }

  void UnsetMetricStatus(const std::string &func_name,
                         rpc::TaskStatus status,
                         bool is_retry) {
    absl::MutexLock l(&mu_);
    if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
      running_in_get_counter_.Decrement({func_name, is_retry});
    } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
      running_in_wait_counter_.Decrement({func_name, is_retry});
    } else {
      RAY_CHECK(false) << "Unexpected status " << rpc::TaskStatus_Name(status);
    }
  }

  std::unordered_map<std::string, std::vector<int64_t>> AsMap() const {
    absl::MutexLock l(&mu_);
    std::unordered_map<std::string, std::vector<int64_t>> total_counts;

    counter_.ForEachEntry(
        [&total_counts](const std::tuple<std::string, TaskStatusType, bool> &key,
                        int64_t value) mutable {
          auto func_name = std::get<0>(key);
          auto status = std::get<1>(key);
          total_counts[func_name].resize(3, 0);
          if (status == kPending) {
            total_counts[func_name][0] = value;
          } else if (status == kRunning) {
            total_counts[func_name][1] = value;
          } else if (status == kFinished) {
            total_counts[func_name][2] = value;
          } else {
            RAY_CHECK(false) << "Invalid task status type " << status;
          }
        });

    return total_counts;
  }

 private:
  mutable absl::Mutex mu_;
  // Tracks all tasks submitted to this worker by state, is_retry.
  CounterMap<std::tuple<std::string, TaskStatusType, bool>> counter_ GUARDED_BY(&mu_);

  // Additionally tracks the sub-states of RUNNING_IN_RAY_GET/WAIT. The counters here
  // overlap with those of counter_.
  CounterMap<std::pair<std::string, bool>> running_in_get_counter_ GUARDED_BY(&mu_);
  CounterMap<std::pair<std::string, bool>> running_in_wait_counter_ GUARDED_BY(&mu_);

  // Used for actor state tracking.
  std::string actor_name_ GUARDED_BY(&mu_) = "";
  int64_t num_tasks_running_ GUARDED_BY(&mu_) = 0;
};

struct TaskToRetry {
  /// Time when the task should be retried.
  int64_t execution_time_ms;

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
class CoreWorker : public rpc::CoreWorkerServiceHandler {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] options The various initialization options.
  /// \param[in] worker_id ID of this worker.
  CoreWorker(const CoreWorkerOptions &options, const WorkerID &worker_id);

  CoreWorker(CoreWorker const &) = delete;

  /// Core worker's deallocation lifecycle
  ///
  /// Shutdown API must be called before deallocating a core worker.
  /// Otherwise, it can have various destruction order related memory corruption.
  ///
  /// If the core worker is initiated at a driver, the driver is responsible for calling
  /// the shutdown API before terminating. If the core worker is initated at a worker,
  /// shutdown must be called before terminating the task execution loop.
  ~CoreWorker();

  void operator=(CoreWorker const &other) = delete;

  ///
  /// Public methods used by `CoreWorkerProcess` and `CoreWorker` itself.
  ///

  /// Connect to the raylet and notify that the core worker is ready.
  /// If the options.connect_on_start is false, it doesn't need to be explicitly
  /// called.
  void ConnectToRaylet();

  /// Gracefully disconnect the worker from Raylet.
  /// Once the method is returned, it is guaranteed that raylet is
  /// notified that this worker is disconnected from a raylet.
  ///
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  /// \param creation_task_exception_pb_bytes It is given when the worker is
  /// disconnected because the actor is failed due to its exception in its init method.
  /// \return Void.
  void Disconnect(const rpc::WorkerExitType &exit_type,
                  const std::string &exit_detail,
                  const std::shared_ptr<LocalMemoryBuffer>
                      &creation_task_exception_pb_bytes = nullptr);

  /// Shut down the worker completely.
  ///
  /// This must be called before deallocating a worker / driver's core worker for memory
  /// safety.
  ///
  /// \return void.
  void Shutdown();

  /// Start receiving and executing tasks.
  /// \return void.
  void RunTaskExecutionLoop();

  const WorkerID &GetWorkerID() const;

  WorkerType GetWorkerType() const { return options_.worker_type; }

  Language GetLanguage() const { return options_.language; }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  const TaskID &GetCurrentTaskId() const { return worker_context_.GetCurrentTaskID(); }

  JobID GetCurrentJobId() const { return worker_context_.GetCurrentJobID(); }

  const int64_t GetTaskDepth() const { return worker_context_.GetTaskDepth(); }

  NodeID GetCurrentNodeId() const { return NodeID::FromBinary(rpc_address_.raylet_id()); }

  const PlacementGroupID &GetCurrentPlacementGroupId() const {
    return worker_context_.GetCurrentPlacementGroupId();
  }

  bool ShouldCaptureChildTasksInPlacementGroup() const {
    return worker_context_.ShouldCaptureChildTasksInPlacementGroup();
  }

  bool GetCurrentTaskRetryExceptions() const {
    if (!options_.is_local_mode) {
      return worker_context_.GetCurrentTask()->GetMessage().retry_exceptions();
    } else {
      return false;
    }
  }

  void SetWebuiDisplay(const std::string &key, const std::string &message);

  void SetActorTitle(const std::string &title);

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
    // TOOD(ilr): better way of keeping an object from being deleted
    if (!options_.is_local_mode) {
      memory_store_->Delete(deleted);
    }
  }

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const;

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

  /// Get the owner information of an object. This should be
  /// called when serializing an object ID, and the returned information should
  /// be stored with the serialized object ID. If the ownership of the object
  /// cannot be established, then we terminate the process.
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
  void GetOwnershipInfoOrDie(const ObjectID &object_id,
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
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[out] object_id Object ID generated for the put.
  /// \param[out] data Buffer for the user to write the object into.
  /// \param[in] created_by_worker create by worker or not.
  /// \param[in] owner_address The address of object's owner. If not provided,
  /// defaults to this worker.
  /// \param[in] inline_small_object Whether to inline create this object if it's
  /// small.
  /// \return Status.
  Status CreateOwnedAndIncrementLocalRef(
      const std::shared_ptr<Buffer> &metadata,
      const size_t data_size,
      const std::vector<ObjectID> &contained_object_ids,
      ObjectID *object_id,
      std::shared_ptr<Buffer> *data,
      bool created_by_worker,
      const std::unique_ptr<rpc::Address> &owner_address = nullptr,
      bool inline_small_object = true);

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

  /// Get a list of objects from the object store. Objects that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids,
             const int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

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
  /// \param[out] results A bitset that indicates each object has appeared or not.
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

  /// Get the locations of a list objects. Locations that failed to be retrieved
  /// will be returned as nullptrs.
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

  /// Get a string describing object store memory usage for debugging purposes.
  ///
  /// \return std::string The string describing memory usage.
  std::string MemoryUsageString();

  ///
  /// Public methods related to task submission.
  ///

  /// Get the caller ID used to submit tasks from this worker to an actor.
  ///
  /// \return The caller ID. For non-actor tasks, this is the current task ID.
  /// For actors, this is the current actor ID. To make sure that all caller
  /// IDs have the same type, we embed the actor ID in a TaskID with the rest
  /// of the bytes zeroed out.
  TaskID GetCallerId() const LOCKS_EXCLUDED(mutex_);

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

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[in] max_retires max number of retry when the task fails.
  /// \param[in] scheduling_strategy Strategy about how to schedule the task.
  /// \param[in] debugger_breakpoint breakpoint to drop into for the debugger after this
  /// task starts executing, or "" if we do not want to drop into the debugger.
  /// should capture parent's placement group implicilty.
  /// \param[in] serialized_retry_exception_allowlist A serialized exception list
  /// that serves as an allowlist of frontend-language exceptions/errors that should be
  /// retried. Default is an empty string, which will be treated as an allow-all in the
  /// language worker.
  /// \return ObjectRefs returned by this task.
  std::vector<rpc::ObjectReference> SubmitTask(
      const RayFunction &function,
      const std::vector<std::unique_ptr<TaskArg>> &args,
      const TaskOptions &task_options,
      int max_retries,
      bool retry_exceptions,
      const rpc::SchedulingStrategy &scheduling_strategy,
      const std::string &debugger_breakpoint,
      const std::string &serialized_retry_exception_allowlist = "");

  /// Create an actor.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[in] extension_data Extension data of the actor handle,
  /// see `ActorHandle` in `core_worker.proto`.
  /// \param[out] actor_id ID of the created actor. This can be used to submit
  /// tasks on the actor.
  /// \return Status error if actor creation fails, likely due to raylet failure.
  Status CreateActor(const RayFunction &function,
                     const std::vector<std::unique_ptr<TaskArg>> &args,
                     const ActorCreationOptions &actor_creation_options,
                     const std::string &extension_data,
                     ActorID *actor_id);

  /// Create a placement group.
  ///
  /// \param[in] function The remote function that generates the placement group object.
  /// \param[in] placement_group_creation_options Options for this placement group
  /// creation task.
  /// \param[out] placement_group_id ID of the created placement group.
  /// This can be used to shedule actor in node
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
  /// \return ObjectRefs returned by this task.
  std::optional<std::vector<rpc::ObjectReference>> SubmitActorTask(
      const ActorID &actor_id,
      const RayFunction &function,
      const std::vector<std::unique_ptr<TaskArg>> &args,
      const TaskOptions &task_options);

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

  /// Decrease the reference count for this actor. Should be called by the
  /// language frontend when a reference to the ActorHandle destroyed.
  ///
  /// \param[in] actor_id The actor ID to decrease the reference count for.
  void RemoveActorHandleReference(const ActorID &actor_id);

  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized,
                                            const ObjectID &outer_object_id);

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

  const ActorID &GetActorId() const { return actor_id_; }

  // Get the resource IDs available to this worker (as assigned by the raylet).
  const ResourceMappingType GetResourceIDs() const;

  /// Create a profile event and push it the TaskEventBuffer when the event is destructed.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(

      const std::string &event_name);

  int64_t GetNumTasksSubmitted() const {
    return direct_task_submitter_->GetNumTasksSubmitted();
  }

  int64_t GetNumLeasesRequested() const {
    return direct_task_submitter_->GetNumLeasesRequested();
  }

 public:
  /// Allocate the return object for an executing task. The caller should write into the
  /// data buffer of the allocated buffer, then call SealReturnObject() to seal it.
  /// To avoid deadlock, the caller should allocate and seal a single object at a time.
  ///
  /// \param[in] object_id Object ID of the return value.
  /// \param[in] data_size Size of the return value.
  /// \param[in] metadata Metadata buffer of the return value.
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
                          std::shared_ptr<RayObject> return_object,
                          const ObjectID &generator_id);

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
  bool PinExistingReturnObject(const ObjectID &return_id,
                               std::shared_ptr<RayObject> *return_object,
                               const ObjectID &generator_id);

  /// Dynamically allocate an object.
  ///
  /// This should be used during task execution, if the task wants to return an
  /// object to the task caller and have the resulting ObjectRef be owned by
  /// the caller. This is in contrast to static allocation, where the caller
  /// decides at task invocation time how many returns the task should have.
  ///
  /// \param[out] The ObjectID that the caller should use to store the object.
  ObjectID AllocateDynamicReturnId();

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

  /// The following methods are handlers for the core worker's gRPC server, which follow
  /// a macro-generated call convention. These are executed on the io_service_ and
  /// post work to the appropriate event loop.
  ///

  /// Implements gRPC server handler.
  void HandlePushTask(rpc::PushTaskRequest request,
                      rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleDirectActorCallArgWaitComplete(
      rpc::DirectActorCallArgWaitCompleteRequest request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleRayletNotifyGCSRestart(rpc::RayletNotifyGCSRestartRequest request,
                                    rpc::RayletNotifyGCSRestartReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
                             rpc::GetObjectStatusReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleWaitForActorOutOfScope(rpc::WaitForActorOutOfScopeRequest request,
                                    rpc::WaitForActorOutOfScopeReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  // Implements gRPC server handler.
  void HandlePubsubLongPolling(rpc::PubsubLongPollingRequest request,
                               rpc::PubsubLongPollingReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  // Implements gRPC server handler.
  void HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request,
                                rpc::PubsubCommandBatchReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  // Implements gRPC server handler.
  void HandleUpdateObjectLocationBatch(
      rpc::UpdateObjectLocationBatchRequest request,
      rpc::UpdateObjectLocationBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleGetObjectLocationsOwner(rpc::GetObjectLocationsOwnerRequest request,
                                     rpc::GetObjectLocationsOwnerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleKillActor(rpc::KillActorRequest request,
                       rpc::KillActorReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleCancelTask(rpc::CancelTaskRequest request,
                        rpc::CancelTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleRemoteCancelTask(rpc::RemoteCancelTaskRequest request,
                              rpc::RemoteCancelTaskReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandlePlasmaObjectReady(rpc::PlasmaObjectReadyRequest request,
                               rpc::PlasmaObjectReadyReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Get statistics from core worker.
  void HandleGetCoreWorkerStats(rpc::GetCoreWorkerStatsRequest request,
                                rpc::GetCoreWorkerStatsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  /// Trigger local GC on this worker.
  void HandleLocalGC(rpc::LocalGCRequest request,
                     rpc::LocalGCReply *reply,
                     rpc::SendReplyCallback send_reply_callback) override;

  /// Delete objects explicitly.
  void HandleDeleteObjects(rpc::DeleteObjectsRequest request,
                           rpc::DeleteObjectsReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  // Spill objects to external storage.
  void HandleSpillObjects(rpc::SpillObjectsRequest request,
                          rpc::SpillObjectsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  // Restore objects from external storage.
  void HandleRestoreSpilledObjects(rpc::RestoreSpilledObjectsRequest request,
                                   rpc::RestoreSpilledObjectsReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  // Delete objects from external storage.
  void HandleDeleteSpilledObjects(rpc::DeleteSpilledObjectsRequest request,
                                  rpc::DeleteSpilledObjectsReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  // Make the this worker exit.
  // This request fails if the core worker owns any object.
  void HandleExit(rpc::ExitRequest request,
                  rpc::ExitReply *reply,
                  rpc::SendReplyCallback send_reply_callback) override;

  // Set local worker as the owner of object.
  // Request by borrower's worker, execute by owner's worker.
  void HandleAssignObjectOwner(rpc::AssignObjectOwnerRequest request,
                               rpc::AssignObjectOwnerReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  ///
  /// Public methods related to async actor call. This should only be used when
  /// the actor is (1) direct actor and (2) using asyncio mode.
  ///

  /// Block current fiber until event is triggered.
  void YieldCurrentFiber(FiberEvent &event);

  /// The callback expected to be implemented by the client.
  using SetResultCallback =
      std::function<void(std::shared_ptr<RayObject>, ObjectID object_id, void *)>;

  /// Perform async get from the object store.
  ///
  /// \param[in] object_id The id to call get on.
  /// \param[in] success_callback The callback to use the result object.
  /// \param[in] python_future the void* object to be passed to SetResultCallback
  /// \return void
  void GetAsync(const ObjectID &object_id,
                SetResultCallback success_callback,
                void *python_future);

  // Get serialized job configuration.
  rpc::JobConfig GetJobConfig() const;

  /// Return true if the core worker is in the exit process.
  bool IsExiting() const;

  /// Retrieve the current statistics about tasks being received and executing.
  /// \return an unordered_map mapping function name to list of (num_received,
  /// num_executing, num_executed). It is a std map instead of absl due to its
  /// interface with language bindings.
  std::unordered_map<std::string, std::vector<int64_t>> GetActorCallStats() const;

 private:
  static json OverrideRuntimeEnv(json &child, const std::shared_ptr<json> parent);

  /// The following tests will use `OverrideRuntimeEnv` function.
  FRIEND_TEST(TestOverrideRuntimeEnv, TestOverrideEnvVars);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestPyModulesInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestOverridePyModules);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestWorkingDirInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestWorkingDirOverride);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestCondaInherit);
  FRIEND_TEST(TestOverrideRuntimeEnv, TestCondaOverride);

  std::shared_ptr<rpc::RuntimeEnvInfo> OverrideTaskOrActorRuntimeEnvInfo(
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
      const std::string &concurrency_group_name = "",
      bool include_job_config = false);
  void SetCurrentTaskId(const TaskID &task_id,
                        uint64_t attempt_number,
                        const std::string &task_name);

  void SetActorId(const ActorID &actor_id);

  /// Run the io_service_ event loop. This should be called in a background thread.
  void RunIOService();

  /// (WORKER mode only) Gracefully exit the worker. `Graceful` means the worker will
  /// exit when it drains all tasks and cleans all owned objects.
  ///
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  /// \param creation_task_exception_pb_bytes It is given when the worker is
  /// disconnected because the actor is failed due to its exception in its init method.
  void Exit(const rpc::WorkerExitType exit_type,
            const std::string &detail,
            const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes =
                nullptr);

  /// Forcefully exit the worker. `Force` means it will exit actor without draining
  /// or cleaning any resources.
  /// \param exit_type The reason why this worker process is disconnected.
  /// \param exit_detail The detailed reason for a given exit.
  void ForceExit(const rpc::WorkerExitType exit_type, const std::string &detail);

  /// Register this worker or driver to GCS.
  void RegisterToGcs();

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
                            std::shared_ptr<RayObject> obj,
                            rpc::GetObjectStatusReply *reply);

  ///
  /// Private methods related to task submission.
  ///

  /// Increase the local reference count for this object ID. Should be called
  /// by the language frontend when a new reference is created.
  ///
  /// \param[in] object_id The object ID to increase the reference count for.
  /// \param[in] call_site The call site from the language frontend.
  void AddLocalReference(const ObjectID &object_id, std::string call_site) {
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
  ///                 worker. If nullptr, reuse the previously assigned
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
  /// \return Status.
  Status ExecuteTask(
      const TaskSpecification &task_spec,
      const std::shared_ptr<ResourceMappingType> &resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
          *dynamic_return_objects,
      ReferenceCounter::ReferenceTableProto *borrowed_refs,
      bool *is_retryable_error,
      bool *is_application_error);

  /// Put an object in the local plasma store.
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
                            rpc::SendReplyCallback send_reply_callback) {
    if (intended_worker_id != worker_context_.GetWorkerID()) {
      std::ostringstream stream;
      stream << "Mismatched WorkerID: ignoring RPC for previous worker "
             << intended_worker_id
             << ", current worker ID: " << worker_context_.GetWorkerID();
      auto msg = stream.str();
      RAY_LOG(ERROR) << msg;
      send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
      return true;
    } else {
      return false;
    }
  }

  /// Request the spillage of an object that we own from the primary that hosts
  /// the primary copy to spill.
  void SpillOwnedObject(const ObjectID &object_id,
                        const std::shared_ptr<RayObject> &obj,
                        std::function<void()> callback);

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

  /// Shared state of the worker. Includes process-level and thread-level state.
  /// TODO(edoakes): we should move process-level state into this class and make
  /// this a ThreadContext.
  WorkerContext worker_context_;

  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_ GUARDED_BY(mutex_);

  std::string main_thread_task_name_ GUARDED_BY(mutex_);

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  instrumented_io_context io_service_;

  /// Keeps the io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// Shared client call manager.
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  /// Shared core worker client pool.
  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// RPC server used to receive tasks to execute.
  std::unique_ptr<rpc::GrpcServer> core_worker_server_;

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  /// Whether or not this worker is connected to the raylet and GCS.
  bool connected_ = false;

  // Client to the GCS shared by core worker interfaces.
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  // Client to the raylet shared by core worker interfaces. This needs to be a
  // shared_ptr for direct calls because we can lease multiple workers through
  // one client, and we need to keep the connection alive until we return all
  // of the workers.
  std::shared_ptr<raylet::RayletClient> local_raylet_client_;

  // Thread that runs a boost::asio service to process IO events.
  std::thread io_thread_;

  // Keeps track of object ID reference counts.
  std::shared_ptr<ReferenceCounter> reference_counter_;

  ///
  /// Fields related to storing and retrieving objects.
  ///

  /// In-memory store for return objects.
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  /// Plasma store interface.
  std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider_;

  std::unique_ptr<FutureResolver> future_resolver_;

  ///
  /// Fields related to task submission.
  ///

  // Tracks the currently pending tasks.
  std::shared_ptr<TaskManager> task_manager_;

  // A class for actor creation.
  std::shared_ptr<ActorCreatorInterface> actor_creator_;

  // Interface to submit tasks directly to other actors.
  std::shared_ptr<CoreWorkerDirectActorTaskSubmitter> direct_actor_submitter_;

  // A class to publish object status from other raylets/workers.
  std::unique_ptr<pubsub::Publisher> object_info_publisher_;

  // A class to subscribe object status from other raylets/workers.
  std::unique_ptr<pubsub::Subscriber> object_info_subscriber_;

  // Interface to submit non-actor tasks directly to leased workers.
  std::unique_ptr<CoreWorkerDirectTaskSubmitter> direct_task_submitter_;

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
  ActorID actor_id_ GUARDED_BY(mutex_);

  /// The currently executing task spec. We have to track this separately since
  /// we cannot access the thread-local worker contexts from GetCoreWorkerStats()
  absl::flat_hash_map<TaskID, TaskSpecification> current_tasks_ GUARDED_BY(mutex_);

  /// Key value pairs to be displayed on Web UI.
  std::unordered_map<std::string, std::string> webui_display_ GUARDED_BY(mutex_);

  /// Actor title that consists of class name, args, kwargs for actor construction.
  std::string actor_title_ GUARDED_BY(mutex_);

  /// Number of tasks that have been pushed to the actor but not executed.
  std::atomic<int64_t> task_queue_length_;

  /// Number of executed tasks.
  std::atomic<int64_t> num_executed_tasks_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker. This is set on task assignment.
  std::shared_ptr<ResourceMappingType> resource_ids_ GUARDED_BY(mutex_);

  /// Common rpc service for all worker modules.
  rpc::CoreWorkerGrpcService grpc_service_;

  /// Used to notify the task receiver when the arguments of a queued
  /// actor task are ready.
  std::shared_ptr<DependencyWaiterImpl> task_argument_waiter_;

  // Interface that receives tasks from direct actor calls.
  std::unique_ptr<CoreWorkerDirectTaskReceiver> direct_task_receiver_;

  /// Event loop where tasks are processed.
  /// task_execution_service_ should be destructed first to avoid
  /// issues like https://github.com/ray-project/ray/issues/18857
  instrumented_io_context task_execution_service_;

  /// The asio work to keep task_execution_service_ alive.
  boost::asio::io_service::work task_execution_service_work_;

  // Queue of tasks to resubmit when the specified time passes.
  std::priority_queue<TaskToRetry, std::deque<TaskToRetry>, TaskToRetryDescComparator>
      to_resubmit_ GUARDED_BY(mutex_);

  /// Map of named actor registry. It doesn't need to hold a lock because
  /// local mode is single-threaded.
  absl::flat_hash_map<std::string, ActorID> local_mode_named_actor_registry_;

  // Guard for `async_plasma_callbacks_` map.
  mutable absl::Mutex plasma_mutex_;

  // Callbacks for when when a plasma object becomes ready.
  absl::flat_hash_map<ObjectID, std::vector<std::function<void(void)>>>
      async_plasma_callbacks_ GUARDED_BY(plasma_mutex_);

  // Fallback for when GetAsync cannot directly get the requested object.
  void PlasmaCallback(SetResultCallback success,
                      std::shared_ptr<RayObject> ray_object,
                      ObjectID object_id,
                      void *py_future);

  /// we are shutting down and not running further tasks.
  /// when exiting_ is set to true HandlePushTask becomes no-op.
  std::atomic<bool> exiting_ = false;

  std::atomic<bool> is_shutdown_ = false;

  int64_t max_direct_call_object_size_;

  friend class CoreWorkerTest;

  TaskCounter task_counter_;

  /// Used to guarantee that submitting actor task is thread safe.
  /// NOTE(MissiontoMars,scv119): In particular, without this mutex,
  /// the checking and increasing of backpressure pending calls counter
  /// is not atomic, which may lead to under counting or over counting.
  absl::Mutex actor_task_mutex_;

  /// A shared pointer between various components that emitting task state events.
  /// e.g. CoreWorker, TaskManager.
  std::unique_ptr<worker::TaskEventBuffer> task_event_buffer_ = nullptr;
};

}  // namespace core
}  // namespace ray
