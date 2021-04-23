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
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/gcs/gcs_client.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_server.h"
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

class CoreWorker;

// If you change this options's definition, you must change the options used in
// other files. Please take a global search and modify them !!!
struct CoreWorkerOptions {
  // Callback that must be implemented and provided by the language-specific worker
  // frontend to execute tasks and return their results.
  using TaskExecutionCallback = std::function<Status(
      TaskType task_type, const std::string task_name, const RayFunction &ray_function,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &arg_reference_ids,
      const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
      std::vector<std::shared_ptr<RayObject>> *results,
      std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes)>;

  CoreWorkerOptions()
      : store_socket(""),
        raylet_socket(""),
        enable_logging(false),
        log_dir(""),
        install_failure_signal_handler(false),
        interactive(false),
        node_ip_address(""),
        node_manager_port(0),
        raylet_ip_address(""),
        driver_name(""),
        stdout_file(""),
        stderr_file(""),
        task_execution_callback(nullptr),
        check_signals(nullptr),
        gc_collect(nullptr),
        spill_objects(nullptr),
        restore_spilled_objects(nullptr),
        delete_spilled_objects(nullptr),
        unhandled_exception_handler(nullptr),
        get_lang_stack(nullptr),
        kill_main(nullptr),
        ref_counting_enabled(false),
        is_local_mode(false),
        num_workers(0),
        terminate_asyncio_thread(nullptr),
        serialized_job_config(""),
        metrics_agent_port(-1),
        connect_on_start(true) {}

  /// Type of this worker (i.e., DRIVER or WORKER).
  WorkerType worker_type;
  /// Application language of this worker (i.e., PYTHON or JAVA).
  Language language;
  /// Object store socket to connect to.
  std::string store_socket;
  /// Raylet socket to connect to.
  std::string raylet_socket;
  /// Job ID of this worker.
  JobID job_id;
  /// Options for the GCS client.
  gcs::GcsClientOptions gcs_options;
  /// Initialize logging if true. Otherwise, it must be initialized and cleaned up by the
  /// caller.
  bool enable_logging;
  /// Directory to write logs to. If this is empty, logs won't be written to a file.
  std::string log_dir;
  /// If false, will not call `RayLog::InstallFailureSignalHandler()`.
  bool install_failure_signal_handler;
  /// Whether this worker is running in a tty.
  bool interactive;
  /// IP address of the node.
  std::string node_ip_address;
  /// Port of the local raylet.
  int node_manager_port;
  /// IP address of the raylet.
  std::string raylet_ip_address;
  /// The name of the driver.
  std::string driver_name;
  /// The stdout file of this process.
  std::string stdout_file;
  /// The stderr file of this process.
  std::string stderr_file;
  /// Language worker callback to execute tasks.
  TaskExecutionCallback task_execution_callback;
  /// The callback to be called when shutting down a `CoreWorker` instance.
  std::function<void(const WorkerID &)> on_worker_shutdown;
  /// Application-language callback to check for signals that have been received
  /// since calling into C++. This will be called periodically (at least every
  /// 1s) during long-running operations. If the function returns anything but StatusOK,
  /// any long-running operations in the core worker will short circuit and return that
  /// status.
  std::function<Status()> check_signals;
  /// Application-language callback to trigger garbage collection in the language
  /// runtime. This is required to free distributed references that may otherwise
  /// be held up in garbage objects.
  std::function<void()> gc_collect;
  /// Application-language callback to spill objects to external storage.
  std::function<std::vector<std::string>(const std::vector<ObjectID> &,
                                         const std::vector<std::string> &)>
      spill_objects;
  /// Application-language callback to restore objects from external storage.
  std::function<int64_t(const std::vector<ObjectID> &, const std::vector<std::string> &)>
      restore_spilled_objects;
  /// Application-language callback to delete objects from external storage.
  std::function<void(const std::vector<std::string> &, rpc::WorkerType)>
      delete_spilled_objects;
  /// Function to call on error objects never retrieved.
  std::function<void(const RayObject &error)> unhandled_exception_handler;
  std::function<void(const std::string &, const std::vector<std::string> &)>
      run_on_util_worker_handler;
  /// Language worker callback to get the current call stack.
  std::function<void(std::string *)> get_lang_stack;
  // Function that tries to interrupt the currently running Python thread.
  std::function<bool()> kill_main;
  /// Whether to enable object ref counting.
  bool ref_counting_enabled;
  /// Is local mode being used.
  bool is_local_mode;
  /// The number of workers to be started in the current process.
  int num_workers;
  /// The function to destroy asyncio event and loops.
  std::function<void()> terminate_asyncio_thread;
  /// Serialized representation of JobConfig.
  std::string serialized_job_config;
  /// The port number of a metrics agent that imports metrics from core workers.
  /// -1 means there's no such agent.
  int metrics_agent_port;
  /// If false, the constructor won't connect and notify raylets that it is
  /// ready. It should be explicitly startd by a caller using CoreWorker::Start.
  /// TODO(sang): Use this method for Java and cpp frontend too.
  bool connect_on_start;
};

/// Lifecycle management of one or more `CoreWorker` instances in a process.
///
/// To start a driver in the current process:
///     CoreWorkerOptions options = {
///         WorkerType::DRIVER,             // worker_type
///         ...,                            // other arguments
///         1,                              // num_workers
///     };
///     CoreWorkerProcess::Initialize(options);
///
/// To shutdown a driver in the current process:
///     CoreWorkerProcess::Shutdown();
///
/// To start one or more workers in the current process:
///     CoreWorkerOptions options = {
///         WorkerType::WORKER,             // worker_type
///         ...,                            // other arguments
///         num_workers,                    // num_workers
///     };
///     CoreWorkerProcess::Initialize(options);
///     ...                                 // Do other stuff
///     CoreWorkerProcess::RunTaskExecutionLoop();
///
/// To shutdown a worker in the current process, return a system exit status (with status
/// code `IntentionalSystemExit` or `UnexpectedSystemExit`) in the task execution
/// callback.
///
/// If more than 1 worker is started, only the threads which invoke the
/// `task_execution_callback` will be automatically associated with the corresponding
/// worker. If you started your own threads and you want to use core worker APIs in these
/// threads, remember to call `CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id)`
/// once in the new thread before calling core worker APIs, to associate the current
/// thread with a worker. You can obtain the worker ID via
/// `CoreWorkerProcess::GetCoreWorker()->GetWorkerID()`. Currently a Java worker process
/// starts multiple workers by default, but can be configured to start only 1 worker by
/// speicifying `num_java_workers_per_process` in the job config.
///
/// If only 1 worker is started (either because the worker type is driver, or the
/// `num_workers` in `CoreWorkerOptions` is set to 1), all threads will be automatically
/// associated to the only worker. Then no need to call `SetCurrentThreadWorkerId` in
/// your own threads. Currently a Python worker process starts only 1 worker.
class CoreWorkerProcess {
 public:
  ///
  /// Public methods used in both DRIVER and WORKER mode.
  ///

  /// Initialize core workers at the process level.
  ///
  /// \param[in] options The various initialization options.
  static void Initialize(const CoreWorkerOptions &options);

  /// Get the core worker associated with the current thread.
  /// NOTE (kfstorm): Here we return a reference instead of a `shared_ptr` to make sure
  /// `CoreWorkerProcess` has full control of the destruction timing of `CoreWorker`.
  static CoreWorker &GetCoreWorker();

  /// Try to get the `CoreWorker` instance by worker ID.
  /// If the current thread is not associated with a core worker, returns a null pointer.
  ///
  /// \param[in] workerId The worker ID.
  /// \return The `CoreWorker` instance.
  static std::shared_ptr<CoreWorker> TryGetWorker(const WorkerID &worker_id);

  /// Set the core worker associated with the current thread by worker ID.
  /// Currently used by Java worker only.
  ///
  /// \param worker_id The worker ID of the core worker instance.
  static void SetCurrentThreadWorkerId(const WorkerID &worker_id);

  /// Whether the current process has been initialized for core worker.
  static bool IsInitialized();

  ///
  /// Public methods used in DRIVER mode only.
  ///

  /// Shutdown the driver completely at the process level.
  static void Shutdown();

  ///
  /// Public methods used in WORKER mode only.
  ///

  /// Start receiving and executing tasks.
  static void RunTaskExecutionLoop();

  // The destructor is not to be used as a public API, but it's required by smart
  // pointers.
  ~CoreWorkerProcess();

 private:
  /// Create an `CoreWorkerProcess` with proper options.
  ///
  /// \param[in] options The various initialization options.
  CoreWorkerProcess(const CoreWorkerOptions &options);

  /// Check that the core worker environment is initialized for this process.
  ///
  /// \return Void.
  static void EnsureInitialized();

  static void HandleAtExit();

  void InitializeSystemConfig();

  /// Get the `CoreWorker` instance by worker ID.
  ///
  /// \param[in] workerId The worker ID.
  /// \return The `CoreWorker` instance.
  std::shared_ptr<CoreWorker> GetWorker(const WorkerID &worker_id) const
      LOCKS_EXCLUDED(worker_map_mutex_);

  /// Create a new `CoreWorker` instance.
  ///
  /// \return The newly created `CoreWorker` instance.
  std::shared_ptr<CoreWorker> CreateWorker() LOCKS_EXCLUDED(worker_map_mutex_);

  /// Remove an existing `CoreWorker` instance.
  ///
  /// \param[in] The existing `CoreWorker` instance.
  /// \return Void.
  void RemoveWorker(std::shared_ptr<CoreWorker> worker) LOCKS_EXCLUDED(worker_map_mutex_);

  /// The various options.
  const CoreWorkerOptions options_;

  /// The core worker instance associated with the current thread.
  /// Use weak_ptr here to avoid memory leak due to multi-threading.
  static thread_local std::weak_ptr<CoreWorker> current_core_worker_;

  /// The only core worker instance, if the number of workers is 1.
  std::shared_ptr<CoreWorker> global_worker_;

  /// The worker ID of the global worker, if the number of workers is 1.
  const WorkerID global_worker_id_;

  /// Map from worker ID to worker.
  std::unordered_map<WorkerID, std::shared_ptr<CoreWorker>> workers_
      GUARDED_BY(worker_map_mutex_);

  /// To protect accessing the `workers_` map.
  mutable absl::Mutex worker_map_mutex_;
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

  void operator=(CoreWorker const &other) = delete;

  ///
  /// Public methods used by `CoreWorkerProcess` and `CoreWorker` itself.
  ///

  /// Connect to the raylet and notify that the core worker is ready.
  /// If the options.connect_on_start is false, it doesn't need to be explicitly
  /// called.
  void ConnectToRaylet();

  /// Gracefully disconnect the worker from Raylet.
  /// If this function is called during shutdown, Raylet will treat it as an intentional
  /// disconnect.
  ///
  /// \return Void.
  void Disconnect(rpc::WorkerExitType exit_type = rpc::WorkerExitType::INTENDED_EXIT,
                  const std::shared_ptr<LocalMemoryBuffer>
                      &creation_task_exception_pb_bytes = nullptr);

  /// Shut down the worker completely.
  ///
  /// \return void.
  void Shutdown();

  /// Block the current thread until the worker is shut down.
  void WaitForShutdown();

  /// Start receiving and executing tasks.
  /// \return void.
  void RunTaskExecutionLoop();

  const WorkerID &GetWorkerID() const;

  WorkerType GetWorkerType() const { return options_.worker_type; }

  Language GetLanguage() const { return options_.language; }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  const TaskID &GetCurrentTaskId() const { return worker_context_.GetCurrentTaskID(); }

  const JobID &GetCurrentJobId() const { return worker_context_.GetCurrentJobID(); }

  NodeID GetCurrentNodeId() const { return NodeID::FromBinary(rpc_address_.raylet_id()); }

  const PlacementGroupID &GetCurrentPlacementGroupId() const {
    return worker_context_.GetCurrentPlacementGroupId();
  }

  bool ShouldCaptureChildTasksInPlacementGroup() const {
    return worker_context_.ShouldCaptureChildTasksInPlacementGroup();
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
    if (options_.ref_counting_enabled && !options_.is_local_mode) {
      memory_store_->Delete(deleted);
    }
  }

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const;

  /// Put an object into plasma. It's a version of Put that directly put the
  /// object into plasma and also pin the object.
  ///
  /// \param[in] The ray object.
  /// \param[in] object_id The object ID to serialize.
  /// appended to the serialized object ID.
  void PutObjectIntoPlasma(const RayObject &object, const ObjectID &object_id);

  /// Promote an object to plasma. If the
  /// object already exists locally, it will be put into the plasma store. If
  /// it doesn't yet exist, it will be spilled to plasma once available.
  ///
  /// \param[in] object_id The object ID to serialize.
  /// appended to the serialized object ID.
  void PromoteObjectToPlasma(const ObjectID &object_id);

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
  rpc::Address GetOwnerAddress(const ObjectID &object_id) const;

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
  void GetOwnershipInfo(const ObjectID &object_id, rpc::Address *owner_address);

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
  /// \param[out] owner_address The address of the object's owner.
  void RegisterOwnershipInfoAndResolveFuture(const ObjectID &object_id,
                                             const ObjectID &outer_object_id,
                                             const rpc::Address &owner_address);

  ///
  /// Public methods related to storing and retrieving objects.
  ///

  /// Set options for this client's interactions with the object store.
  ///
  /// \param[in] name Unique name for this object store client.
  /// \param[in] limit The maximum amount of memory in bytes that this client
  /// can use in the object store.
  Status SetClientOptions(std::string name, int64_t limit_bytes);

  /// Put an object into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object, const std::vector<ObjectID> &contained_object_ids,
             ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[in] object_id Object ID specified by the user.
  /// \param[in] pin_object Whether or not to tell the raylet to pin this object.
  /// \return Status.
  Status Put(const RayObject &object, const std::vector<ObjectID> &contained_object_ids,
             const ObjectID &object_id, bool pin_object = false);

  /// Create and return a buffer in the object store that can be directly written
  /// into. After writing to the buffer, the caller must call `SealOwned()` to
  /// finalize the object. The `CreateOwned()` and `SealOwned()` combination is
  /// an alternative interface to `Put()` that allows frontends to avoid an extra
  /// copy when possible.
  ///
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] contained_object_ids The IDs serialized in this object.
  /// \param[out] object_id Object ID generated for the put.
  /// \param[out] data Buffer for the user to write the object into.
  /// \return Status.
  Status CreateOwned(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                     const std::vector<ObjectID> &contained_object_ids,
                     ObjectID *object_id, std::shared_ptr<Buffer> *data);

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
  Status CreateExisting(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                        const ObjectID &object_id, const rpc::Address &owner_address,
                        std::shared_ptr<Buffer> *data);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `CreateOwned()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \param[in] pin_object Whether or not to pin the object at the local raylet.
  /// \return Status.
  Status SealOwned(const ObjectID &object_id, bool pin_object);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `CreateExisting()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \param[in] pin_object Whether or not to pin the object at the local raylet.
  /// \param[in] owner_address Address of the owner of the object who will be contacted by
  /// the raylet if the object is pinned. If not provided, defaults to this worker.
  /// \return Status.
  Status SealExisting(const ObjectID &object_id, bool pin_object,
                      const absl::optional<rpc::Address> &owner_address = absl::nullopt);

  /// Get a list of objects from the object store. Objects that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \param[in] plasma_objects_only Only get objects from Plasma Store.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, const int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results,
             bool plasma_objects_only = false);

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
  Status Contains(const ObjectID &object_id, bool *has_object,
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
  Status Wait(const std::vector<ObjectID> &object_ids, const int num_objects,
              const int64_t timeout_ms, std::vector<bool> *results, bool fetch_local);

  /// Delete a list of objects from the plasma object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only);

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
  Status PushError(const JobID &job_id, const std::string &type,
                   const std::string &error_message, double timestamp);

  /// Sets a resource with the specified capacity and client id
  /// \param[in] resource_name Name of the resource to be set.
  /// \param[in] capacity Capacity of the resource.
  /// \param[in] node_id NodeID where the resource is to be set.
  /// \return Status
  Status SetResource(const std::string &resource_name, const double capacity,
                     const NodeID &node_id);

  /// Request an object to be spilled to external storage.
  /// \param[in] object_ids The objects to be spilled.
  /// \return Status. Returns Status::Invalid if any of the objects are not
  /// eligible for spilling (they have gone out of scope or we do not own the
  /// object). Otherwise, the return status is ok and we will use best effort
  /// to spill the object.
  Status SpillObjects(const std::vector<ObjectID> &object_ids);

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \param[in] max_retires max number of retry when the task fails.
  /// \param[in] placement_options placement group options.
  /// \param[in] placement_group_capture_child_tasks whether or not the submitted task
  /// \param[in] debugger_breakpoint breakpoint to drop into for the debugger after this
  /// task starts executing, or "" if we do not want to drop into the debugger.
  /// should capture parent's placement group implicilty.
  void SubmitTask(const RayFunction &function,
                  const std::vector<std::unique_ptr<TaskArg>> &args,
                  const TaskOptions &task_options, std::vector<ObjectID> *return_ids,
                  int max_retries, BundleID placement_options,
                  bool placement_group_capture_child_tasks,
                  const std::string &debugger_breakpoint);

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
                     const std::string &extension_data, ActorID *actor_id);

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
                                 int timeout_seconds);

  /// Submit an actor task.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status error if the task is invalid or if the task submission
  /// failed. Tasks can be invalid for direct actor calls because not all tasks
  /// are currently supported.
  void SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                       const std::vector<std::unique_ptr<TaskArg>> &args,
                       const TaskOptions &task_options,
                       std::vector<ObjectID> *return_ids);

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
  Status SerializeActorHandle(const ActorID &actor_id, std::string *output,
                              ObjectID *actor_handle_id) const;

  ///
  /// Public methods related to task execution. Should not be used by driver processes.
  ///

  const ActorID &GetActorId() const { return actor_id_; }

  // Get the resource IDs available to this worker (as assigned by the raylet).
  const ResourceMappingType GetResourceIDs() const;

  /// Create a profile event with a reference to the core worker's profiler.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(const std::string &event_type);

 public:
  /// Allocate the return objects for an executing task. The caller should write into the
  /// data buffers of the allocated buffers.
  ///
  /// \param[in] object_ids Object IDs of the return values.
  /// \param[in] data_sizes Sizes of the return values.
  /// \param[in] metadatas Metadata buffers of the return values.
  /// \param[in] contained_object_ids IDs serialized within each return object.
  /// \param[out] return_objects RayObjects containing buffers to write results into.
  /// \return Status.
  Status AllocateReturnObjects(
      const std::vector<ObjectID> &object_ids, const std::vector<size_t> &data_sizes,
      const std::vector<std::shared_ptr<Buffer>> &metadatas,
      const std::vector<std::vector<ObjectID>> &contained_object_ids,
      std::vector<std::shared_ptr<RayObject>> *return_objects);

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
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return The shared_ptr to the actor handle if found, nullptr otherwise.
  /// The second pair contains the status of getting a named actor handle.
  std::pair<std::shared_ptr<const ActorHandle>, Status> GetNamedActorHandle(
      const std::string &name);

  ///
  /// The following methods are handlers for the core worker's gRPC server, which follow
  /// a macro-generated call convention. These are executed on the io_service_ and
  /// post work to the appropriate event loop.
  ///

  /// Implements gRPC server handler.
  void HandlePushTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleDirectActorCallArgWaitComplete(
      const rpc::DirectActorCallArgWaitCompleteRequest &request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleGetObjectStatus(const rpc::GetObjectStatusRequest &request,
                             rpc::GetObjectStatusReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleWaitForActorOutOfScope(const rpc::WaitForActorOutOfScopeRequest &request,
                                    rpc::WaitForActorOutOfScopeReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleSubscribeForObjectEviction(
      const rpc::SubscribeForObjectEvictionRequest &request,
      rpc::SubscribeForObjectEvictionReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  // Implements gRPC server handler.
  void HandlePubsubLongPolling(const rpc::PubsubLongPollingRequest &request,
                               rpc::PubsubLongPollingReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleWaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
                               rpc::WaitForRefRemovedReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleAddObjectLocationOwner(const rpc::AddObjectLocationOwnerRequest &request,
                                    rpc::AddObjectLocationOwnerReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleRemoveObjectLocationOwner(
      const rpc::RemoveObjectLocationOwnerRequest &request,
      rpc::RemoveObjectLocationOwnerReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleGetObjectLocationsOwner(const rpc::GetObjectLocationsOwnerRequest &request,
                                     rpc::GetObjectLocationsOwnerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleKillActor(const rpc::KillActorRequest &request, rpc::KillActorReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleCancelTask(const rpc::CancelTaskRequest &request,
                        rpc::CancelTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandleRemoteCancelTask(const rpc::RemoteCancelTaskRequest &request,
                              rpc::RemoteCancelTaskReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Implements gRPC server handler.
  void HandlePlasmaObjectReady(const rpc::PlasmaObjectReadyRequest &request,
                               rpc::PlasmaObjectReadyReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Get statistics from core worker.
  void HandleGetCoreWorkerStats(const rpc::GetCoreWorkerStatsRequest &request,
                                rpc::GetCoreWorkerStatsReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  /// Trigger local GC on this worker.
  void HandleLocalGC(const rpc::LocalGCRequest &request, rpc::LocalGCReply *reply,
                     rpc::SendReplyCallback send_reply_callback) override;

  // Run request on an python-based IO worker
  void HandleRunOnUtilWorker(const rpc::RunOnUtilWorkerRequest &request,
                             rpc::RunOnUtilWorkerReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  // Spill objects to external storage.
  void HandleSpillObjects(const rpc::SpillObjectsRequest &request,
                          rpc::SpillObjectsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  // Add spilled URL to owned reference.
  void HandleAddSpilledUrl(const rpc::AddSpilledUrlRequest &request,
                           rpc::AddSpilledUrlReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  // Restore objects from external storage.
  void HandleRestoreSpilledObjects(const rpc::RestoreSpilledObjectsRequest &request,
                                   rpc::RestoreSpilledObjectsReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  // Delete objects from external storage.
  void HandleDeleteSpilledObjects(const rpc::DeleteSpilledObjectsRequest &request,
                                  rpc::DeleteSpilledObjectsReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  // Make the this worker exit.
  // This request fails if the core worker owns any object.
  void HandleExit(const rpc::ExitRequest &request, rpc::ExitReply *reply,
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
  void GetAsync(const ObjectID &object_id, SetResultCallback success_callback,
                void *python_future);

  // Get serialized job configuration.
  const rpc::JobConfig &GetJobConfig() const;

  // Get gcs_client
  std::shared_ptr<gcs::GcsClient> GetGcsClient() const;

  /// Return true if the core worker is in the exit process.
  bool IsExiting() const;

 private:
  void SetCurrentTaskId(const TaskID &task_id);

  void SetActorId(const ActorID &actor_id);

  /// Run the io_service_ event loop. This should be called in a background thread.
  void RunIOService();

  /// (WORKER mode only) Exit the worker. This is the entrypoint used to shutdown a
  /// worker.
  void Exit(rpc::WorkerExitType exit_type,
            const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes =
                nullptr);

  /// Register this worker or driver to GCS.
  void RegisterToGcs();

  /// Check if the raylet has failed. If so, shutdown.
  void CheckForRayletFailure();

  /// Heartbeat for internal bookkeeping.
  void InternalHeartbeat();

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
  ///                     by value (not via plasma).
  /// \param results[out] borrowed_refs Refs that this task (or a nested task)
  ///                     was or is still borrowing. This includes all
  ///                     objects whose IDs we passed to the task in its
  ///                     arguments and recursively, any object IDs that were
  ///                     contained in those objects.
  /// \return Status.
  Status ExecuteTask(const TaskSpecification &task_spec,
                     const std::shared_ptr<ResourceMappingType> &resource_ids,
                     std::vector<std::shared_ptr<RayObject>> *return_objects,
                     ReferenceCounter::ReferenceTableProto *borrowed_refs);

  /// Execute a local mode task (runs normal ExecuteTask)
  ///
  /// \param spec[in] task_spec Task specification.
  void ExecuteTaskLocalMode(const TaskSpecification &task_spec,
                            const ActorID &actor_id = ActorID::Nil());

  /// KillActor API for a local mode.
  Status KillActorLocalMode(const ActorID &actor_id);

  /// Get a handle to a named actor for local mode.
  std::pair<std::shared_ptr<const ActorHandle>, Status> GetNamedActorHandleLocalMode(
      const std::string &name);

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
                                  std::vector<ObjectID> *arg_reference_ids,
                                  std::vector<ObjectID> *pinned_ids);

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

  /// Handler if a raylet node is removed from the cluster.
  void OnNodeRemoved(const NodeID &node_id);

  /// Request the spillage of an object that we own from the primary that hosts
  /// the primary copy to spill.
  void SpillOwnedObject(const ObjectID &object_id, const std::shared_ptr<RayObject> &obj,
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

  /// Shared state of the worker. Includes process-level and thread-level state.
  /// TODO(edoakes): we should move process-level state into this class and make
  /// this a ThreadContext.
  WorkerContext worker_context_;

  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_ GUARDED_BY(mutex_);

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

  // Interface to submit tasks directly to other actors.
  std::shared_ptr<CoreWorkerDirectActorTaskSubmitter> direct_actor_submitter_;

  // A class to publish object status from other raylets/workers.
  std::shared_ptr<pubsub::Publisher> object_status_publisher_;

  // A class to subscribe object status from other raylets/workers.
  std::shared_ptr<pubsub::Subscriber> object_status_subscriber_;

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
  TaskSpecification current_task_ GUARDED_BY(mutex_);

  /// Key value pairs to be displayed on Web UI.
  std::unordered_map<std::string, std::string> webui_display_ GUARDED_BY(mutex_);

  /// Actor title that consists of class name, args, kwargs for actor construction.
  std::string actor_title_ GUARDED_BY(mutex_);

  /// Number of tasks that have been pushed to the actor but not executed.
  std::atomic<int64_t> task_queue_length_;

  /// Number of executed tasks.
  std::atomic<int64_t> num_executed_tasks_;

  /// Event loop where tasks are processed.
  instrumented_io_context task_execution_service_;

  /// The asio work to keep task_execution_service_ alive.
  boost::asio::io_service::work task_execution_service_work_;

  /// Profiler including a background thread that pushes profiling events to the GCS.
  std::shared_ptr<worker::Profiler> profiler_;

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

  // Queue of tasks to resubmit when the specified time passes.
  std::deque<std::pair<int64_t, TaskSpecification>> to_resubmit_ GUARDED_BY(mutex_);

  /// Map of named actor registry. It doesn't need to hold a lock because
  /// local mode is single-threaded.
  absl::flat_hash_map<std::string, ActorID> local_mode_named_actor_registry_;

  // Guard for `async_plasma_callbacks_` map.
  mutable absl::Mutex plasma_mutex_;

  // Callbacks for when when a plasma object becomes ready.
  absl::flat_hash_map<ObjectID, std::vector<std::function<void(void)>>>
      async_plasma_callbacks_ GUARDED_BY(plasma_mutex_);

  // Fallback for when GetAsync cannot directly get the requested object.
  void PlasmaCallback(SetResultCallback success, std::shared_ptr<RayObject> ray_object,
                      ObjectID object_id, void *py_future);

  /// Whether we are shutting down and not running further tasks.
  bool exiting_ = false;

  int64_t max_direct_call_object_size_;

  friend class CoreWorkerTest;

  std::unique_ptr<rpc::JobConfig> job_config_;
};

}  // namespace ray
