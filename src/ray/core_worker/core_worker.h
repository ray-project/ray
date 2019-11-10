#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/buffer.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_client.h"
#include "ray/rpc/worker/worker_server.h"

/// The set of gRPC handlers and their associated level of concurrency. If you want to
/// add a new call to the worker gRPC server, do the following:
/// 1) Add the rpc to the WorkerService in core_worker.proto, e.g., "ExampleCall"
/// 2) Add a new handler to the macro below: "RAY_CORE_WORKER_RPC_HANDLER(ExampleCall, 1)"
/// 3) Add a method to the CoreWorker class below: "CoreWorker::HandleExampleCall"
#define RAY_CORE_WORKER_RPC_HANDLERS                       \
  RAY_CORE_WORKER_RPC_HANDLER(AssignTask, 5)               \
  RAY_CORE_WORKER_RPC_HANDLER(DirectActorAssignTask, 9999) \
  RAY_CORE_WORKER_RPC_HANDLER(DirectActorCallArgWaitComplete, 100)

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
  // Callback that must be implemented and provided by the language-specific worker
  // frontend to execute tasks and return their results.
  using TaskExecutionCallback = std::function<Status(
      TaskType task_type, const RayFunction &ray_function,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &arg_reference_ids,
      const std::vector<ObjectID> &return_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] language Language of this worker.
  /// \param[in] store_socket Object store socket to connect to.
  /// \param[in] raylet_socket Raylet socket to connect to.
  /// \param[in] job_id Job ID of this worker.
  /// \param[in] gcs_options Options for the GCS client.
  /// \param[in] log_dir Directory to write logs to. If this is empty, logs
  ///            won't be written to a file.
  /// \param[in] node_ip_address IP address of the node.
  /// \param[in] task_execution_callback Language worker callback to execute tasks.
  /// \parma[in] check_signals Language worker function to check for signals and handle
  ///            them. If the function returns anything but StatusOK, any long-running
  ///            operations in the core worker will short circuit and return that status.
  /// \parma[in] exit_handler Language worker function to orderly shutdown the worker.
  ///            We guarantee this will be run on the main thread of the worker.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  CoreWorker(const WorkerType worker_type, const Language language,
             const std::string &store_socket, const std::string &raylet_socket,
             const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
             const std::string &log_dir, const std::string &node_ip_address,
             const TaskExecutionCallback &task_execution_callback,
             std::function<Status()> check_signals = nullptr,
             std::function<void()> exit_handler = nullptr);

  ~CoreWorker();

  void Disconnect();

  WorkerType GetWorkerType() const { return worker_type_; }

  Language GetLanguage() const { return language_; }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  RayletClient &GetRayletClient() { return *raylet_client_; }

  const TaskID &GetCurrentTaskId() const { return worker_context_.GetCurrentTaskID(); }

  void SetCurrentTaskId(const TaskID &task_id);

  const JobID &GetCurrentJobId() const { return worker_context_.GetCurrentJobID(); }

  void SetActorId(const ActorID &actor_id) {
    RAY_CHECK(actor_id_.IsNil());
    actor_id_ = actor_id;
  }

  /// Increase the reference count for this object ID.
  ///
  /// \param[in] object_id The object ID to increase the reference count for.
  void AddObjectIDReference(const ObjectID &object_id) {
    reference_counter_.AddReference(object_id);
  }

  /// Decrease the reference count for this object ID.
  ///
  /// \param[in] object_id The object ID to decrease the reference count for.
  void RemoveObjectIDReference(const ObjectID &object_id) {
    reference_counter_.RemoveReference(object_id);
  }

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
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object, ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by the user.
  /// \return Status.
  Status Put(const RayObject &object, const ObjectID &object_id);

  /// Create and return a buffer in the object store that can be directly written
  /// into. After writing to the buffer, the caller must call `Seal()` to finalize
  /// the object. The `Create()` and `Seal()` combination is an alternative interface
  /// to `Put()` that allows frontends to avoid an extra copy when possible.
  ///
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[out] object_id Object ID generated for the put.
  /// \param[out] data Buffer for the user to write the object into.
  /// \return Status.
  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                ObjectID *object_id, std::shared_ptr<Buffer> *data);

  /// Create and return a buffer in the object store that can be directly written
  /// into. After writing to the buffer, the caller must call `Seal()` to finalize
  /// the object. The `Create()` and `Seal()` combination is an alternative interface
  /// to `Put()` that allows frontends to avoid an extra copy when possible.
  ///
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] object_id Object ID specified by the user.
  /// \param[out] data Buffer for the user to write the object into.
  /// \return Status.
  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `Create()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \return Status.
  Status Seal(const ObjectID &object_id);

  /// Get a list of objects from the object store. Objects that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, const int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Return whether or not the object store contains the given object.
  ///
  /// \param[in] object_id ID of the objects to check for.
  /// \param[out] has_object Whether or not the object is present.
  /// \return Status.
  Status Contains(const ObjectID &object_id, bool *has_object);

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
              const int64_t timeout_ms, std::vector<bool> *results);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

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
  TaskID GetCallerId() const;

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status error if task submission fails, likely due to raylet failure.
  Status SubmitTask(const RayFunction &function, const std::vector<TaskArg> &args,
                    const TaskOptions &task_options, std::vector<ObjectID> *return_ids);

  /// Create an actor.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[out] actor_handle Handle to the actor.
  /// \param[out] actor_id ID of the created actor. This can be used to submit
  /// tasks on the actor.
  /// \return Status error if actor creation fails, likely due to raylet failure.
  Status CreateActor(const RayFunction &function, const std::vector<TaskArg> &args,
                     const ActorCreationOptions &actor_creation_options,
                     ActorID *actor_id);

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
  Status SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                         const std::vector<TaskArg> &args,
                         const TaskOptions &task_options,
                         std::vector<ObjectID> *return_ids);

  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized);

  /// Serialize an actor handle.
  ///
  /// This should be called when passing an actor handle to another task or
  /// actor.
  ///
  /// \param[in] actor_id The ID of the actor handle to serialize.
  /// \param[out] The serialized handle.
  /// \return Status::Invalid if we don't have the specified handle.
  Status SerializeActorHandle(const ActorID &actor_id, std::string *output) const;

  ///
  /// Public methods related to task execution. Should not be used by driver processes.
  ///

  const ActorID &GetActorId() const { return actor_id_; }

  // Get the resource IDs available to this worker (as assigned by the raylet).
  const ResourceMappingType GetResourceIDs() const { return resource_ids_; }

  /// Create a profile event with a reference to the core worker's profiler.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(const std::string &event_type);

  /// Start receiving and executing tasks.
  /// \return void.
  void StartExecutingTasks();

  /// Allocate the return objects for an executing task. The caller should write into the
  /// data buffers of the allocated buffers.
  ///
  /// \param[in] object_ids Object IDs of the return values.
  /// \param[in] data_sizes Sizes of the return values.
  /// \param[in] metadatas Metadata buffers of the return values.
  /// \param[out] return_objects RayObjects containing buffers to write results into.
  /// \return Status.
  Status AllocateReturnObjects(const std::vector<ObjectID> &object_ids,
                               const std::vector<size_t> &data_sizes,
                               const std::vector<std::shared_ptr<Buffer>> &metadatas,
                               std::vector<std::shared_ptr<RayObject>> *return_objects);

  /* Handlers for the worker's gRPC server. These are executed on the io_service_ and post
   * work to the appropriate event loop.
   */

  /// Handle an "AssignTask" event corresponding to scheduling a normal or an actor task
  /// on this worker from the raylet.
  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        rpc::AssignTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

  /// Handle a "DirectActorAssignTask" event corresponding to scheduling an actor task
  /// on this worker from another worker.
  void HandleDirectActorAssignTask(const rpc::DirectActorAssignTaskRequest &request,
                                   rpc::DirectActorAssignTaskReply *reply,
                                   rpc::SendReplyCallback send_reply_callback);

  /// Handle a "DirectActorAssignTask" event corresponding to the raylet notifiying this
  /// worker that an argument is ready.
  void HandleDirectActorCallArgWaitComplete(
      const rpc::DirectActorCallArgWaitCompleteRequest &request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback);

 private:
  /// Run the io_service_ event loop. This should be called in a background thread.
  void RunIOService();

  /// Shut down the worker completely.
  /// \return void.
  void Shutdown();

  /// Send the list of active object IDs to the raylet.
  void ReportActiveObjectIDs();

  ///
  /// Private methods related to task submission.
  ///

  /// Submit the task to the raylet and add its dependencies to the reference counter.
  Status SubmitTaskToRaylet(const TaskSpecification &task_spec);

  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle);

  /// Get a handle to an actor. This asserts that the worker actually has this
  /// handle.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  ///
  /// Private methods related to task execution. Should not be used by driver processes.
  ///

  /// Execute a task.
  ///
  /// \param spec[in] Task specification.
  /// \param spec[in] Resource IDs of resources assigned to this worker.
  /// \param results[out] Result objects that should be returned by value (not via
  ///                     plasma).
  /// \return Status.
  Status ExecuteTask(const TaskSpecification &task_spec,
                     const ResourceMappingType &resource_ids,
                     std::vector<std::shared_ptr<RayObject>> *return_objects);

  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passed by value,
  /// just copy their content.
  ///
  /// \param spec[in] Task specification.
  /// \param args[out] Argument data as RayObjects.
  /// \param args[out] ObjectIDs corresponding to each by reference argument. The length
  ///                  of this vector will be the same as args, and by value arguments
  ///                  will have ObjectID::Nil().
  ///                  // TODO(edoakes): this is a bit of a hack that's necessary because
  ///                  we have separate serialization paths for by-value and by-reference
  ///                  arguments in Python. This should ideally be handled better there.
  /// \return The arguments for passing to task executor.
  Status BuildArgsForExecutor(const TaskSpecification &task,
                              std::vector<std::shared_ptr<RayObject>> *args,
                              std::vector<ObjectID> *arg_reference_ids);

  /// Type of this worker (i.e., DRIVER or WORKER).
  const WorkerType worker_type_;

  /// Application language of this worker (i.e., PYTHON or JAVA).
  const Language language_;

  /// Directory where log files are written.
  const std::string log_dir_;

  /// Application-language callback to check for signals that have been received
  /// since calling into C++. This will be called periodically (at least every
  /// 1s) during long-running operations.
  std::function<Status()> check_signals_;

  /// Shared state of the worker. Includes process-level and thread-level state.
  /// TODO(edoakes): we should move process-level state into this class and make
  /// this a ThreadContext.
  WorkerContext worker_context_;

  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_;

  // Flag indicating whether this worker has been shut down.
  bool shutdown_ = false;

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  boost::asio::io_service io_service_;

  /// Keeps the io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// Timer used to periodically send heartbeat containing active object IDs to the
  /// raylet.
  boost::asio::steady_timer heartbeat_timer_;

  /// RPC server used to receive tasks to execute.
  rpc::GrpcServer worker_server_;

  // Client to the GCS shared by core worker interfaces.
  gcs::RedisGcsClient gcs_client_;

  // Client to the raylet shared by core worker interfaces.
  std::unique_ptr<RayletClient> raylet_client_;

  // Thread that runs a boost::asio service to process IO events.
  std::thread io_thread_;

  // Keeps track of object ID reference counts.
  ReferenceCounter reference_counter_;

  ///
  /// Fields related to storing and retrieving objects.
  ///

  /// In-memory store for return objects. This is used for `MEMORY` store provider.
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  /// Plasma store interface.
  std::unique_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider_;

  /// In-memory store interface.
  std::unique_ptr<CoreWorkerMemoryStoreProvider> memory_store_provider_;

  ///
  /// Fields related to task submission.
  ///

  // Interface to submit tasks directly to other actors.
  std::unique_ptr<CoreWorkerDirectActorTaskSubmitter> direct_actor_submitter_;

  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_;

  ///
  /// Fields related to task execution.
  ///

  /// Our actor ID. If this is nil, then we execute only stateless tasks.
  ActorID actor_id_;

  /// Event loop where tasks are processed.
  boost::asio::io_service task_execution_service_;

  /// The asio work to keep task_execution_service_ alive.
  boost::asio::io_service::work task_execution_service_work_;

  /// Profiler including a background thread that pushes profiling events to the GCS.
  std::shared_ptr<worker::Profiler> profiler_;

  /// Task execution callback.
  TaskExecutionCallback task_execution_callback_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  // Interface that receives tasks from the raylet.
  std::unique_ptr<CoreWorkerRayletTaskReceiver> raylet_task_receiver_;

  /// Common rpc service for all worker modules.
  rpc::WorkerGrpcService grpc_service_;

  // Interface that receives tasks from direct actor calls.
  std::unique_ptr<CoreWorkerDirectActorTaskReceiver> direct_actor_task_receiver_;

  friend class CoreWorkerTest;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
