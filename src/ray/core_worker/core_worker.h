#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/task_interface.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
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
  /// \param[in] execution_callback Language worker callback to execute tasks.
  /// \param[in] use_memory_store Whether or not to use the in-memory object store
  ///            in addition to the plasma store.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  /// NOTE(edoakes): the use_memory_store flag is a stop-gap solution to the issue
  ///                that randomly generated ObjectIDs may use the memory store
  ///                instead of the plasma store.
  CoreWorker(const WorkerType worker_type, const Language language,
             const std::string &store_socket, const std::string &raylet_socket,
             const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
             const std::string &log_dir, const std::string &node_ip_address,
             const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
             bool use_memory_store = true);

  ~CoreWorker();

  void Disconnect();

  /// Type of this worker.
  WorkerType GetWorkerType() const { return worker_type_; }

  /// Language of this worker.
  Language GetLanguage() const { return language_; }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  RayletClient &GetRayletClient() { return *raylet_client_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return *task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return *object_interface_; }

  /// Create a profile event with a reference to the core worker's profiler.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(const std::string &event_type);

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() {
    RAY_CHECK(task_execution_interface_ != nullptr);
    return *task_execution_interface_;
  }

  const TaskID &GetCurrentTaskId() const { return worker_context_.GetCurrentTaskID(); }

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentJobId(const JobID &job_id) { worker_context_.SetCurrentJobId(job_id); }

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentTaskId(const TaskID &task_id);

  void SetActorId(const ActorID &actor_id) {
    RAY_CHECK(actor_id_.IsNil());
    actor_id_ = actor_id;
  }

  const ActorID &GetActorId() const { return actor_id_; }

  /// Get the caller ID used to submit tasks from this worker to an actor.
  ///
  /// \return The caller ID. For non-actor tasks, this is the current task ID.
  /// For actors, this is the current actor ID. To make sure that all caller
  /// IDs have the same type, we embed the actor ID in a TaskID with the rest
  /// of the bytes zeroed out.
  TaskID GetCallerId() const;

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
  /// \param actor_id The actor handle to get.
  /// \return A handle to the requested actor.
  ActorHandle &GetActorHandle(const ActorID &actor_id);

 private:
  void StartIOService();

  const WorkerType worker_type_;
  const Language language_;
  const std::string raylet_socket_;
  const std::string log_dir_;
  WorkerContext worker_context_;
  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_;
  /// Our actor ID. If this is nil, then we execute only stateless tasks.
  ActorID actor_id_;

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  boost::asio::io_service io_service_;
  /// Keeps the io_service_ alive.
  boost::asio::io_service::work io_work_;

  std::thread io_thread_;
  std::shared_ptr<worker::Profiler> profiler_;
  std::unique_ptr<RayletClient> raylet_client_;
  std::unique_ptr<gcs::RedisGcsClient> gcs_client_;
  std::unique_ptr<CoreWorkerTaskInterface> task_interface_;
  std::unique_ptr<CoreWorkerObjectInterface> object_interface_;

  /// Map from actor ID to a handle to that actor.
  std::unordered_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_;

  /// Only available if it's not a driver.
  std::unique_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
