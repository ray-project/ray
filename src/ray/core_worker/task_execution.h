#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/worker_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

class CoreWorker;

namespace raylet {
class TaskSpecification;
}

/// The interface that contains all `CoreWorker` methods that are related to task
/// execution.
class CoreWorkerTaskExecutionInterface {
 public:
  // TODO
  using NormalTaskCallback = std::function<Status(
      const RayFunction &ray_function, const JobID &job_id, const TaskID &task_id,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &return_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  // TODO
  using ActorTaskCallback = std::function<Status(
      const RayFunction &ray_function, const JobID &job_id, const TaskID &task_id,
      const ActorID &actor_id, bool create_actor,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &return_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  CoreWorkerTaskExecutionInterface(WorkerContext &worker_context,
                                   std::unique_ptr<RayletClient> &raylet_client,
                                   CoreWorkerObjectInterface &object_interface,
                                   const std::shared_ptr<worker::Profiler> profiler,
                                   const NormalTaskCallback &normal_task_callback,
                                   const ActorTaskCallback &actor_task_callback);

  // Get the resource IDs available to this worker (as assigned by the raylet).
  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

  /// Start receiving and executing tasks.
  /// \return void.
  void Run();

  /// Stop receiving and executing tasks.
  /// \return void.
  void Stop();

 private:
  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passed by value,
  /// just copy their content.
  ///
  /// \param spec[in] Task specification.
  /// \param args[out] The arguments for passing to task executor.
  ///
  Status BuildArgsForExecutor(const TaskSpecification &spec,
                              std::vector<std::shared_ptr<RayObject>> *args);

  /// Execute a task.
  ///
  /// \param spec[in] Task specification.
  /// \param spec[in] Resource IDs of resources assigned to this worker.
  /// \param results[out] Results for task execution.
  /// \return Status.
  Status ExecuteTask(const TaskSpecification &task_spec,
                     const ResourceMappingType &resource_ids,
                     std::vector<std::shared_ptr<RayObject>> *results);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;
  /// Reference to the parent CoreWorker's objects interface.
  CoreWorkerObjectInterface &object_interface_;

  // Reference to the parent CoreWorker's profiler.
  const std::shared_ptr<worker::Profiler> profiler_;

  // Normal task execution callback.
  NormalTaskCallback normal_task_callback_;

  // Actor task execution callback.
  ActorTaskCallback actor_task_callback_;

  /// All the task task receivers supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskReceiver>>
      task_receivers_;

  /// The RPC server.
  rpc::GrpcServer worker_server_;

  /// Event loop where tasks are processed.
  std::shared_ptr<boost::asio::io_service> main_service_;

  /// The asio work to keep main_service_ alive.
  boost::asio::io_service::work main_work_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  // Profile event for when the worker is idle. Should be reset when the worker
  // enters and exits an idle period.
  std::unique_ptr<worker::ProfileEvent> idle_profile_event_;

  friend class CoreWorker;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
