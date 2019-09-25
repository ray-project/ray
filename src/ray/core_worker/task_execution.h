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
  /// The callback provided app-language workers that executes tasks.
  ///
  /// \param ray_function[in] Information about the function to execute.
  /// \param args[in] Arguments of the task.
  /// \param results[out] Results of the task execution.
  /// \return Status.
  using TaskExecutor = std::function<Status(
      const RayFunction &ray_function,
      const std::vector<std::shared_ptr<RayObject>> &args, int num_returns,
      const TaskSpecification &task_spec,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  CoreWorkerTaskExecutionInterface(WorkerContext &worker_context,
                                   std::unique_ptr<RayletClient> &raylet_client,
                                   CoreWorkerObjectInterface &object_interface,
                                   worker::Profiler &profiler,
                                   const TaskExecutor &executor);

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

  worker::Profiler &profiler_;

  // Task execution callback.
  TaskExecutor execution_callback_;

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

  std::unique_ptr<worker::ProfilingEvent> idle_profile_event_;

  friend class CoreWorker;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
