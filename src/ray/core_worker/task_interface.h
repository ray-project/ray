#ifndef RAY_CORE_WORKER_TASK_INTERFACE_H
#define RAY_CORE_WORKER_TASK_INTERFACE_H

#include "ray/common/buffer.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/protobuf/core_worker.pb.h"

namespace ray {

/// Options of a non-actor-creation task.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(int num_returns, std::unordered_map<std::string, double> &resources)
      : num_returns(num_returns), resources(resources) {}

  /// Number of returns of this task.
  int num_returns = 1;
  /// Resources required by this task.
  std::unordered_map<std::string, double> resources;
};

/// Options of an actor creation task.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(uint64_t max_reconstructions, bool is_direct_call,
                       const std::unordered_map<std::string, double> &resources,
                       const std::unordered_map<std::string, double> &placement_resources,
                       const std::vector<std::string> &dynamic_worker_options)
      : max_reconstructions(max_reconstructions),
        is_direct_call(is_direct_call),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options) {}

  /// Maximum number of times that the actor should be reconstructed when it dies
  /// unexpectedly. It must be non-negative. If it's 0, the actor won't be reconstructed.
  const uint64_t max_reconstructions = 0;
  /// Whether to use direct actor call. If this is set to true, callers will submit
  /// tasks directly to the created actor without going through raylet.
  const bool is_direct_call = false;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
  /// Resources required to place this actor.
  const std::unordered_map<std::string, double> placement_resources;
  /// The dynamic options used in the worker command when starting a worker process for
  /// an actor creation task.
  const std::vector<std::string> dynamic_worker_options;
};

/// The interface that contains all `CoreWorker` methods that are related to task
/// submission.
class CoreWorkerTaskInterface {
 public:
  CoreWorkerTaskInterface(WorkerContext &worker_context,
                          std::unique_ptr<RayletClient> &raylet_client,
                          CoreWorkerObjectInterface &object_interface,
                          boost::asio::io_service &io_service);

  /// Submit a normal task.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status SubmitTask(const TaskID &caller_id, const RayFunction &function,
                    const std::vector<TaskArg> &args, const TaskOptions &task_options,
                    std::vector<ObjectID> *return_ids);

  /// Create an actor.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[out] actor_handle Handle to the actor.
  /// \return Status.
  Status CreateActor(const TaskID &caller_id, const RayFunction &function,
                     const std::vector<TaskArg> &args,
                     const ActorCreationOptions &actor_creation_options,
                     std::unique_ptr<ActorHandle> *actor_handle);

  /// Submit an actor task.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status.
  Status SubmitActorTask(const TaskID &caller_id, ActorHandle &actor_handle,
                         const RayFunction &function, const std::vector<TaskArg> &args,
                         const TaskOptions &task_options,
                         std::vector<ObjectID> *return_ids);

  /// Handle an update about an actor.
  ///
  /// \param[in] actor_id The ID of the actor whose status has changed.
  /// \param[in] actor_data The actor's new status information.
  void HandleDirectActorUpdate(const ActorID &actor_id,
                               const gcs::ActorTableData &actor_data);

 private:
  /// Build common attributes of the task spec, and compute return ids.
  ///
  /// \param[in] builder Builder to build a `TaskSpec`.
  /// \param[in] job_id The ID of the job submitting the task.
  /// \param[in] task_id The ID of this task.
  /// \param[in] task_index The task index used to build this task.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] num_returns Number of returns.
  /// \param[in] required_resources Resources required by this task.
  /// \param[in] required_placement_resources Resources required by placing this task on a
  /// node.
  /// \param[in] transport_type The transport used for this task.
  /// \param[out] return_ids Return IDs.
  /// \return Void.
  void BuildCommonTaskSpec(
      TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
      const int task_index, const TaskID &caller_id, const RayFunction &function,
      const std::vector<TaskArg> &args, uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      TaskTransportType transport_type, std::vector<ObjectID> *return_ids);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;

  /// All the task submitters supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskSubmitter>>
      task_submitters_;

  friend class CoreWorkerTest;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_INTERFACE_H
