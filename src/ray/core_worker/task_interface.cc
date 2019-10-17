#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskInterface::CoreWorkerTaskInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerObjectInterface &object_interface, boost::asio::io_service &io_service)
    : worker_context_(worker_context) {
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter(raylet_client)));
  task_submitters_.emplace(TaskTransportType::DIRECT_ACTOR,
                           std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                               new CoreWorkerDirectActorTaskSubmitter(
                                   io_service, object_interface.CreateStoreProvider(
                                                   StoreProviderType::MEMORY))));
}

void CoreWorkerTaskInterface::BuildCommonTaskSpec(
    TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
    const int task_index, const TaskID &caller_id, const RayFunction &function,
    const std::vector<TaskArg> &args, uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    TaskTransportType transport_type, std::vector<ObjectID> *return_ids) {
  // Build common task spec.
  builder.SetCommonTaskSpec(
      task_id, function.GetLanguage(), function.GetFunctionDescriptor(), job_id,
      worker_context_.GetCurrentTaskID(), task_index, caller_id, num_returns,
      required_resources, required_placement_resources);
  // Set task arguments.
  for (const auto &arg : args) {
    if (arg.IsPassedByReference()) {
      builder.AddByRefArg(arg.GetReference());
    } else {
      builder.AddByValueArg(arg.GetValue());
    }
  }

  // Compute return IDs.
  (*return_ids).resize(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    (*return_ids)[i] =
        ObjectID::ForTaskReturn(task_id, i + 1,
                                /*transport_type=*/static_cast<int>(transport_type));
  }
}

Status CoreWorkerTaskInterface::SubmitTask(const TaskID &caller_id,
                                           const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &task_options,
                                           std::vector<ObjectID> *return_ids) {
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const auto task_id =
      TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                            worker_context_.GetCurrentTaskID(), next_task_index);
  BuildCommonTaskSpec(builder, worker_context_.GetCurrentJobID(), task_id,
                      next_task_index, caller_id, function, args,
                      task_options.num_returns, task_options.resources, {},
                      TaskTransportType::RAYLET, return_ids);
  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::CreateActor(
    const TaskID &caller_id, const RayFunction &function,
    const std::vector<TaskArg> &args, const ActorCreationOptions &actor_creation_options,
    std::unique_ptr<ActorHandle> *actor_handle) {
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
                  next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  BuildCommonTaskSpec(builder, job_id, actor_creation_task_id, next_task_index, caller_id,
                      function, args, 1, actor_creation_options.resources,
                      actor_creation_options.placement_resources,
                      TaskTransportType::RAYLET, &return_ids);
  builder.SetActorCreationTaskSpec(actor_id, actor_creation_options.max_reconstructions,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.is_direct_call);

  *actor_handle = std::unique_ptr<ActorHandle>(new ActorHandle(
      actor_id, job_id, /*actor_cursor=*/return_ids[0], function.GetLanguage(),
      actor_creation_options.is_direct_call, function.GetFunctionDescriptor()));

  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::SubmitActorTask(const TaskID &caller_id,
                                                ActorHandle &actor_handle,
                                                const RayFunction &function,
                                                const std::vector<TaskArg> &args,
                                                const TaskOptions &task_options,
                                                std::vector<ObjectID> *return_ids) {
  // Add one for actor cursor object id for tasks.
  const int num_returns = task_options.num_returns + 1;

  const bool is_direct_call = actor_handle.IsDirectCallActor();
  const TaskTransportType transport_type =
      is_direct_call ? TaskTransportType::DIRECT_ACTOR : TaskTransportType::RAYLET;

  // Build common task spec.
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const TaskID actor_task_id = TaskID::ForActorTask(
      worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
      next_task_index, actor_handle.GetActorID());
  BuildCommonTaskSpec(builder, actor_handle.CreationJobID(), actor_task_id,
                      next_task_index, caller_id, function, args, num_returns,
                      task_options.resources, {}, transport_type, return_ids);

  const ObjectID new_cursor = return_ids->back();
  actor_handle.SetActorTaskSpec(builder, transport_type, new_cursor);

  // Submit task.
  auto status = task_submitters_[transport_type]->SubmitTask(builder.Build());
  // Remove cursor from return ids.
  return_ids->pop_back();

  return status;
}

void CoreWorkerTaskInterface::HandleDirectActorUpdate(
    const ActorID &actor_id, const gcs::ActorTableData &actor_data) {
  auto &submitter = task_submitters_[TaskTransportType::DIRECT_ACTOR];
  auto &direct_actor_submitter =
      reinterpret_cast<std::unique_ptr<CoreWorkerDirectActorTaskSubmitter> &>(submitter);
  direct_actor_submitter->HandleActorUpdate(actor_id, actor_data);
}

}  // namespace ray
