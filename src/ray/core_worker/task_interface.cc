#include "task_interface.h"
#include "context.h"
#include "core_worker.h"
#include "ray/raylet/task_spec.h"

namespace ray {

Status CoreWorkerTaskInterface::SubmitTask(const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &call_options,
                                           std::vector<ObjectID> *return_ids) {
  
  auto &context = core_worker_.worker_context_;
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  auto num_returns = call_options.num_returns;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::ForTaskReturn(task_id, num_returns);
  }

  std::vector<std::shared_ptr<raylet::TaskArgument>> task_arguments;
  for (auto &arg : args) {
    if (arg.IsPassedByReference()) {
      std::vector<ObjectID> references{ arg.GetReference() };
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByReference>(references));
    } else {
      auto data = arg.GetValue();
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByValue>(
          data->Data(), data->Size()));
    }
  }

  ray::raylet::TaskSpecification spec(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), -1,
      task_arguments, num_returns, call_options.resources,
      function.language, function.function_descriptor);
  
  std::vector<ObjectID> execution_dependencies;
  return core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<TaskArg> &args,
    const ActorCreationOptions &actor_creation_options,
    std::unique_ptr<ActorHandle> *actor_handle) {

  auto &context = core_worker_.worker_context_;
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  std::vector<ObjectID> return_ids;
  return_ids.push_back(ObjectID::ForTaskReturn(task_id, 1));
  ActorID actor_creation_id = ActorID::FromBinary(return_ids[0].Binary());

  std::vector<std::shared_ptr<raylet::TaskArgument>> task_arguments;
  for (auto &arg : args) {
    if (arg.IsPassedByReference()) {
      std::vector<ObjectID> references{ arg.GetReference() };
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByReference>(references));
    } else {
      auto data = arg.GetValue();
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByValue>(
          data->Data(), data->Size()));
    }
  }

  // Note that the caller is supposed to specify required placement resources
  // correctly via actor_creation_options.resources.
  ray::raylet::TaskSpecification spec(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), -1, actor_creation_id,
      ObjectID::Nil(), actor_creation_options.max_reconstructions,
      ActorID::Nil(), ActorHandleID::Nil(), 0, {},
      task_arguments, 1, actor_creation_options.resources,
      actor_creation_options.resources,
      function.language, function.function_descriptor);
      
  *actor_handle = std::unique_ptr<ActorHandle>(
      new ActorHandle(actor_creation_id, ActorHandleID::Nil()));

  std::vector<ObjectID> execution_dependencies;
  return core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::SubmitActorTask(ActorHandle &actor_handle,
                                              const RayFunction &function,
                                              const std::vector<TaskArg> &args,
                                              const TaskOptions &call_options,
                                              std::vector<ObjectID> *return_ids) {
  
  auto &context = core_worker_.worker_context_;
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  // add one for actor cursor object id.
  auto num_returns = call_options.num_returns + 1;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::ForTaskReturn(task_id, num_returns);
  }

  auto actor_cursor = (*return_ids).back();

  std::vector<std::shared_ptr<raylet::TaskArgument>> task_arguments;
  for (auto &arg : args) {
    if (arg.IsPassedByReference()) {
      std::vector<ObjectID> references{ arg.GetReference() };
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByReference>(references));
    } else {
      auto data = arg.GetValue();
      task_arguments.push_back(std::make_shared<raylet::TaskArgumentByValue>(
          data->Data(), data->Size()));
    }
  }

  std::vector<ActorHandleID> new_actor_handles{ actor_handle.GetNewActorHandle() };
  ray::raylet::TaskSpecification spec(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), -1, ActorID::Nil(), actor_cursor, 0,
      actor_handle.ActorID(), actor_handle.ActorHandleID(),
      actor_handle.IncreaseTaskCounter(), new_actor_handles,
      task_arguments, num_returns, call_options.resources, {},
      function.language, function.function_descriptor);
  
  std::vector<ObjectID> execution_dependencies;
  execution_dependencies.push_back(actor_handle.ActorCursor());

  auto status = core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);
  
  actor_handle.SetActorCursor(actor_cursor);
  // remove cursor from return ids.
  (*return_ids).pop_back();
  return status;
}


}  // namespace ray
