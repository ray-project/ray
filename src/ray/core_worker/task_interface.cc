#include "task_interface.h"

namespace ray {

Status CoreWorkerTaskInterface::CallTask(const RayFunction &function,
                                         const std::vector<Arg> &args,
                                         const CallOptions &call_options,
                                         std::vector<ObjectID> *return_ids) {
  
  auto &context = core_worker_->GetContext();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  auto num_returns = call_options.num_returns;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::for_task_return(task_id, num_returns);
  }

  std::vector<std::shared_ptr<TaskArgument>> task_arguments;
  for (const auto &arg : args) {
    if (arg.id != nullptr) {
      task_arguments.push_back(TaskArgumentByReference({arg.id}));
    } else {
      task_arguments.push_back(TaskArgumentByValue(
          arg.data->Data(), arg.data->Length()));
    }
  }

  ray::raylet::TaskSpecification spec(context_.GetCurrentDriverID(),
      context_.GetCurrentTaskID(), -1,
      task_arguments, num_returns, call_options.resources,
      function.language, function.function_descriptors);
  
  std::vector<ObjectID> execution_dependencies;
  return SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<Arg> args,
    const ActorCreationOptions &actor_creation_options, ActorHandle *actor_handle) {

  auto &context = core_worker_->GetContext();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  std::vector<ObjectID> return_ids;
  return_ids.push_back(ObjectID::for_task_return(task_id, 1));
  const auto &actor_creation_id = return_ids[0];

  std::vector<std::shared_ptr<TaskArgument>> task_arguments;
  for (const auto &arg : args) {
    if (arg.id != nullptr) {
      task_arguments.push_back(TaskArgumentByReference({arg.id}));
    } else {
      task_arguments.push_back(TaskArgumentByValue(
          arg.data->Data(), arg.data->Length()));
    }
  }

  // Note that the caller is supposed to specify required placement resources
  // correctly via actor_creation_options.resources.
  ray::raylet::TaskSpecification spec(context_.GetCurrentDriverID(),
      task_id, context_.GetCurrentTaskID(), -1, actor_creation_id,
      actor_creation_options.max_reconstructions,
      ActorID()::nil(), ActorHandleID()::nil(), 0, {},
      task_arguments, 1, actor_creation_options.resources,
      actor_creation_options.resources,
      function.language, function.function_descriptors);
      
  *actor_handle = ActorHandle(actor_creation_id, ActorHandleID()::nil());

  std::vector<ObjectID> execution_dependencies;
  return SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::CallActorTask(ActorHandle &actor_handle,
                                              const RayFunction &function,
                                              const std::vector<Arg> &args,
                                              const CallOptions &call_options,
                                              std::vector<ObjectID> *return_ids) {

  
  auto &context = core_worker_->GetContext();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  // add one for actor cursor object id.
  auto num_returns = call_options.num_returns + 1;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::for_task_return(task_id, num_returns);
  }

  std::vector<std::shared_ptr<TaskArgument>> task_arguments;
  for (const auto &arg : args) {
    if (arg.id != nullptr) {
      task_arguments.push_back(TaskArgumentByReference({arg.id}));
    } else {
      task_arguments.push_back(TaskArgumentByValue(
          arg.data->Data(), arg.data->Length()));
    }
  }

  ray::raylet::TaskSpecification spec(context_.GetCurrentDriverID(),
      task_id, context_.GetCurrentTaskID(), -1, ActorID::nil(), 0,
      actor_handle.ActorID(), actor_handle.ActorHandleID(),
      actor.IncreaseTaskCounter(),
      // TODO: implement this.
      actor.GetNewActorHandles(),
      task_arguments, num_returns, call_options.resources, {},
      function.language, function.function_descriptors);
  
  std::vector<ObjectID> execution_dependencies;
  execution_dependencies.push_back(actor_handle.ActorCursor());

  auto status = SubmitTask(execution_dependencies, spec);
  
  actor_handle.SetActorCursor(return_ids.back());
  // remove cursor from return ids.
  return_ids.pop_back();
  return status;
}


}  // namespace ray
