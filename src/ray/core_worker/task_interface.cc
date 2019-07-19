#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id, const class ActorHandleID &actor_handle_id,
    const Language actor_language,
    const std::vector<std::string> &actor_creation_task_function_descriptor) {
  inner_.set_actor_id(actor_id.Data(), actor_id.Size());
  inner_.set_actor_handle_id(actor_handle_id.Data(), actor_handle_id.Size());
  inner_.set_actor_language(actor_language);
  *inner_.mutable_actor_creation_task_function_descriptor() = {
      actor_creation_task_function_descriptor.begin(),
      actor_creation_task_function_descriptor.end()};
  inner_.set_actor_cursor(actor_id.Data(), actor_id.Size());
}

ActorHandle::ActorHandle(const ActorHandle &other)
    : inner_(other.inner_), new_actor_handles_(other.new_actor_handles_) {}

ray::ActorID ActorHandle::ActorID() const {
  return ActorID::FromBinary(inner_.actor_id());
};

ray::ActorHandleID ActorHandle::ActorHandleID() const {
  return ActorHandleID::FromBinary(inner_.actor_handle_id());
};

Language ActorHandle::ActorLanguage() const { return inner_.actor_language(); };

std::vector<std::string> ActorHandle::ActorCreationTaskFunctionDescriptor() const {
  return VectorFromProtobuf(inner_.actor_creation_task_function_descriptor());
};

ObjectID ActorHandle::ActorCursor() const {
  return ObjectID::FromBinary(inner_.actor_cursor());
};

int64_t ActorHandle::TaskCounter() const { return inner_.task_counter(); };

int64_t ActorHandle::NumForks() const { return inner_.num_forks(); };

ActorHandle ActorHandle::Fork() {
  ActorHandle new_handle;
  std::unique_lock<std::mutex> guard(mutex_);
  new_handle.inner_ = inner_;
  inner_.set_num_forks(inner_.num_forks() + 1);
  const auto next_actor_handle_id = ComputeNextActorHandleId(
      ActorHandleID::FromBinary(inner_.actor_handle_id()), inner_.num_forks());
  new_handle.inner_.set_actor_handle_id(next_actor_handle_id.Data(),
                                        next_actor_handle_id.Size());
  new_actor_handles_.push_back(next_actor_handle_id);
  guard.unlock();

  new_handle.inner_.set_task_counter(0);
  new_handle.inner_.set_num_forks(0);
  return new_handle;
}

void ActorHandle::Serialize(std::string *output) {
  std::unique_lock<std::mutex> guard(mutex_);
  inner_.SerializeToString(output);
}

ActorHandle ActorHandle::Deserialize(const std::string &data) {
  ActorHandle ret;
  ret.inner_.ParseFromString(data);
  return ret;
}

ActorHandle::ActorHandle() {}

void ActorHandle::SetActorCursor(const ObjectID &actor_cursor) {
  inner_.set_actor_cursor(actor_cursor.Binary());
};

int64_t ActorHandle::IncreaseTaskCounter() {
  int64_t old = inner_.task_counter();
  inner_.set_task_counter(old + 1);
  return old;
}

std::vector<ray::ActorHandleID> ActorHandle::NewActorHandles() const {
  return new_actor_handles_;
}

void ActorHandle::ClearNewActorHandles() { new_actor_handles_.clear(); }

CoreWorkerTaskInterface::CoreWorkerTaskInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client)
    : worker_context_(worker_context) {
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter(raylet_client)));
}

TaskSpecBuilder CoreWorkerTaskInterface::BuildCommonTaskSpec(
    const RayFunction &function, const std::vector<TaskArg> &args, uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    std::vector<ObjectID> *return_ids) {
  TaskSpecBuilder builder;
  auto next_task_index = worker_context_.GetNextTaskIndex();
  // Build common task spec.
  builder.SetCommonTaskSpec(
      function.language, function.function_descriptor, worker_context_.GetCurrentJobID(),
      worker_context_.GetCurrentTaskID(), next_task_index, num_returns,
      required_resources, required_placement_resources);
  // Set task arguments.
  for (const auto &arg : args) {
    if (arg.IsPassedByReference()) {
      builder.AddByRefArg(arg.GetReference());
    } else {
      builder.AddByValueArg(arg.GetValue()->Data(), arg.GetValue()->Size());
    }
  }

  // Compute return IDs.
  const auto task_id = TaskID::FromBinary(builder.GetMessage().task_id());
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::ForTaskReturn(task_id, i + 1);
  }
  return builder;
}

Status CoreWorkerTaskInterface::SubmitTask(const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &task_options,
                                           std::vector<ObjectID> *return_ids) {
  auto builder = BuildCommonTaskSpec(function, args, task_options.num_returns,
                                     task_options.resources, {}, return_ids);
  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<TaskArg> &args,
    const ActorCreationOptions &actor_creation_options,
    std::unique_ptr<ActorHandle> *actor_handle) {
  std::vector<ObjectID> return_ids;
  auto builder = BuildCommonTaskSpec(function, args, 1, actor_creation_options.resources,
                                     actor_creation_options.resources, &return_ids);

  const ActorID actor_id = ActorID::FromBinary(return_ids[0].Binary());
  builder.SetActorCreationTaskSpec(actor_id, actor_creation_options.max_reconstructions,
                                   {});

  *actor_handle = std::unique_ptr<ActorHandle>(new ActorHandle(
      actor_id, ActorHandleID::Nil(), function.language, function.function_descriptor));
  (*actor_handle)->IncreaseTaskCounter();
  (*actor_handle)->SetActorCursor(return_ids[0]);

  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::SubmitActorTask(ActorHandle &actor_handle,
                                                const RayFunction &function,
                                                const std::vector<TaskArg> &args,
                                                const TaskOptions &task_options,
                                                std::vector<ObjectID> *return_ids) {
  // Add one for actor cursor object id.
  const auto num_returns = task_options.num_returns + 1;

  // Build common task spec.
  auto builder = BuildCommonTaskSpec(function, args, num_returns, task_options.resources,
                                     {}, return_ids);

  std::unique_lock<std::mutex> guard(actor_handle.mutex_);
  // Build actor task spec.
  const auto actor_creation_dummy_object_id =
      ObjectID::FromBinary(actor_handle.ActorID().Binary());
  builder.SetActorTaskSpec(
      actor_handle.ActorID(), actor_handle.ActorHandleID(),
      actor_creation_dummy_object_id,
      /*previous_actor_task_dummy_object_id=*/actor_handle.ActorCursor(),
      actor_handle.IncreaseTaskCounter(), actor_handle.NewActorHandles());

  // Manipulate actor handle state.
  auto actor_cursor = (*return_ids).back();
  actor_handle.SetActorCursor(actor_cursor);
  actor_handle.ClearNewActorHandles();
  guard.unlock();

  // Submit task.
  auto status = task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());

  // Remove cursor from return ids.
  (*return_ids).pop_back();
  return status;
}

}  // namespace ray
