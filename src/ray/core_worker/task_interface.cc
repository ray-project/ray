#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id, const class ActorHandleID &actor_handle_id,
    const class JobID &job_id, const Language actor_language, bool is_direct_call,
    const std::vector<std::string> &actor_creation_task_function_descriptor) {
  inner_.set_actor_id(actor_id.Data(), actor_id.Size());
  inner_.set_actor_handle_id(actor_handle_id.Data(), actor_handle_id.Size());
  inner_.set_job_id(job_id.Data(), job_id.Size());
  inner_.set_actor_language(actor_language);
  *inner_.mutable_actor_creation_task_function_descriptor() = {
      actor_creation_task_function_descriptor.begin(),
      actor_creation_task_function_descriptor.end()};
  const auto &actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const auto &actor_creation_dummy_object_id =
      ObjectID::ForTaskReturn(actor_creation_task_id, /*index=*/1, /*transport_type=*/0);
  inner_.set_actor_cursor(actor_creation_dummy_object_id.Data(),
                          actor_creation_dummy_object_id.Size());
  inner_.set_is_direct_call(is_direct_call);
}

ActorHandle::ActorHandle(ActorHandle &parent, bool in_band) {
  std::unique_lock<std::mutex> guard(parent.mutex_);
  inner_ = parent.inner_;
  class ActorHandleID new_actor_handle_id;
  if (in_band) {
    new_actor_handle_id = ComputeForkedActorHandleId(
        ActorHandleID::FromBinary(parent.inner_.actor_handle_id()),
        parent.IncrementNumForks());
    // Notify the backend to expect this new actor handle. The backend will
    // not release the cursor for any new handles until the first task for
    // each of the new handles is submitted.
    // NOTE(swang): There is currently no garbage collection for actor
    // handles until the actor itself is removed.
    new_actor_handles_.push_back(new_actor_handle_id);
  } else {
    // If this serialization is happening out-of-band, we set the actor handle ID.
    // to nil to signal that it should be computed when the handle is deserialized.
    new_actor_handle_id = ActorHandleID::Nil();
    // The execution dependency for a pickled actor handle is never safe
    // to release, since it could be unpickled and submit another
    // dependent task at any time. Therefore, we notify the backend of a
    // random handle ID that will never actually be used.
    new_actor_handles_.push_back(ActorHandleID::FromRandom());
  }
  guard.unlock();

  inner_.set_actor_handle_id(new_actor_handle_id.Data(), new_actor_handle_id.Size());
  inner_.set_task_counter(0);
  inner_.set_num_forks(0);
}

ActorHandle::ActorHandle(const std::string &serialized, const TaskID &current_task_id) {
  inner_.ParseFromString(serialized);
  // If the actor handle ID is nil, this serialized handle was created by an out-of-band
  // mechanism (see fork constructor above), so we compute a new actor handle ID.
  // TODO(pcm): This still leads to a lot of actor handles being
  // created, there should be a better way to handle pickled
  // actor handles.
  // TODO(swang): Unpickling the same actor handle twice in the same
  // task will break the application, and unpickling it twice in the
  // same actor is likely a performance bug. We should consider
  // logging a warning in these cases.
  if (ActorHandleID::FromBinary(inner_.actor_handle_id()).IsNil()) {
    const class ActorHandleID new_actor_handle_id = ComputeOutOfBandActorHandleId(
        ActorHandleID::FromBinary(inner_.actor_handle_id()), current_task_id);
    inner_.set_actor_handle_id(new_actor_handle_id.Data(), new_actor_handle_id.Size());
  }
}

ActorID ActorHandle::ActorID() const { return ActorID::FromBinary(inner_.actor_id()); };

ActorHandleID ActorHandle::ActorHandleID() const {
  return ActorHandleID::FromBinary(inner_.actor_handle_id());
};

JobID ActorHandle::JobID() const { return JobID::FromBinary(inner_.job_id()); };

Language ActorHandle::ActorLanguage() const { return inner_.actor_language(); };

std::vector<std::string> ActorHandle::ActorCreationTaskFunctionDescriptor() const {
  return VectorFromProtobuf(inner_.actor_creation_task_function_descriptor());
};

ObjectID ActorHandle::ActorCursor() const {
  return ObjectID::FromBinary(inner_.actor_cursor());
};

int64_t ActorHandle::TaskCounter() const { return inner_.task_counter(); };

int64_t ActorHandle::IncrementTaskCounter() {
  int64_t old = inner_.task_counter();
  inner_.set_task_counter(old + 1);
  return old;
}

int64_t ActorHandle::NumForks() const { return inner_.num_forks(); };

int64_t ActorHandle::IncrementNumForks() {
  int64_t old = inner_.num_forks();
  inner_.set_num_forks(old + 1);
  return old;
}

bool ActorHandle::IsDirectCallActor() const { return inner_.is_direct_call(); }

void ActorHandle::Serialize(std::string *output) {
  std::unique_lock<std::mutex> guard(mutex_);
  inner_.SerializeToString(output);
}

ActorHandle::ActorHandle() {}

void ActorHandle::SetActorCursor(const ObjectID &actor_cursor) {
  inner_.set_actor_cursor(actor_cursor.Binary());
};

std::vector<ActorHandleID> ActorHandle::NewActorHandles() const {
  return new_actor_handles_;
}

void ActorHandle::ClearNewActorHandles() { new_actor_handles_.clear(); }

CoreWorkerTaskInterface::CoreWorkerTaskInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerObjectInterface &object_interface, boost::asio::io_service &io_service,
    gcs::RedisGcsClient &gcs_client)
    : worker_context_(worker_context) {
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter(raylet_client)));
  task_submitters_.emplace(
      TaskTransportType::DIRECT_ACTOR,
      std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
          new CoreWorkerDirectActorTaskSubmitter(
              io_service, gcs_client,
              object_interface.CreateStoreProvider(StoreProviderType::MEMORY))));
}

void CoreWorkerTaskInterface::BuildCommonTaskSpec(
    TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
    const int task_index, const RayFunction &function, const std::vector<TaskArg> &args,
    uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    TaskTransportType transport_type, std::vector<ObjectID> *return_ids) {
  // Build common task spec.
  builder.SetCommonTaskSpec(task_id, function.GetLanguage(),
                            function.GetFunctionDescriptor(), job_id,
                            worker_context_.GetCurrentTaskID(), task_index, num_returns,
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

Status CoreWorkerTaskInterface::SubmitTask(const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &task_options,
                                           std::vector<ObjectID> *return_ids) {
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const auto task_id =
      TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                            worker_context_.GetCurrentTaskID(), next_task_index);
  BuildCommonTaskSpec(builder, worker_context_.GetCurrentJobID(), task_id,
                      next_task_index, function, args, task_options.num_returns,
                      task_options.resources, {}, TaskTransportType::RAYLET, return_ids);
  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<TaskArg> &args,
    const ActorCreationOptions &actor_creation_options,
    std::unique_ptr<ActorHandle> *actor_handle) {
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
                  next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  BuildCommonTaskSpec(builder, job_id, actor_creation_task_id, next_task_index, function,
                      args, 1, actor_creation_options.resources,
                      actor_creation_options.placement_resources,
                      TaskTransportType::RAYLET, &return_ids);
  builder.SetActorCreationTaskSpec(actor_id, actor_creation_options.max_reconstructions,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.is_direct_call);

  *actor_handle = std::unique_ptr<ActorHandle>(new ActorHandle(
      actor_id, ActorHandleID::Nil(), job_id, function.GetLanguage(),
      actor_creation_options.is_direct_call, function.GetFunctionDescriptor()));
  (*actor_handle)->IncrementTaskCounter();
  (*actor_handle)->SetActorCursor(return_ids[0]);

  return task_submitters_[TaskTransportType::RAYLET]->SubmitTask(builder.Build());
}

Status CoreWorkerTaskInterface::SubmitActorTask(ActorHandle &actor_handle,
                                                const RayFunction &function,
                                                const std::vector<TaskArg> &args,
                                                const TaskOptions &task_options,
                                                std::vector<ObjectID> *return_ids) {
  // Add one for actor cursor object id for tasks.
  const auto num_returns = task_options.num_returns + 1;

  const bool is_direct_call = actor_handle.IsDirectCallActor();
  const auto transport_type =
      is_direct_call ? TaskTransportType::DIRECT_ACTOR : TaskTransportType::RAYLET;

  // Build common task spec.
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const auto actor_task_id = TaskID::ForActorTask(
      worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
      next_task_index, actor_handle.ActorID());
  BuildCommonTaskSpec(builder, actor_handle.JobID(), actor_task_id, next_task_index,
                      function, args, num_returns, task_options.resources, {},
                      transport_type, return_ids);

  std::unique_lock<std::mutex> guard(actor_handle.mutex_);
  // Build actor task spec.
  const auto actor_creation_task_id =
      TaskID::ForActorCreationTask(actor_handle.ActorID());
  const auto actor_creation_dummy_object_id =
      ObjectID::ForTaskReturn(actor_creation_task_id, /*index=*/1,
                              /*transport_type=*/static_cast<int>(transport_type));
  builder.SetActorTaskSpec(
      actor_handle.ActorID(), actor_handle.ActorHandleID(),
      actor_creation_dummy_object_id,
      /*previous_actor_task_dummy_object_id=*/actor_handle.ActorCursor(),
      actor_handle.IncrementTaskCounter(), actor_handle.NewActorHandles());

  // Manipulate actor handle state.
  auto actor_cursor = (*return_ids).back();
  actor_handle.SetActorCursor(actor_cursor);
  actor_handle.ClearNewActorHandles();

  guard.unlock();

  // Submit task.
  auto status = task_submitters_[transport_type]->SubmitTask(builder.Build());
  // Remove cursor from return ids.
  (*return_ids).pop_back();

  return status;
}

}  // namespace ray
