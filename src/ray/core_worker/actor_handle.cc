#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/task_interface.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id, const class ActorHandleID &actor_handle_id,
    const class JobID &job_id, const ObjectID &initial_cursor,
    const Language actor_language, bool is_direct_call,
    const std::vector<std::string> &actor_creation_task_function_descriptor) {
  inner_.set_actor_id(actor_id.Data(), actor_id.Size());
  inner_.set_actor_handle_id(actor_handle_id.Data(), actor_handle_id.Size());
  inner_.set_creation_job_id(job_id.Data(), job_id.Size());
  inner_.set_actor_language(actor_language);
  *inner_.mutable_actor_creation_task_function_descriptor() = {
      actor_creation_task_function_descriptor.begin(),
      actor_creation_task_function_descriptor.end()};
  inner_.set_actor_cursor(initial_cursor.Binary());
  inner_.set_is_direct_call(is_direct_call);
  // Increment the task counter to account for the actor creation task.
  task_counter_++;
}

ActorHandle::ActorHandle(ActorHandle &parent, bool in_band) {
  std::unique_lock<std::mutex> guard(parent.mutex_);
  inner_ = parent.inner_;
  class ActorHandleID new_actor_handle_id;
  if (in_band) {
    new_actor_handle_id = ComputeForkedActorHandleId(
        ActorHandleID::FromBinary(parent.inner_.actor_handle_id()),
        parent.num_forks_++);
    // Notify the backend to expect this new actor handle. The backend will
    // not release the cursor for any new handles until the first task for
    // each of the new handles is submitted.
    // NOTE(swang): There is currently no garbage collection for actor
    // handles until the actor itself is removed.
    parent.new_actor_handles_.push_back(new_actor_handle_id);
  } else {
    // If this serialization is happening out-of-band, we set the actor handle ID.
    // to nil to signal that it should be computed when the handle is deserialized.
    new_actor_handle_id = ActorHandleID::Nil();
    // The execution dependency for a pickled actor handle is never safe
    // to release, since it could be unpickled and submit another
    // dependent task at any time. Therefore, we notify the backend of a
    // random handle ID that will never actually be used.
    parent.new_actor_handles_.push_back(ActorHandleID::FromRandom());
  }
  guard.unlock();

  inner_.set_actor_handle_id(new_actor_handle_id.Data(), new_actor_handle_id.Size());
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

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder,
                                   const TaskTransportType transport_type,
                                   const ObjectID new_cursor) {
  std::unique_lock<std::mutex> guard(mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(ActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::ForTaskReturn(actor_creation_task_id, /*index=*/1,
                              /*transport_type=*/static_cast<int>(transport_type));
  builder.SetActorTaskSpec(ActorID(), ActorHandleID(), actor_creation_dummy_object_id,
                           /*previous_actor_task_dummy_object_id=*/ActorCursor(),
                           task_counter_++, new_actor_handles_);

  inner_.set_actor_cursor(new_cursor.Binary());
  new_actor_handles_.clear();
}

void ActorHandle::Serialize(std::string *output) {
  std::unique_lock<std::mutex> guard(mutex_);
  inner_.SerializeToString(output);
}

}  // namespace ray
