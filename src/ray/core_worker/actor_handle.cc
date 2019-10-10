#include <memory>

#include "ray/core_worker/actor_handle.h"

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id,
    const class JobID &job_id, const ObjectID &initial_cursor,
    const Language actor_language, bool is_direct_call,
    const std::vector<std::string> &actor_creation_task_function_descriptor) {
  inner_.set_actor_id(actor_id.Data(), actor_id.Size());
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

ActorHandle::ActorHandle(const std::string &serialized) {
  inner_.ParseFromString(serialized);
}

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder,
                                   const TaskID &actor_caller_id,
                                   const TaskTransportType transport_type,
                                   const ObjectID new_cursor) {
  std::unique_lock<std::mutex> guard(mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::ForTaskReturn(actor_creation_task_id, /*index=*/1,
                              /*transport_type=*/static_cast<int>(transport_type));
  builder.SetActorTaskSpec(GetActorID(), actor_caller_id,
                           actor_creation_dummy_object_id,
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
