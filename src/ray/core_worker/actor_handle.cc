#include "ray/core_worker/actor_handle.h"

#include <memory>

namespace {

ray::rpc::ActorHandle CreateInnerActorHandle(
    const class ActorID &actor_id, const class JobID &job_id,
    const ObjectID &initial_cursor, const Language actor_language, bool is_direct_call,
    const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data) {
  ray::rpc::ActorHandle inner;
  inner.set_actor_id(actor_id.Data(), actor_id.Size());
  inner.set_creation_job_id(job_id.Data(), job_id.Size());
  inner.set_actor_language(actor_language);
  *inner.mutable_actor_creation_task_function_descriptor() =
      actor_creation_task_function_descriptor->GetMessage();
  inner.set_actor_cursor(initial_cursor.Binary());
  inner.set_is_direct_call(is_direct_call);
  inner.set_extension_data(extension_data);
  return inner;
}

ray::rpc::ActorHandle CreateInnerActorHandleFromString(const std::string &serialized) {
  ray::rpc::ActorHandle inner;
  inner.ParseFromString(serialized);
  return inner;
}

}  // namespace

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id, const class JobID &job_id,
    const ObjectID &initial_cursor, const Language actor_language, bool is_direct_call,
    const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data)
    : ActorHandle(CreateInnerActorHandle(
          actor_id, job_id, initial_cursor, actor_language, is_direct_call,
          actor_creation_task_function_descriptor, extension_data)) {}

ActorHandle::ActorHandle(const std::string &serialized)
    : ActorHandle(CreateInnerActorHandleFromString(serialized)) {}

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder,
                                   const TaskTransportType transport_type,
                                   const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::ForTaskReturn(actor_creation_task_id, /*index=*/1,
                              /*transport_type=*/static_cast<int>(transport_type));
  builder.SetActorTaskSpec(GetActorID(), actor_creation_dummy_object_id,
                           /*previous_actor_task_dummy_object_id=*/actor_cursor_,
                           task_counter_++);
  actor_cursor_ = new_cursor;
}

void ActorHandle::Serialize(std::string *output) { inner_.SerializeToString(output); }

void ActorHandle::Reset() {
  absl::MutexLock guard(&mutex_);
  actor_cursor_ = ObjectID::FromBinary(inner_.actor_cursor());
}

}  // namespace ray
