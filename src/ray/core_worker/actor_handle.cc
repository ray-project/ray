// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/actor_handle.h"

#include <memory>

namespace {

ray::rpc::ActorHandle CreateInnerActorHandle(
    const class ActorID &actor_id, const TaskID &owner_id,
    const ray::rpc::Address &owner_address, const class JobID &job_id,
    const ObjectID &initial_cursor, const Language actor_language,
    const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data) {
  ray::rpc::ActorHandle inner;
  inner.set_actor_id(actor_id.Data(), actor_id.Size());
  inner.set_owner_id(owner_id.Binary());
  inner.mutable_owner_address()->CopyFrom(owner_address);
  inner.set_creation_job_id(job_id.Data(), job_id.Size());
  inner.set_actor_language(actor_language);
  *inner.mutable_actor_creation_task_function_descriptor() =
      actor_creation_task_function_descriptor->GetMessage();
  inner.set_actor_cursor(initial_cursor.Binary());
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
    const class ActorID &actor_id, const TaskID &owner_id,
    const rpc::Address &owner_address, const class JobID &job_id,
    const ObjectID &initial_cursor, const Language actor_language,
    const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data)
    : ActorHandle(CreateInnerActorHandle(
          actor_id, owner_id, owner_address, job_id, initial_cursor, actor_language,
          actor_creation_task_function_descriptor, extension_data)) {}

ActorHandle::ActorHandle(const std::string &serialized)
    : ActorHandle(CreateInnerActorHandleFromString(serialized)) {}

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder, const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id = ObjectID::ForTaskReturn(
      actor_creation_task_id, /*index=*/1,
      /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
  builder.SetActorTaskSpec(GetActorID(), actor_creation_dummy_object_id,
                           /*previous_actor_task_dummy_object_id=*/actor_cursor_,
                           task_counter_++);
  actor_cursor_ = new_cursor;
}

void ActorHandle::Serialize(std::string *output) { inner_.SerializeToString(output); }

void ActorHandle::Reset() {
  absl::MutexLock guard(&mutex_);
  task_counter_ = 0;
  actor_cursor_ = ObjectID::FromBinary(inner_.actor_cursor());
}

}  // namespace ray
