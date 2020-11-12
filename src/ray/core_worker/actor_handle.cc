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
    const std::string &extension_data, int64_t max_task_retries) {
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
  inner.set_max_task_retries(max_task_retries);
  return inner;
}

ray::rpc::ActorHandle CreateInnerActorHandleFromString(const std::string &serialized) {
  ray::rpc::ActorHandle inner;
  inner.ParseFromString(serialized);
  return inner;
}

ray::rpc::ActorHandle CreateInnerActorHandleFromActorTableData(
    const ray::rpc::ActorTableData &actor_table_data) {
  ray::rpc::ActorHandle inner;
  inner.set_actor_id(actor_table_data.actor_id());
  inner.set_owner_id(actor_table_data.parent_id());
  inner.mutable_owner_address()->CopyFrom(actor_table_data.owner_address());
  inner.set_creation_job_id(actor_table_data.job_id());
  inner.set_actor_language(actor_table_data.language());
  inner.mutable_actor_creation_task_function_descriptor()->CopyFrom(
      actor_table_data.function_descriptor());
  auto actor_id = ActorID::FromBinary(actor_table_data.actor_id());
  auto task_id = TaskID::ForActorCreationTask(actor_id);
  inner.set_actor_cursor(ObjectID::FromIndex(task_id, 1).Binary());
  inner.set_extension_data(actor_table_data.extension_data());
  inner.set_max_task_retries(actor_table_data.max_task_retries());
  return inner;
}

}  // namespace

namespace ray {

ActorHandle::ActorHandle(
    const class ActorID &actor_id, const TaskID &owner_id,
    const rpc::Address &owner_address, const class JobID &job_id,
    const ObjectID &initial_cursor, const Language actor_language,
    const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data, int64_t max_task_retries)
    : ActorHandle(CreateInnerActorHandle(
          actor_id, owner_id, owner_address, job_id, initial_cursor, actor_language,
          actor_creation_task_function_descriptor, extension_data, max_task_retries)) {}

ActorHandle::ActorHandle(const std::string &serialized)
    : ActorHandle(CreateInnerActorHandleFromString(serialized)) {}

ActorHandle::ActorHandle(const rpc::ActorTableData &actor_table_data)
    : ActorHandle(CreateInnerActorHandleFromActorTableData(actor_table_data)) {}

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder, const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::FromIndex(actor_creation_task_id, /*index=*/1);
  builder.SetActorTaskSpec(GetActorID(), actor_creation_dummy_object_id,
                           /*previous_actor_task_dummy_object_id=*/actor_cursor_,
                           task_counter_++);
  actor_cursor_ = new_cursor;
}

void ActorHandle::SetResubmittedActorTaskSpec(TaskSpecification &spec,
                                              const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  auto mutable_spec = spec.GetMutableMessage().mutable_actor_task_spec();
  mutable_spec->set_previous_actor_task_dummy_object_id(actor_cursor_.Binary());
  mutable_spec->set_actor_counter(task_counter_++);
  actor_cursor_ = new_cursor;
}

void ActorHandle::Serialize(std::string *output) { inner_.SerializeToString(output); }

}  // namespace ray
