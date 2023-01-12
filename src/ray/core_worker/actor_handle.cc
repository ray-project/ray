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

namespace ray {
namespace core {
namespace {
rpc::ActorHandle CreateInnerActorHandle(
    const class ActorID &actor_id,
    const TaskID &owner_id,
    const rpc::Address &owner_address,
    const class JobID &job_id,
    const ObjectID &initial_cursor,
    const Language actor_language,
    const FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data,
    int64_t max_task_retries,
    const std::string &name,
    const std::string &ray_namespace,
    int32_t max_pending_calls,
    bool execute_out_of_order) {
  rpc::ActorHandle inner;
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
  inner.set_name(name);
  inner.set_ray_namespace(ray_namespace);
  inner.set_execute_out_of_order(execute_out_of_order);
  inner.set_max_pending_calls(max_pending_calls);
  return inner;
}

rpc::ActorHandle CreateInnerActorHandleFromString(const std::string &serialized) {
  rpc::ActorHandle inner;
  inner.ParseFromString(serialized);
  return inner;
}

rpc::ActorHandle CreateInnerActorHandleFromActorData(
    const rpc::ActorTableData &actor_table_data, const rpc::TaskSpec &task_spec) {
  rpc::ActorHandle inner;
  inner.set_actor_id(actor_table_data.actor_id());
  inner.set_owner_id(actor_table_data.parent_id());
  inner.mutable_owner_address()->CopyFrom(actor_table_data.owner_address());
  inner.set_creation_job_id(actor_table_data.job_id());
  inner.set_actor_language(task_spec.language());
  inner.mutable_actor_creation_task_function_descriptor()->CopyFrom(
      actor_table_data.function_descriptor());
  inner.set_actor_cursor(
      ObjectID::FromIndex(
          TaskID::ForActorCreationTask(ActorID::FromBinary(actor_table_data.actor_id())),
          1)
          .Binary());
  inner.set_extension_data(task_spec.actor_creation_task_spec().extension_data());
  inner.set_max_task_retries(task_spec.actor_creation_task_spec().max_task_retries());
  inner.set_name(actor_table_data.name());
  inner.set_ray_namespace(actor_table_data.ray_namespace());
  inner.set_execute_out_of_order(
      task_spec.actor_creation_task_spec().execute_out_of_order());
  inner.set_max_pending_calls(task_spec.actor_creation_task_spec().max_pending_calls());
  return inner;
}
}  // namespace

ActorHandle::ActorHandle(
    const class ActorID &actor_id,
    const TaskID &owner_id,
    const rpc::Address &owner_address,
    const class JobID &job_id,
    const ObjectID &initial_cursor,
    const Language actor_language,
    const FunctionDescriptor &actor_creation_task_function_descriptor,
    const std::string &extension_data,
    int64_t max_task_retries,
    const std::string &name,
    const std::string &ray_namespace,
    int32_t max_pending_calls,
    bool execute_out_of_order)
    : ActorHandle(CreateInnerActorHandle(actor_id,
                                         owner_id,
                                         owner_address,
                                         job_id,
                                         initial_cursor,
                                         actor_language,
                                         actor_creation_task_function_descriptor,
                                         extension_data,
                                         max_task_retries,
                                         name,
                                         ray_namespace,
                                         max_pending_calls,
                                         execute_out_of_order)) {}

ActorHandle::ActorHandle(const std::string &serialized)
    : ActorHandle(CreateInnerActorHandleFromString(serialized)) {}

ActorHandle::ActorHandle(const rpc::ActorTableData &actor_table_data,
                         const rpc::TaskSpec &task_spec)
    : ActorHandle(CreateInnerActorHandleFromActorData(actor_table_data, task_spec)) {}

void ActorHandle::SetActorTaskSpec(TaskSpecBuilder &builder, const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::FromIndex(actor_creation_task_id, /*index=*/1);
  builder.SetActorTaskSpec(GetActorID(), actor_creation_dummy_object_id, task_counter_++);
}

void ActorHandle::SetResubmittedActorTaskSpec(TaskSpecification &spec,
                                              const ObjectID new_cursor) {
  absl::MutexLock guard(&mutex_);
  auto mutable_spec = spec.GetMutableMessage().mutable_actor_task_spec();
  mutable_spec->set_actor_counter(task_counter_++);
}

void ActorHandle::Serialize(std::string *output) { inner_.SerializeToString(output); }

std::string ActorHandle::GetName() const { return inner_.name(); }

std::string ActorHandle::GetNamespace() const { return inner_.ray_namespace(); }

}  // namespace core
}  // namespace ray
