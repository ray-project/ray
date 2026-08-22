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

#include "ray/core_worker/actor_management/actor_handle.h"

#include <memory>
#include <string>
#include <unordered_map>

#include "ray/util/logging.h"

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
    bool allow_out_of_order_execution,
    bool enable_tensor_transport,
    std::optional<bool> enable_task_events,
    const std::unordered_map<std::string, std::string> &labels,
    bool is_detached,
    int64_t actor_generator_backpressure_num_objects) {
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
  inner.set_max_pending_calls(max_pending_calls);
  inner.set_allow_out_of_order_execution(allow_out_of_order_execution);
  inner.set_enable_tensor_transport(enable_tensor_transport);
  inner.set_enable_task_events(enable_task_events.value_or(kDefaultTaskEventEnabled));
  inner.mutable_labels()->insert(labels.begin(), labels.end());
  inner.set_is_detached(is_detached);
  if (actor_generator_backpressure_num_objects > 0) {
    inner.set_actor_generator_backpressure_num_objects(
        actor_generator_backpressure_num_objects);
  }
  return inner;
}

rpc::ActorHandle CreateInnerActorHandleFromString(const std::string &serialized) {
  rpc::ActorHandle inner;
  inner.ParseFromString(serialized);
  return inner;
}

rpc::ActorHandle CreateInnerActorHandleFromActorData(
    const rpc::ActorTableData &actor_table_data, const rpc::TaskSpec &task_spec) {
  // Fields below come from three sources: the actor table entry, the creation task
  // spec, and the creator's own handle, which it serialized into that spec and the GCS
  // returns untouched. The last one is the only source for options that neither of the
  // first two carries, so anything added to the ActorHandle proto without a matching
  // ActorCreationTaskSpec field has to be read from there or a handle obtained by name
  // silently reports the option's default.
  rpc::ActorHandle created_handle;
  if (!created_handle.ParseFromString(
          task_spec.actor_creation_task_spec().serialized_actor_handle())) {
    // A failed parse leaves the message partially populated, so drop it and fall back
    // to the defaults rather than reading a field out of the garbage. No in-tree writer
    // produces an unparseable handle; this is defensive.
    created_handle.Clear();
    // Rate limited because the name cache is only populated once a task is submitted to
    // the actor, so a corrupt entry is reparsed on every lookup by any worker that only
    // looks the actor up.
    RAY_LOG_EVERY_MS(WARNING, 60000)
            .WithField(ActorID::FromBinary(actor_table_data.actor_id()))
        << "Could not parse the creator's serialized actor handle. Options carried only "
           "by that handle will fall back to their defaults.";
  }

  rpc::ActorHandle inner;
  inner.set_actor_id(actor_table_data.actor_id());
  inner.set_owner_id(actor_table_data.parent_id());
  inner.mutable_owner_address()->CopyFrom(actor_table_data.owner_address());
  inner.set_creation_job_id(actor_table_data.job_id());
  inner.set_actor_language(task_spec.language());
  inner.mutable_actor_creation_task_function_descriptor()->CopyFrom(
      actor_table_data.function_descriptor());
  inner.set_enable_task_events(task_spec.enable_task_events());
  inner.set_actor_cursor(
      ObjectID::FromIndex(
          TaskID::ForActorCreationTask(ActorID::FromBinary(actor_table_data.actor_id())),
          1)
          .Binary());
  inner.set_extension_data(task_spec.actor_creation_task_spec().extension_data());
  inner.set_max_task_retries(task_spec.actor_creation_task_spec().max_task_retries());
  inner.set_name(actor_table_data.name());
  inner.set_ray_namespace(actor_table_data.ray_namespace());
  inner.set_allow_out_of_order_execution(
      task_spec.actor_creation_task_spec().allow_out_of_order_execution());
  // ActorCreationTaskSpec also has a max_pending_calls field, but nothing on the path
  // that reaches the GCS writes it, so the value read there was always 0 (unlimited).
  inner.set_max_pending_calls(created_handle.max_pending_calls());
  inner.set_enable_tensor_transport(created_handle.enable_tensor_transport());
  inner.mutable_labels()->insert(task_spec.labels().begin(), task_spec.labels().end());
  inner.set_is_detached(task_spec.actor_creation_task_spec().is_detached());
  int64_t actor_generator_backpressure_num_objects =
      task_spec.actor_creation_task_spec().actor_generator_backpressure_num_objects();
  if (actor_generator_backpressure_num_objects > 0) {
    inner.set_actor_generator_backpressure_num_objects(
        actor_generator_backpressure_num_objects);
  }
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
    bool allow_out_of_order_execution,
    bool enable_tensor_transport,
    std::optional<bool> enable_task_events,
    const std::unordered_map<std::string, std::string> &labels,
    bool is_detached,
    int64_t actor_generator_backpressure_num_objects)
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
                                         allow_out_of_order_execution,
                                         enable_tensor_transport,
                                         enable_task_events,
                                         labels,
                                         is_detached,
                                         actor_generator_backpressure_num_objects)) {}

ActorHandle::ActorHandle(const std::string &serialized)
    : ActorHandle(CreateInnerActorHandleFromString(serialized)) {}

ActorHandle::ActorHandle(const rpc::ActorTableData &actor_table_data,
                         const rpc::TaskSpec &task_spec)
    : ActorHandle(CreateInnerActorHandleFromActorData(actor_table_data, task_spec)) {}

void ActorHandle::SetActorTaskSpec(
    TaskSpecBuilder &builder,
    const ObjectID new_cursor,
    int max_retries,
    bool retry_exceptions,
    const std::string &serialized_retry_exception_allowlist,
    const std::string &concurrency_group_name,
    const std::optional<std::string> &tensor_transport) {
  absl::MutexLock guard(&mutex_);
  // Build actor task spec.
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(GetActorID());
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::FromIndex(actor_creation_task_id, /*index=*/1);
  int64_t actor_generator_bp = -1;
  if (inner_.has_actor_generator_backpressure_num_objects()) {
    actor_generator_bp = inner_.actor_generator_backpressure_num_objects();
  }
  builder.SetActorTaskSpec(GetActorID(),
                           actor_creation_dummy_object_id,
                           max_retries,
                           retry_exceptions,
                           serialized_retry_exception_allowlist,
                           concurrency_group_counters_[concurrency_group_name]++,
                           tensor_transport,
                           inner_.is_detached(),
                           actor_generator_bp);
}

void ActorHandle::SetResubmittedActorTaskSpec(TaskSpecification &spec) {
  absl::MutexLock guard(&mutex_);
  auto mutable_spec = spec.GetMutableMessage().mutable_actor_task_spec();
  const std::string &group = spec.ConcurrencyGroupName();
  mutable_spec->set_concurrency_group_sequence_number(
      concurrency_group_counters_[group]++);
}

void ActorHandle::Serialize(std::string *output) { inner_.SerializeToString(output); }

std::string ActorHandle::GetName() const { return inner_.name(); }

std::string ActorHandle::GetNamespace() const { return inner_.ray_namespace(); }

}  // namespace core
}  // namespace ray
