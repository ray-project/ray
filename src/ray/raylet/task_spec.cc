
#include <sstream>

#include "ray/raylet/task_spec.h"
#include "ray/rpc/util.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

using rpc::MapFromProtobuf;
using rpc::VectorFromProtobuf;

void TaskSpecification::ComputeResources() {
  auto required_resources = MapFromProtobuf(message_->required_resources());
  auto required_placement_resources =
      MapFromProtobuf(message_->required_placement_resources());
  if (required_placement_resources.empty()) {
    required_placement_resources = required_resources;
  }
  required_resources_ = ResourceSet(required_resources);
  required_placement_resources_ = ResourceSet(required_placement_resources);
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  return TaskID::FromBinary(message_->task_id());
}

JobID TaskSpecification::JobId() const { return JobID::FromBinary(message_->job_id()); }

TaskID TaskSpecification::ParentTaskId() const {
  return TaskID::FromBinary(message_->parent_task_id());
}

size_t TaskSpecification::ParentCounter() const { return message_->parent_counter(); }

std::vector<std::string> TaskSpecification::FunctionDescriptor() const {
  return VectorFromProtobuf(message_->function_descriptor());
}

std::string TaskSpecification::FunctionDescriptorString() const {
  auto list = VectorFromProtobuf(message_->function_descriptor());
  std::ostringstream stream;
  // The 4th is the code hash which is binary bits. No need to output it.
  size_t size = std::min(static_cast<size_t>(3), list.size());
  for (int i = 0; i < size; ++i) {
    if (i != 0) {
      stream << ",";
    }
    stream << list[i];
  }
  return stream.str();
}

size_t TaskSpecification::NumArgs() const { return message_->args_size(); }

size_t TaskSpecification::NumReturns() const { return message_->num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::ForTaskReturn(TaskId(), return_index + 1);
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

size_t TaskSpecification::ArgIdCount(size_t arg_index) const {
  return message_->args(arg_index).object_ids_size();
}

ObjectID TaskSpecification::ArgId(size_t arg_index, size_t id_index) const {
  return ObjectID::FromBinary(message_->args(arg_index).object_ids(id_index));
}

const uint8_t *TaskSpecification::ArgVal(size_t arg_index) const {
  return reinterpret_cast<const uint8_t *>(message_->args(arg_index).data().c_str());
}

size_t TaskSpecification::ArgValLength(size_t arg_index) const {
  return message_->args(arg_index).data().size();
}

const ResourceSet TaskSpecification::GetRequiredResources() const {
  return required_resources_;
}

const ResourceSet TaskSpecification::GetRequiredPlacementResources() const {
  return required_placement_resources_;
}

bool TaskSpecification::IsDriverTask() const {
  // Driver tasks are empty tasks that have no function ID set.
  return FunctionDescriptor().empty();
}

rpc::Language TaskSpecification::GetLanguage() const { return message_->language(); }

bool TaskSpecification::IsActorCreationTask() const {
  return message_->type() == rpc::TaskType::ACTOR_CREATION_TASK;
}

bool TaskSpecification::IsActorTask() const {
  return message_->type() == rpc::TaskType::ACTOR_TASK;
}

ActorID TaskSpecification::ActorCreationId() const {
  if (!IsActorCreationTask()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_->actor_creation_task_spec().actor_id());
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  if (!IsActorTask()) {
    return ObjectID::Nil();
  }
  return ObjectID::FromBinary(
      message_->actor_task_spec().actor_creation_dummy_object_id());
}

uint64_t TaskSpecification::MaxActorReconstructions() const {
  if (!IsActorCreationTask()) {
    return 0;
  }
  return message_->actor_creation_task_spec().max_actor_reconstructions();
}

ActorID TaskSpecification::ActorId() const {
  if (!IsActorTask()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_->actor_task_spec().actor_id());
}

ActorHandleID TaskSpecification::ActorHandleId() const {
  if (!IsActorTask()) {
    return ActorHandleID::Nil();
  }
  return ActorHandleID::FromBinary(message_->actor_task_spec().actor_handle_id());
}

uint64_t TaskSpecification::ActorCounter() const {
  if (!IsActorTask()) {
    return 0;
  }
  return message_->actor_task_spec().actor_counter();
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1);
}

std::vector<ActorHandleID> TaskSpecification::NewActorHandles() const {
  if (!IsActorTask()) {
    return {};
  }
  return rpc::IdVectorFromProtobuf<ActorHandleID>(
      message_->actor_task_spec().new_actor_handles());
}

std::vector<std::string> TaskSpecification::DynamicWorkerOptions() const {
  return rpc::VectorFromProtobuf(
      message_->actor_creation_task_spec().dynamic_worker_options());
}

TaskSpecification *CreateTaskSpec(
    const JobID &job_id, const TaskID &parent_task_id, uint64_t parent_counter,
    const ActorID &actor_creation_id, const ObjectID &actor_creation_dummy_object_id,
    uint64_t max_actor_reconstructions, const ActorID &actor_id,
    const ActorHandleID &actor_handle_id, uint64_t actor_counter,
    const std::vector<ActorHandleID> &new_actor_handles,
    const std::vector<std::shared_ptr<rpc::TaskArg>> &task_arguments, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const Language &language, const std::vector<std::string> &function_descriptor,
    const std::vector<std::string> &dynamic_worker_options) {
  std::unique_ptr<rpc::TaskSpec> message(new rpc::TaskSpec());

  // Set common fields.
  message->set_language(language);
  for (const auto &fd : function_descriptor) {
    message->add_function_descriptor(fd);
  }
  message->set_job_id(job_id.Binary());
  message->set_task_id(GenerateTaskId(job_id, parent_task_id, parent_counter).Binary());
  message->set_parent_task_id(parent_task_id.Binary());
  message->set_parent_counter(parent_counter);
  for (const auto &arg : task_arguments) {
    *message->add_args() = *arg;
  }
  message->set_num_returns(num_returns);
  message->mutable_required_resources()->insert(required_resources.begin(),
                                                required_resources.end());
  message->mutable_required_placement_resources()->insert(
      required_placement_resources.begin(), required_placement_resources.end());

  if (!actor_creation_id.IsNil()) {
    message->set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    auto creation_spec = message->mutable_actor_creation_task_spec();
    creation_spec->set_actor_id(actor_creation_id.Binary());
    creation_spec->set_max_actor_reconstructions(max_actor_reconstructions);
    for (const auto &option : dynamic_worker_options) {
      creation_spec->add_dynamic_worker_options(option);
    }
  } else if (!actor_id.IsNil()) {
    message->set_type(rpc::TaskType::ACTOR_TASK);
    auto actor_spec = message->mutable_actor_task_spec();
    actor_spec->set_actor_id(actor_id.Binary());
    actor_spec->set_actor_handle_id(actor_handle_id.Binary());
    actor_spec->set_actor_counter(actor_counter);
    actor_spec->set_actor_creation_dummy_object_id(
        actor_creation_dummy_object_id.Binary());
    for (const auto &new_handle : new_actor_handles) {
      actor_spec->add_new_actor_handles(new_handle.Binary());
    }
  } else {
    message->set_type(rpc::TaskType::NORMAL_TASK);
  }

  return new TaskSpecification(std::move(message));
}

void BuildCommonTaskSpec(
    rpc::TaskSpec &message, const Language &language,
    const std::vector<std::string> &function_descriptor, const JobID &job_id,
    const TaskID &parent_task_id, uint64_t parent_counter, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources) {
  // Set common fields.
  message.set_type(rpc::TaskType::NORMAL_TASK);
  message.set_language(language);
  for (const auto &fd : function_descriptor) {
    message.add_function_descriptor(fd);
  }
  message.set_job_id(job_id.Binary());
  message.set_task_id(GenerateTaskId(job_id, parent_task_id, parent_counter).Binary());
  message.set_parent_task_id(parent_task_id.Binary());
  message.set_parent_counter(parent_counter);
  message.set_num_returns(num_returns);
  message.mutable_required_resources()->insert(required_resources.begin(),
                                               required_resources.end());
  message.mutable_required_placement_resources()->insert(
      required_placement_resources.begin(), required_placement_resources.end());
}

void BuildActorCreationTaskSpec(rpc::TaskSpec &message, const ActorID &actor_id,
                                uint64_t max_reconstructions,
                                const std::vector<std::string> &dynamic_worker_options) {
  message.set_type(TaskType::ACTOR_CREATION_TASK);
  auto actor_creation_spec = message.mutable_actor_creation_task_spec();
  actor_creation_spec->set_actor_id(actor_id.Binary());
  actor_creation_spec->set_max_actor_reconstructions(max_reconstructions);
  for (const auto &option : dynamic_worker_options) {
    actor_creation_spec->add_dynamic_worker_options(option);
  }
}

void BuildActorTaskSpec(rpc::TaskSpec &message, const ActorID &actor_id,
                        const ActorHandleID &actor_handle_id,
                        const ObjectID &actor_creation_dummy_object_id,
                        const uint64_t actor_counter,
                        const std::vector<ActorHandleID> &new_handle_ids) {
  message.set_type(TaskType::ACTOR_TASK);
  auto actor_spec = message.mutable_actor_task_spec();
  actor_spec->set_actor_id(actor_id.Binary());
  actor_spec->set_actor_handle_id(actor_handle_id.Binary());
  actor_spec->set_actor_creation_dummy_object_id(actor_creation_dummy_object_id.Binary());
  actor_spec->set_actor_counter(actor_counter);
  for (const auto &id : new_handle_ids) {
    actor_spec->add_new_actor_handles(id.Binary());
  }
}

}  // namespace raylet

}  // namespace ray
