
#include <sstream>

#include "ray/raylet/task_spec.h"
#include "ray/rpc/util.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

using rpc::MapFromProtobuf;
using rpc::VectorFromProtobuf;

void TaskSpecification::ComputeResources() {
  auto required_resources = MapFromProtobuf(message_.required_resources());
  auto required_placement_resources =
      MapFromProtobuf(message_.required_placement_resources());
  if (required_placement_resources.empty()) {
    required_placement_resources = required_resources;
  }
  required_resources_ = ResourceSet(required_resources);
  required_placement_resources_ = ResourceSet(required_placement_resources);
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  return TaskID::FromBinary(message_.task_id());
}

JobID TaskSpecification::JobId() const { return JobID::FromBinary(message_.job_id()); }

TaskID TaskSpecification::ParentTaskId() const {
  return TaskID::FromBinary(message_.parent_task_id());
}

size_t TaskSpecification::ParentCounter() const { return message_.parent_counter(); }

std::vector<std::string> TaskSpecification::FunctionDescriptor() const {
  return VectorFromProtobuf(message_.function_descriptor());
}

std::string TaskSpecification::FunctionDescriptorString() const {
  auto list = VectorFromProtobuf(message_.function_descriptor());
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

size_t TaskSpecification::NumArgs() const { return message_.args_size(); }

size_t TaskSpecification::NumReturns() const { return message_.num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::ForTaskReturn(TaskId(), return_index + 1);
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

size_t TaskSpecification::ArgIdCount(size_t arg_index) const {
  return message_.args(arg_index).object_ids_size();
}

ObjectID TaskSpecification::ArgId(size_t arg_index, size_t id_index) const {
  return ObjectID::FromBinary(message_.args(arg_index).object_ids(id_index));
}

const uint8_t *TaskSpecification::ArgVal(size_t arg_index) const {
  return reinterpret_cast<const uint8_t *>(message_.args(arg_index).data().data());
}

size_t TaskSpecification::ArgValLength(size_t arg_index) const {
  return message_.args(arg_index).data().size();
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

rpc::Language TaskSpecification::GetLanguage() const { return message_.language(); }

bool TaskSpecification::IsActorCreationTask() const {
  return message_.type() == rpc::TaskType::ACTOR_CREATION_TASK;
}

bool TaskSpecification::IsActorTask() const {
  return message_.type() == rpc::TaskType::ACTOR_TASK;
}

ActorID TaskSpecification::ActorCreationId() const {
  // TODO(hchen) Add a check to make sure this function can only be called if
  //   task is an actor creation task.
  if (!IsActorCreationTask()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_.actor_creation_task_spec().actor_id());
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  if (!IsActorTask()) {
    return ObjectID::Nil();
  }
  return ObjectID::FromBinary(
      message_.actor_task_spec().actor_creation_dummy_object_id());
}

uint64_t TaskSpecification::MaxActorReconstructions() const {
  if (!IsActorCreationTask()) {
    return 0;
  }
  return message_.actor_creation_task_spec().max_actor_reconstructions();
}

ActorID TaskSpecification::ActorId() const {
  if (!IsActorTask()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_.actor_task_spec().actor_id());
}

ActorHandleID TaskSpecification::ActorHandleId() const {
  if (!IsActorTask()) {
    return ActorHandleID::Nil();
  }
  return ActorHandleID::FromBinary(message_.actor_task_spec().actor_handle_id());
}

uint64_t TaskSpecification::ActorCounter() const {
  if (!IsActorTask()) {
    return 0;
  }
  return message_.actor_task_spec().actor_counter();
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
      message_.actor_task_spec().new_actor_handles());
}

std::vector<std::string> TaskSpecification::DynamicWorkerOptions() const {
  return rpc::VectorFromProtobuf(
      message_.actor_creation_task_spec().dynamic_worker_options());
}

}  // namespace raylet

}  // namespace ray
