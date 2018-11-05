#include "task_spec.h"

#include "ray/common/common_protocol.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

TaskArgument::~TaskArgument() {}

TaskArgumentByReference::TaskArgumentByReference(const std::vector<ObjectID> &references)
    : references_(references) {}

flatbuffers::Offset<Arg> TaskArgumentByReference::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  return CreateArg(fbb, to_flatbuf(fbb, references_));
}

TaskArgumentByValue::TaskArgumentByValue(const uint8_t *value, size_t length) {
  value_.assign(value, value + length);
}

flatbuffers::Offset<Arg> TaskArgumentByValue::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  auto arg =
      fbb.CreateString(reinterpret_cast<const char *>(value_.data()), value_.size());
  auto empty_ids = fbb.CreateVectorOfStrings({});
  return CreateArg(fbb, empty_ids, arg);
}

void TaskSpecification::AssignSpecification(const uint8_t *spec, size_t spec_size) {
  spec_.assign(spec, spec + spec_size);
  // Initialize required_resources_ and required_placement_resources_
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  required_resources_ = map_from_flatbuf(*message->required_resources());
  required_placement_resources_ =
      map_from_flatbuf(*message->required_placement_resources());
  // If the required_placement_resources field is empty, then the placement
  // resources default to the required resources.
  if (required_placement_resources_.size() == 0) {
    required_placement_resources_ = required_resources_;
  }
}

TaskSpecification::TaskSpecification(const flatbuffers::String &string) {
  AssignSpecification(reinterpret_cast<const uint8_t *>(string.data()), string.size());
}

TaskSpecification::TaskSpecification(const std::string &string) {
  AssignSpecification(reinterpret_cast<const uint8_t *>(string.data()), string.size());
}

TaskSpecification::TaskSpecification(
    const UniqueID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
    const FunctionID &function_id,
    const std::vector<std::shared_ptr<TaskArgument>> &task_arguments, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const Language &language)
    : TaskSpecification(driver_id, parent_task_id, parent_counter, ActorID::nil(),
                        ObjectID::nil(), ActorID::nil(), ActorHandleID::nil(), -1,
                        function_id, task_arguments, num_returns, required_resources,
                        std::unordered_map<std::string, double>(), language) {}

TaskSpecification::TaskSpecification(
    const UniqueID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
    const ActorID &actor_creation_id, const ObjectID &actor_creation_dummy_object_id,
    const ActorID &actor_id, const ActorHandleID &actor_handle_id, int64_t actor_counter,
    const FunctionID &function_id,
    const std::vector<std::shared_ptr<TaskArgument>> &task_arguments, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const Language &language)
    : spec_() {
  flatbuffers::FlatBufferBuilder fbb;

  TaskID task_id = GenerateTaskId(driver_id, parent_task_id, parent_counter);

  // Add argument object IDs.
  std::vector<flatbuffers::Offset<Arg>> arguments;
  for (auto &argument : task_arguments) {
    arguments.push_back(argument->ToFlatbuffer(fbb));
  }

  // Add return object IDs.
  std::vector<flatbuffers::Offset<flatbuffers::String>> returns;
  for (int64_t i = 1; i < num_returns + 1; i++) {
    ObjectID return_id = ComputeReturnId(task_id, i);
    returns.push_back(to_flatbuf(fbb, return_id));
  }

  // Serialize the TaskSpecification.
  auto spec = CreateTaskInfo(
      fbb, to_flatbuf(fbb, driver_id), to_flatbuf(fbb, task_id),
      to_flatbuf(fbb, parent_task_id), parent_counter, to_flatbuf(fbb, actor_creation_id),
      to_flatbuf(fbb, actor_creation_dummy_object_id), to_flatbuf(fbb, actor_id),
      to_flatbuf(fbb, actor_handle_id), actor_counter, false,
      to_flatbuf(fbb, function_id), fbb.CreateVector(arguments),
      fbb.CreateVector(returns), map_to_flatbuf(fbb, required_resources),
      map_to_flatbuf(fbb, required_placement_resources), language);
  fbb.Finish(spec);
  AssignSpecification(fbb.GetBufferPointer(), fbb.GetSize());
}

flatbuffers::Offset<flatbuffers::String> TaskSpecification::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  return fbb.CreateString(reinterpret_cast<const char *>(data()), size());
}

// TODO(atumanov): copy/paste most TaskSpec_* methods from task.h and make them
// methods of this class.
const uint8_t *TaskSpecification::data() const { return spec_.data(); }

size_t TaskSpecification::size() const { return spec_.size(); }

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->task_id());
}
UniqueID TaskSpecification::DriverId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->driver_id());
}
TaskID TaskSpecification::ParentTaskId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->parent_task_id());
}
int64_t TaskSpecification::ParentCounter() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->parent_counter();
}
FunctionID TaskSpecification::FunctionId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->function_id());
}

int64_t TaskSpecification::NumArgs() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->args()->size();
}

int64_t TaskSpecification::NumReturns() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->returns()->size();
}

ObjectID TaskSpecification::ReturnId(int64_t return_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->returns()->Get(return_index));
}

bool TaskSpecification::ArgByRef(int64_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

int TaskSpecification::ArgIdCount(int64_t arg_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  auto ids = message->args()->Get(arg_index)->object_ids();
  return ids->size();
}

ObjectID TaskSpecification::ArgId(int64_t arg_index, int64_t id_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->args()->Get(arg_index)->object_ids()->Get(id_index));
}

const uint8_t *TaskSpecification::ArgVal(int64_t arg_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return reinterpret_cast<const uint8_t *>(
      message->args()->Get(arg_index)->data()->c_str());
}

size_t TaskSpecification::ArgValLength(int64_t arg_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->args()->Get(arg_index)->data()->size();
}

double TaskSpecification::GetRequiredResource(const std::string &resource_name) const {
  RAY_CHECK(required_resources_.empty() == false);
  auto it = required_resources_.find(resource_name);
  RAY_CHECK(it != required_resources_.end());
  return it->second;
}

const ResourceSet TaskSpecification::GetRequiredResources() const {
  RAY_CHECK(required_resources_.empty() == false);
  return ResourceSet(required_resources_);
}

const ResourceSet TaskSpecification::GetRequiredPlacementResources() const {
  RAY_CHECK(required_placement_resources_.empty() == false);
  return ResourceSet(required_placement_resources_);
}

bool TaskSpecification::IsDriverTask() const {
  // Driver tasks are empty tasks that have no function ID set.
  return FunctionId().is_nil();
}

Language TaskSpecification::GetLanguage() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->language();
}

bool TaskSpecification::IsActorCreationTask() const {
  return !ActorCreationId().is_nil();
}

bool TaskSpecification::IsActorTask() const { return !ActorId().is_nil(); }

ActorID TaskSpecification::ActorCreationId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->actor_creation_id());
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->actor_creation_dummy_object_id());
}

ActorID TaskSpecification::ActorId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->actor_id());
}

ActorHandleID TaskSpecification::ActorHandleId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf(*message->actor_handle_id());
}

int64_t TaskSpecification::ActorCounter() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->actor_counter();
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1);
}

}  // namespace raylet

}  // namespace ray
