#include "task_spec.h"

#include <sstream>

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
  return CreateArg(fbb, ids_to_flatbuf(fbb, references_));
}

TaskArgumentByValue::TaskArgumentByValue(const uint8_t *value, size_t length) {
  value_.assign(value, value + length);
}

flatbuffers::Offset<Arg> TaskArgumentByValue::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  auto arg =
      fbb.CreateString(reinterpret_cast<const char *>(value_.data()), value_.size());
  const auto &empty_ids = fbb.CreateString("");
  return CreateArg(fbb, empty_ids, arg);
}

void TaskSpecification::AssignSpecification(const uint8_t *spec, size_t spec_size) {
  spec_.assign(spec, spec + spec_size);
  // Initialize required_resources_ and required_placement_resources_
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  auto required_resources = map_from_flatbuf(*message->required_resources());
  auto required_placement_resources =
      map_from_flatbuf(*message->required_placement_resources());
  // If the required_placement_resources field is empty, then the placement
  // resources default to the required resources.
  if (required_placement_resources.size() == 0) {
    required_placement_resources = required_resources;
  }
  required_resources_ = ResourceSet(required_resources);
  required_placement_resources_ = ResourceSet(required_placement_resources);
}

TaskSpecification::TaskSpecification(const flatbuffers::String &string) {
  AssignSpecification(reinterpret_cast<const uint8_t *>(string.data()), string.size());
}

TaskSpecification::TaskSpecification(const std::string &string) {
  AssignSpecification(reinterpret_cast<const uint8_t *>(string.data()), string.size());
}

TaskSpecification::TaskSpecification(const uint8_t *spec, size_t spec_size) {
  AssignSpecification(spec, spec_size);
}

TaskSpecification::TaskSpecification(
    const DriverID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
    const std::vector<std::shared_ptr<TaskArgument>> &task_arguments, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const Language &language, const std::vector<std::string> &function_descriptor)
    : TaskSpecification(driver_id, parent_task_id, parent_counter, ActorID::nil(),
                        ObjectID::nil(), 0, ActorID::nil(), ActorHandleID::nil(), -1, {},
                        task_arguments, num_returns, required_resources,
                        std::unordered_map<std::string, double>(), language,
                        function_descriptor) {}

TaskSpecification::TaskSpecification(
    const DriverID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
    const ActorID &actor_creation_id, const ObjectID &actor_creation_dummy_object_id,
    const int64_t max_actor_reconstructions, const ActorID &actor_id,
    const ActorHandleID &actor_handle_id, int64_t actor_counter,
    const std::vector<ActorHandleID> &new_actor_handles,
    const std::vector<std::shared_ptr<TaskArgument>> &task_arguments, int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const Language &language, const std::vector<std::string> &function_descriptor)
    : spec_() {
  flatbuffers::FlatBufferBuilder fbb;

  TaskID task_id = GenerateTaskId(driver_id, parent_task_id, parent_counter);

  // Add argument object IDs.
  std::vector<flatbuffers::Offset<Arg>> arguments;
  for (auto &argument : task_arguments) {
    arguments.push_back(argument->ToFlatbuffer(fbb));
  }

  // Generate return ids.
  std::vector<ray::ObjectID> returns;
  for (int64_t i = 1; i < num_returns + 1; ++i) {
    returns.push_back(ComputeReturnId(task_id, i));
  }

  // Serialize the TaskSpecification.
  auto spec = CreateTaskInfo(
      fbb, to_flatbuf(fbb, driver_id), to_flatbuf(fbb, task_id),
      to_flatbuf(fbb, parent_task_id), parent_counter, to_flatbuf(fbb, actor_creation_id),
      to_flatbuf(fbb, actor_creation_dummy_object_id), max_actor_reconstructions,
      to_flatbuf(fbb, actor_id), to_flatbuf(fbb, actor_handle_id), actor_counter,
      ids_to_flatbuf(fbb, new_actor_handles), fbb.CreateVector(arguments),
      ids_to_flatbuf(fbb, returns), map_to_flatbuf(fbb, required_resources),
      map_to_flatbuf(fbb, required_placement_resources), language,
      string_vec_to_flatbuf(fbb, function_descriptor));
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
  return from_flatbuf<TaskID>(*message->task_id());
}
DriverID TaskSpecification::DriverId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf<DriverID>(*message->driver_id());
}
TaskID TaskSpecification::ParentTaskId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf<TaskID>(*message->parent_task_id());
}
int64_t TaskSpecification::ParentCounter() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->parent_counter();
}
std::vector<std::string> TaskSpecification::FunctionDescriptor() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return string_vec_from_flatbuf(*message->function_descriptor());
}

std::string TaskSpecification::FunctionDescriptorString() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  auto list = string_vec_from_flatbuf(*message->function_descriptor());
  std::ostringstream stream;
  // The 4th is the code hash which is binary bits. No need to output it.
  int size = std::min(static_cast<size_t>(3), list.size());
  for (int i = 0; i < size; ++i) {
    if (i != 0) {
      stream << ",";
    }
    stream << list[i];
  }
  return stream.str();
}

int64_t TaskSpecification::NumArgs() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->args()->size();
}

int64_t TaskSpecification::NumReturns() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return (message->returns()->size() / kUniqueIDSize);
}

ObjectID TaskSpecification::ReturnId(int64_t return_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return ids_from_flatbuf<ObjectID>(*message->returns())[return_index];
}

bool TaskSpecification::ArgByRef(int64_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

int TaskSpecification::ArgIdCount(int64_t arg_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  auto ids = message->args()->Get(arg_index)->object_ids();
  return (ids->size() / kUniqueIDSize);
}

ObjectID TaskSpecification::ArgId(int64_t arg_index, int64_t id_index) const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  const auto &object_ids =
      ids_from_flatbuf<ObjectID>(*message->args()->Get(arg_index)->object_ids());
  return object_ids[id_index];
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
  return from_flatbuf<ActorID>(*message->actor_creation_id());
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf<ObjectID>(*message->actor_creation_dummy_object_id());
}

int64_t TaskSpecification::MaxActorReconstructions() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->max_actor_reconstructions();
}

ActorID TaskSpecification::ActorId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf<ActorID>(*message->actor_id());
}

ActorHandleID TaskSpecification::ActorHandleId() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return from_flatbuf<ActorHandleID>(*message->actor_handle_id());
}

int64_t TaskSpecification::ActorCounter() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->actor_counter();
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1);
}

std::vector<ActorHandleID> TaskSpecification::NewActorHandles() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return ids_from_flatbuf<ActorHandleID>(*message->new_actor_handles());
}

}  // namespace raylet

}  // namespace ray
