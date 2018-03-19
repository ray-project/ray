#include "task_spec.h"

#include "common.h"
#include "common_protocol.h"

namespace ray {

TaskArgument::~TaskArgument() {}

TaskArgumentByReference::TaskArgumentByReference(const std::vector<ObjectID> &references)
    : references_(references) {}

flatbuffers::Offset<Arg> TaskArgumentByReference::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  return CreateArg(fbb, to_flatbuf(fbb, references_));
}

const BYTE *TaskArgumentByReference::HashData() const {
  return reinterpret_cast<const BYTE *>(references_.data());
}

size_t TaskArgumentByReference::HashDataLength() const {
  return references_.size() * sizeof(ObjectID);
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

const BYTE *TaskArgumentByValue::HashData() const { return value_.data(); }

size_t TaskArgumentByValue::HashDataLength() const { return value_.size(); }

static const ObjectID task_compute_return_id(TaskID task_id, int64_t return_index) {
  // Here, return_indices need to be >= 0, so we can use negative indices for put.
  RAY_DCHECK(return_index >= 0);
  // TODO(rkn): This line requires object and task IDs to be the same size.
  ObjectID return_id = task_id;
  int64_t *first_bytes = (int64_t *)&return_id;
  // XOR the first bytes of the object ID with the return index.
  // We add one so the first return ID is not the same as the task ID.
  *first_bytes = *first_bytes ^ (return_index + 1);
  return return_id;
}

TaskSpecification::TaskSpecification(const uint8_t *spec, size_t spec_size)
    : spec_(spec, spec + spec_size) {}

TaskSpecification::TaskSpecification(const flatbuffers::String &string)
    : TaskSpecification(reinterpret_cast<const uint8_t *>(string.data()), string.size()) {
}

TaskSpecification::TaskSpecification(
    UniqueID driver_id, TaskID parent_task_id, int64_t parent_counter,
    // UniqueID actor_id,
    // UniqueID actor_handle_id,
    // int64_t actor_counter,
    FunctionID function_id, const std::vector<TaskArgument> &task_arguments,
    int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources) {
  flatbuffers::FlatBufferBuilder fbb;

  // Compute hashes.
  SHA256_CTX ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, (BYTE *)&driver_id, sizeof(driver_id));
  sha256_update(&ctx, (BYTE *)&parent_task_id, sizeof(parent_task_id));
  sha256_update(&ctx, (BYTE *)&parent_counter, sizeof(parent_counter));
  // sha256_update(&ctx, (BYTE *) &actor_id, sizeof(actor_id));
  // sha256_update(&ctx, (BYTE *) &actor_counter, sizeof(actor_counter));
  // sha256_update(&ctx, (BYTE *) &is_actor_checkpoint_method,
  //              sizeof(is_actor_checkpoint_method));
  sha256_update(&ctx, (BYTE *)&function_id, sizeof(function_id));

  // Serialize and hash the arguments.
  std::vector<flatbuffers::Offset<Arg>> arguments;
  for (auto &argument : task_arguments) {
    arguments.push_back(argument.ToFlatbuffer(fbb));
    sha256_update(&ctx, (BYTE *)argument.HashData(), argument.HashDataLength());
  }

  // Compute the final task ID from the hash.
  BYTE buff[DIGEST_SIZE];
  sha256_final(&ctx, buff);
  TaskID task_id;
  RAY_DCHECK(sizeof(task_id) <= DIGEST_SIZE);
  memcpy(&task_id, buff, sizeof(task_id));

  // Add return object IDs.
  std::vector<flatbuffers::Offset<flatbuffers::String>> returns;
  for (int64_t i = 0; i < num_returns; i++) {
    ObjectID return_id = task_compute_return_id(task_id, i);
    returns.push_back(to_flatbuf(fbb, return_id));
  }

  // Serialize the TaskSpecification.
  auto spec = CreateTaskInfo(
      fbb, to_flatbuf(fbb, driver_id), to_flatbuf(fbb, task_id),
      to_flatbuf(fbb, parent_task_id), parent_counter, to_flatbuf(fbb, ActorID::nil()),
      to_flatbuf(fbb, ActorID::nil()), to_flatbuf(fbb, WorkerID::nil()),
      to_flatbuf(fbb, ActorHandleID::nil()), 0, false, to_flatbuf(fbb, function_id),
      fbb.CreateVector(arguments), fbb.CreateVector(returns),
      map_to_flatbuf(fbb, required_resources));
  fbb.Finish(spec);
  TaskSpecification(fbb.GetBufferPointer(), fbb.GetSize());
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
  throw std::runtime_error("Method not implemented");
}
TaskID TaskSpecification::ParentTaskId() const {
  throw std::runtime_error("Method not implemented");
}
int64_t TaskSpecification::ParentCounter() const {
  throw std::runtime_error("Method not implemented");
}
FunctionID TaskSpecification::FunctionId() const {
  throw std::runtime_error("Method not implemented");
}

int64_t TaskSpecification::NumArgs() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  return message->args()->size();
}

int64_t TaskSpecification::NumReturns() const {
  throw std::runtime_error("Method not implemented");
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
  throw std::runtime_error("Method not implemented");
}
size_t TaskSpecification::ArgValLength(int64_t arg_index) const {
  throw std::runtime_error("Method not implemented");
}
double TaskSpecification::GetRequiredResource(const std::string &resource_name) const {
  throw std::runtime_error("Method not implemented");
}
const ResourceSet TaskSpecification::GetRequiredResources() const {
  auto message = flatbuffers::GetRoot<TaskInfo>(spec_.data());
  auto required_resources = map_from_flatbuf(*message->required_resources());
  return ResourceSet(required_resources);
}

}  // namespace ray
