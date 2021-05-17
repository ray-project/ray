#include "ray/common/task/task_spec.h"

#include <sstream>

#include "ray/util/logging.h"

namespace ray {

absl::Mutex TaskSpecification::mutex_;
std::unordered_map<SchedulingClassDescriptor, SchedulingClass>
    TaskSpecification::sched_cls_to_id_;
std::unordered_map<SchedulingClass, SchedulingClassDescriptor>
    TaskSpecification::sched_id_to_cls_;
int TaskSpecification::next_sched_id_;

SchedulingClassDescriptor &TaskSpecification::GetSchedulingClassDescriptor(
    SchedulingClass id) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_id_to_cls_.find(id);
  RAY_CHECK(it != sched_id_to_cls_.end()) << "invalid id: " << id;
  return it->second;
}

SchedulingClass TaskSpecification::GetSchedulingClass(const ResourceSet &sched_cls) {
  SchedulingClass sched_cls_id;
  absl::MutexLock lock(&mutex_);
  auto it = sched_cls_to_id_.find(sched_cls);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;
    // TODO(ekl) we might want to try cleaning up task types in these cases
    if (sched_cls_id > 100) {
      RAY_LOG(WARNING) << "More than " << sched_cls_id
                       << " types of tasks seen, this may reduce performance.";
    } else if (sched_cls_id > 1000) {
      RAY_LOG(ERROR) << "More than " << sched_cls_id
                     << " types of tasks seen, this may reduce performance.";
    }
    sched_cls_to_id_[sched_cls] = sched_cls_id;
    sched_id_to_cls_[sched_cls_id] = sched_cls;
  } else {
    sched_cls_id = it->second;
  }
  return sched_cls_id;
}

const BundleID TaskSpecification::PlacementGroupBundleId() const {
  return std::make_pair(PlacementGroupID::FromBinary(message_->placement_group_id()),
                        message_->placement_group_bundle_index());
}

bool TaskSpecification::PlacementGroupCaptureChildTasks() const {
  return message_->placement_group_capture_child_tasks();
}

void TaskSpecification::ComputeResources() {
  auto &required_resources = message_->required_resources();

  if (required_resources.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    required_resources_ = ResourceSet::Nil();
  } else {
    required_resources_.reset(new ResourceSet(MapFromProtobuf(required_resources)));
  }

  auto &required_placement_resources = message_->required_placement_resources().empty()
                                           ? required_resources
                                           : message_->required_placement_resources();

  if (required_placement_resources.empty()) {
    required_placement_resources_ = ResourceSet::Nil();
  } else {
    required_placement_resources_.reset(
        new ResourceSet(MapFromProtobuf(required_placement_resources)));
  }

  if (!IsActorTask()) {
    // There is no need to compute `SchedulingClass` for actor tasks since
    // the actor tasks need not be scheduled.

    // Map the scheduling class descriptor to an integer for performance.
    auto sched_cls = GetRequiredResources();
    sched_cls_id_ = GetSchedulingClass(sched_cls);
  }
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  if (message_->task_id().empty() /* e.g., empty proto default */) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->task_id());
}

JobID TaskSpecification::JobId() const {
  if (message_->job_id().empty() /* e.g., empty proto default */) {
    return JobID::Nil();
  }
  return JobID::FromBinary(message_->job_id());
}

TaskID TaskSpecification::ParentTaskId() const {
  if (message_->parent_task_id().empty() /* e.g., empty proto default */) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->parent_task_id());
}

size_t TaskSpecification::ParentCounter() const { return message_->parent_counter(); }

ray::FunctionDescriptor TaskSpecification::FunctionDescriptor() const {
  return ray::FunctionDescriptorBuilder::FromProto(message_->function_descriptor());
}

std::string TaskSpecification::SerializedRuntimeEnv() const {
  return message_->serialized_runtime_env();
}
const SchedulingClass TaskSpecification::GetSchedulingClass() const {
  RAY_CHECK(sched_cls_id_ > 0);
  return sched_cls_id_;
}

size_t TaskSpecification::NumArgs() const { return message_->args_size(); }

size_t TaskSpecification::NumReturns() const { return message_->num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::FromIndex(TaskId(), return_index + 1);
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return message_->args(arg_index).has_object_ref();
}

ObjectID TaskSpecification::ArgId(size_t arg_index) const {
  return ObjectID::FromBinary(message_->args(arg_index).object_ref().object_id());
}

rpc::ObjectReference TaskSpecification::ArgRef(size_t arg_index) const {
  RAY_CHECK(ArgByRef(arg_index));
  return message_->args(arg_index).object_ref();
}

const uint8_t *TaskSpecification::ArgData(size_t arg_index) const {
  return reinterpret_cast<const uint8_t *>(message_->args(arg_index).data().data());
}

size_t TaskSpecification::ArgDataSize(size_t arg_index) const {
  return message_->args(arg_index).data().size();
}

const uint8_t *TaskSpecification::ArgMetadata(size_t arg_index) const {
  return reinterpret_cast<const uint8_t *>(message_->args(arg_index).metadata().data());
}

size_t TaskSpecification::ArgMetadataSize(size_t arg_index) const {
  return message_->args(arg_index).metadata().size();
}

const std::vector<ObjectID> TaskSpecification::ArgInlinedIds(size_t arg_index) const {
  return IdVectorFromProtobuf<ObjectID>(message_->args(arg_index).nested_inlined_ids());
}

const ResourceSet &TaskSpecification::GetRequiredResources() const {
  return *required_resources_;
}

std::vector<ObjectID> TaskSpecification::GetDependencyIds() const {
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    if (ArgByRef(i)) {
      dependencies.push_back(ArgId(i));
    }
  }
  if (IsActorTask()) {
    dependencies.push_back(PreviousActorTaskDummyObjectId());
  }
  return dependencies;
}

std::vector<rpc::ObjectReference> TaskSpecification::GetDependencies(
    bool add_dummy_dependency) const {
  std::vector<rpc::ObjectReference> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    if (ArgByRef(i)) {
      dependencies.push_back(message_->args(i).object_ref());
    }
  }
  if (add_dummy_dependency && IsActorTask()) {
    const auto &dummy_ref =
        GetReferenceForActorDummyObject(PreviousActorTaskDummyObjectId());
    dependencies.push_back(dummy_ref);
  }
  return dependencies;
}

const ResourceSet &TaskSpecification::GetRequiredPlacementResources() const {
  return *required_placement_resources_;
}

std::string TaskSpecification::GetDebuggerBreakpoint() const {
  return message_->debugger_breakpoint();
}

std::unordered_map<std::string, std::string>
TaskSpecification::OverrideEnvironmentVariables() const {
  return MapFromProtobuf(message_->override_environment_variables());
}

bool TaskSpecification::IsDriverTask() const {
  return message_->type() == TaskType::DRIVER_TASK;
}

const std::string TaskSpecification::GetName() const { return message_->name(); }

Language TaskSpecification::GetLanguage() const { return message_->language(); }

bool TaskSpecification::IsNormalTask() const {
  return message_->type() == TaskType::NORMAL_TASK;
}

bool TaskSpecification::IsActorCreationTask() const {
  return message_->type() == TaskType::ACTOR_CREATION_TASK;
}

bool TaskSpecification::IsActorTask() const {
  return message_->type() == TaskType::ACTOR_TASK;
}

// === Below are getter methods specific to actor creation tasks.

ActorID TaskSpecification::ActorCreationId() const {
  RAY_CHECK(IsActorCreationTask());
  return ActorID::FromBinary(message_->actor_creation_task_spec().actor_id());
}

int64_t TaskSpecification::MaxActorRestarts() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().max_actor_restarts();
}

std::vector<std::string> TaskSpecification::DynamicWorkerOptions() const {
  RAY_CHECK(IsActorCreationTask());
  return VectorFromProtobuf(
      message_->actor_creation_task_spec().dynamic_worker_options());
}

TaskID TaskSpecification::CallerId() const {
  return TaskID::FromBinary(message_->caller_id());
}

const rpc::Address &TaskSpecification::CallerAddress() const {
  return message_->caller_address();
}

WorkerID TaskSpecification::CallerWorkerId() const {
  return WorkerID::FromBinary(message_->caller_address().worker_id());
}

// === Below are getter methods specific to actor tasks.

ActorID TaskSpecification::ActorId() const {
  RAY_CHECK(IsActorTask());
  return ActorID::FromBinary(message_->actor_task_spec().actor_id());
}

uint64_t TaskSpecification::ActorCounter() const {
  RAY_CHECK(IsActorTask());
  return message_->actor_task_spec().actor_counter();
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  return ObjectID::FromBinary(
      message_->actor_task_spec().actor_creation_dummy_object_id());
}

ObjectID TaskSpecification::PreviousActorTaskDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  return ObjectID::FromBinary(
      message_->actor_task_spec().previous_actor_task_dummy_object_id());
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1);
}

int TaskSpecification::MaxActorConcurrency() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().max_concurrency();
}

bool TaskSpecification::IsAsyncioActor() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().is_asyncio();
}

bool TaskSpecification::IsDetachedActor() const {
  return IsActorCreationTask() && message_->actor_creation_task_spec().is_detached();
}

std::string TaskSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "Type=" << TaskType_Name(message_->type())
         << ", Language=" << Language_Name(message_->language());

  if (required_resources_ != nullptr) {
    stream << ", Resources: {";

    // Print resource description.
    for (auto entry : GetRequiredResources().GetResourceMap()) {
      stream << entry.first << ": " << entry.second << ", ";
    }
    stream << "}";
  }

  stream << ", function_descriptor=";

  // Print function descriptor.
  stream << FunctionDescriptor()->ToString();

  stream << ", task_id=" << TaskId() << ", task_name=" << GetName()
         << ", job_id=" << JobId() << ", num_args=" << NumArgs()
         << ", num_returns=" << NumReturns();

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    stream << ", actor_creation_task_spec={actor_id=" << ActorCreationId()
           << ", max_restarts=" << MaxActorRestarts()
           << ", max_concurrency=" << MaxActorConcurrency()
           << ", is_asyncio_actor=" << IsAsyncioActor()
           << ", is_detached=" << IsDetachedActor() << "}";
  } else if (IsActorTask()) {
    // Print actor task spec.
    stream << ", actor_task_spec={actor_id=" << ActorId()
           << ", actor_caller_id=" << CallerId() << ", actor_counter=" << ActorCounter()
           << "}";
  }

  return stream.str();
}

std::string TaskSpecification::CallSiteString() const {
  std::ostringstream stream;
  auto desc = FunctionDescriptor();
  if (IsActorCreationTask()) {
    stream << "(deserialize actor creation task arg) ";
  } else if (IsActorTask()) {
    stream << "(deserialize actor task arg) ";
  } else {
    stream << "(deserialize task arg) ";
  }
  stream << FunctionDescriptor()->CallSiteString();
  return stream.str();
}

}  // namespace ray
