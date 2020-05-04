#include <sstream>

#include "ray/common/task/task_spec.h"
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

void TaskSpecification::ComputeResources() {
  auto required_resources = MapFromProtobuf(message_->required_resources());
  auto required_placement_resources =
      MapFromProtobuf(message_->required_placement_resources());
  if (required_placement_resources.empty()) {
    required_placement_resources = required_resources;
  }

  if (required_resources.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    required_resources_ = ResourceSet::Nil();
  } else {
    required_resources_.reset(new ResourceSet(required_resources));
  }

  if (required_placement_resources.empty()) {
    required_placement_resources_ = ResourceSet::Nil();
  } else {
    required_placement_resources_.reset(new ResourceSet(required_placement_resources));
  }

  if (!IsActorTask()) {
    // There is no need to compute `SchedulingClass` for actor tasks since
    // the actor tasks need not be scheduled.

    // Map the scheduling class descriptor to an integer for performance.
    auto sched_cls = std::make_pair(GetRequiredResources(), FunctionDescriptor());
    absl::MutexLock lock(&mutex_);
    auto it = sched_cls_to_id_.find(sched_cls);
    if (it == sched_cls_to_id_.end()) {
      sched_cls_id_ = ++next_sched_id_;
      // TODO(ekl) we might want to try cleaning up task types in these cases
      if (sched_cls_id_ > 100) {
        RAY_LOG(WARNING) << "More than " << sched_cls_id_
                         << " types of tasks seen, this may reduce performance.";
      } else if (sched_cls_id_ > 1000) {
        RAY_LOG(ERROR) << "More than " << sched_cls_id_
                       << " types of tasks seen, this may reduce performance.";
      }
      sched_cls_to_id_[sched_cls] = sched_cls_id_;
      sched_id_to_cls_[sched_cls_id_] = sched_cls;
    } else {
      sched_cls_id_ = it->second;
    }
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

const SchedulingClass TaskSpecification::GetSchedulingClass() const {
  RAY_CHECK(sched_cls_id_ > 0);
  return sched_cls_id_;
}

size_t TaskSpecification::NumArgs() const { return message_->args_size(); }

size_t TaskSpecification::NumReturns() const { return message_->num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index,
                                     TaskTransportType transport_type) const {
  return ObjectID::ForTaskReturn(TaskId(), return_index + 1,
                                 static_cast<uint8_t>(transport_type));
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

std::vector<ObjectID> TaskSpecification::GetDependencies() const {
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    int count = ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(ArgId(i, j));
    }
  }
  if (IsActorTask()) {
    dependencies.push_back(PreviousActorTaskDummyObjectId());
  }
  return dependencies;
}

const ResourceSet &TaskSpecification::GetRequiredPlacementResources() const {
  return *required_placement_resources_;
}

bool TaskSpecification::IsDriverTask() const {
  return message_->type() == TaskType::DRIVER_TASK;
}

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

uint64_t TaskSpecification::MaxActorReconstructions() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().max_actor_reconstructions();
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
  return ReturnId(NumReturns() - 1, TaskTransportType::RAYLET);
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
         << ", Language=" << Language_Name(message_->language())
         << ", function_descriptor=";

  // Print function descriptor.
  stream << FunctionDescriptor()->ToString();

  stream << ", task_id=" << TaskId() << ", job_id=" << JobId()
         << ", num_args=" << NumArgs() << ", num_returns=" << NumReturns();

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    stream << ", actor_creation_task_spec={actor_id=" << ActorCreationId()
           << ", max_reconstructions=" << MaxActorReconstructions()
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
