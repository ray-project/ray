// Copyright 2019-2021 The Ray Authors.
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

#include "ray/common/task/task_spec.h"

#include <boost/functional/hash.hpp>
#include <sstream>

#include "ray/common/ray_config.h"
#include "ray/common/runtime_env_common.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"

namespace ray {

absl::Mutex TaskSpecification::mutex_;
absl::flat_hash_map<SchedulingClassDescriptor, SchedulingClass>
    TaskSpecification::sched_cls_to_id_;
absl::flat_hash_map<SchedulingClass, SchedulingClassDescriptor>
    TaskSpecification::sched_id_to_cls_;
int TaskSpecification::next_sched_id_;

SchedulingClassDescriptor &TaskSpecification::GetSchedulingClassDescriptor(
    SchedulingClass id) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_id_to_cls_.find(id);
  RAY_CHECK(it != sched_id_to_cls_.end()) << "invalid id: " << id;
  return it->second;
}

SchedulingClass TaskSpecification::GetSchedulingClass(
    const SchedulingClassDescriptor &sched_cls) {
  SchedulingClass sched_cls_id;
  absl::MutexLock lock(&mutex_);
  auto it = sched_cls_to_id_.find(sched_cls);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;
    // TODO(ekl) we might want to try cleaning up task types in these cases
    if (sched_cls_id > 100) {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "More than " << sched_cls_id
          << " types of tasks seen, this may reduce performance.";
    }
    sched_cls_to_id_[sched_cls] = sched_cls_id;
    sched_id_to_cls_.emplace(sched_cls_id, sched_cls);
  } else {
    sched_cls_id = it->second;
  }
  return sched_cls_id;
}

const BundleID TaskSpecification::PlacementGroupBundleId() const {
  if (message_->scheduling_strategy().scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::
          kPlacementGroupSchedulingStrategy) {
    return std::make_pair(
        PlacementGroupID::FromBinary(message_->scheduling_strategy()
                                         .placement_group_scheduling_strategy()
                                         .placement_group_id()),
        message_->scheduling_strategy()
            .placement_group_scheduling_strategy()
            .placement_group_bundle_index());
  } else {
    return std::make_pair(PlacementGroupID::Nil(), -1);
  }
}

bool TaskSpecification::PlacementGroupCaptureChildTasks() const {
  if (message_->scheduling_strategy().scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::
          kPlacementGroupSchedulingStrategy) {
    return message_->scheduling_strategy()
        .placement_group_scheduling_strategy()
        .placement_group_capture_child_tasks();
  } else {
    return false;
  }
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
    const bool is_actor_creation_task = IsActorCreationTask();
    const bool should_report_placement_resources =
        RayConfig::instance().report_actor_placement_resources();
    const auto &resource_set =
        (is_actor_creation_task && should_report_placement_resources)
            ? GetRequiredPlacementResources()
            : GetRequiredResources();
    const auto &function_descriptor = FunctionDescriptor();
    auto depth = GetDepth();
    auto sched_cls_desc = SchedulingClassDescriptor(
        resource_set, function_descriptor, depth, GetSchedulingStrategy());
    // Map the scheduling class descriptor to an integer for performance.
    sched_cls_id_ = GetSchedulingClass(sched_cls_desc);
  }

  runtime_env_hash_ = CalculateRuntimeEnvHash(SerializedRuntimeEnv());
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  if (message_->task_id().empty() /* e.g., empty proto default */) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->task_id());
}

const std::string TaskSpecification::GetSerializedActorHandle() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().serialized_actor_handle();
}

JobID TaskSpecification::JobId() const {
  if (message_->job_id().empty() /* e.g., empty proto default */) {
    return JobID::Nil();
  }
  return JobID::FromBinary(message_->job_id());
}

const rpc::JobConfig &TaskSpecification::JobConfig() const {
  return message_->job_config();
}

TaskID TaskSpecification::ParentTaskId() const {
  if (message_->parent_task_id().empty() /* e.g., empty proto default */) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->parent_task_id());
}

ActorID TaskSpecification::RootDetachedActorId() const {
  if (message_->root_detached_actor_id().empty() /* e.g., empty proto default */) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_->root_detached_actor_id());
}

TaskID TaskSpecification::SubmitterTaskId() const {
  if (message_->submitter_task_id().empty() /* e.g., empty proto default */) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->submitter_task_id());
}

size_t TaskSpecification::ParentCounter() const { return message_->parent_counter(); }

ray::FunctionDescriptor TaskSpecification::FunctionDescriptor() const {
  return ray::FunctionDescriptorBuilder::FromProto(message_->function_descriptor());
}

rpc::RuntimeEnvInfo TaskSpecification::RuntimeEnvInfo() const {
  return message_->runtime_env_info();
}

std::string TaskSpecification::SerializedRuntimeEnv() const {
  return message_->runtime_env_info().serialized_runtime_env();
}

rpc::RuntimeEnvConfig TaskSpecification::RuntimeEnvConfig() const {
  return message_->runtime_env_info().runtime_env_config();
}

bool TaskSpecification::HasRuntimeEnv() const {
  return !IsRuntimeEnvEmpty(SerializedRuntimeEnv());
}

uint64_t TaskSpecification::AttemptNumber() const { return message_->attempt_number(); }

bool TaskSpecification::IsRetry() const { return AttemptNumber() > 0; }

int32_t TaskSpecification::MaxRetries() const { return message_->max_retries(); }

int TaskSpecification::GetRuntimeEnvHash() const { return runtime_env_hash_; }

const SchedulingClass TaskSpecification::GetSchedulingClass() const {
  if (!IsActorTask()) {
    // Actor task doesn't have scheudling id, so we don't need to check this.
    RAY_CHECK(sched_cls_id_ > 0);
  }
  return sched_cls_id_;
}

size_t TaskSpecification::NumArgs() const { return message_->args_size(); }

size_t TaskSpecification::NumReturns() const { return message_->num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::FromIndex(TaskId(), return_index + 1);
}

size_t TaskSpecification::NumStreamingGeneratorReturns() const {
  return message_->num_streaming_generator_returns();
}

ObjectID TaskSpecification::StreamingGeneratorReturnId(size_t generator_index) const {
  // Streaming generator task has only 1 return ID.
  RAY_CHECK_EQ(NumReturns(), 1UL);
  RAY_CHECK_LT(generator_index, RayConfig::instance().max_num_generator_returns());
  // index 1 is reserved for the first task return from a generator task itself.
  return ObjectID::FromIndex(TaskId(), 2 + generator_index);
}

void TaskSpecification::SetNumStreamingGeneratorReturns(
    uint64_t num_streaming_generator_returns) {
  message_->set_num_streaming_generator_returns(num_streaming_generator_returns);
}

bool TaskSpecification::ReturnsDynamic() const { return message_->returns_dynamic(); }

// TODO(sang): Merge this with ReturnsDynamic once migrating to the
// streaming generator.
bool TaskSpecification::IsStreamingGenerator() const {
  return message_->streaming_generator();
}

int64_t TaskSpecification::GeneratorBackpressureNumObjects() const {
  auto result = message_->generator_backpressure_num_objects();
  // generator_backpressure_num_objects == 0 makes no sense.
  // it means it pauses the generator even before it starts.
  RAY_CHECK_NE(result, 0);
  return result;
}

std::vector<ObjectID> TaskSpecification::DynamicReturnIds() const {
  RAY_CHECK(message_->returns_dynamic());
  std::vector<ObjectID> dynamic_return_ids;
  for (const auto &dynamic_return_id : message_->dynamic_return_ids()) {
    dynamic_return_ids.push_back(ObjectID::FromBinary(dynamic_return_id));
  }
  return dynamic_return_ids;
}

void TaskSpecification::AddDynamicReturnId(const ObjectID &dynamic_return_id) {
  message_->add_dynamic_return_ids(dynamic_return_id.Binary());
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return message_->args(arg_index).has_object_ref();
}

ObjectID TaskSpecification::ArgId(size_t arg_index) const {
  return ObjectID::FromBinary(message_->args(arg_index).object_ref().object_id());
}

const rpc::ObjectReference &TaskSpecification::ArgRef(size_t arg_index) const {
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

const std::vector<rpc::ObjectReference> TaskSpecification::ArgInlinedRefs(
    size_t arg_index) const {
  return VectorFromProtobuf<rpc::ObjectReference>(
      message_->args(arg_index).nested_inlined_refs());
}

const ResourceSet &TaskSpecification::GetRequiredResources() const {
  return *required_resources_;
}

const rpc::SchedulingStrategy &TaskSpecification::GetSchedulingStrategy() const {
  return message_->scheduling_strategy();
}

bool TaskSpecification::IsNodeAffinitySchedulingStrategy() const {
  return GetSchedulingStrategy().scheduling_strategy_case() ==
         rpc::SchedulingStrategy::SchedulingStrategyCase::kNodeAffinitySchedulingStrategy;
}

NodeID TaskSpecification::GetNodeAffinitySchedulingStrategyNodeId() const {
  RAY_CHECK(IsNodeAffinitySchedulingStrategy());
  return NodeID::FromBinary(
      GetSchedulingStrategy().node_affinity_scheduling_strategy().node_id());
}

bool TaskSpecification::GetNodeAffinitySchedulingStrategySoft() const {
  RAY_CHECK(IsNodeAffinitySchedulingStrategy());
  return GetSchedulingStrategy().node_affinity_scheduling_strategy().soft();
}

std::vector<ObjectID> TaskSpecification::GetDependencyIds() const {
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    if (ArgByRef(i)) {
      dependencies.push_back(ArgId(i));
    }
  }
  return dependencies;
}

std::vector<rpc::ObjectReference> TaskSpecification::GetDependencies() const {
  std::vector<rpc::ObjectReference> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    if (ArgByRef(i)) {
      dependencies.push_back(message_->args(i).object_ref());
    }
  }
  return dependencies;
}

const ResourceSet &TaskSpecification::GetRequiredPlacementResources() const {
  return *required_placement_resources_;
}

std::string TaskSpecification::GetDebuggerBreakpoint() const {
  return message_->debugger_breakpoint();
}

int64_t TaskSpecification::GetDepth() const { return message_->depth(); }

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

bool TaskSpecification::IsSpreadSchedulingStrategy() const {
  return message_->scheduling_strategy().scheduling_strategy_case() ==
         rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy;
}

const std::string TaskSpecification::GetSerializedRetryExceptionAllowlist() const {
  return message_->serialized_retry_exception_allowlist();
}

bool TaskSpecification::EnableTaskEvents() const {
  return message_->enable_task_events();
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

std::vector<std::string> TaskSpecification::DynamicWorkerOptionsOrEmpty() const {
  if (!IsActorCreationTask()) {
    return {};
  }
  return VectorFromProtobuf(
      message_->actor_creation_task_spec().dynamic_worker_options());
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

bool TaskSpecification::ShouldRetryExceptions() const {
  return message_->retry_exceptions();
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

int TaskSpecification::MaxActorConcurrency() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().max_concurrency();
}

std::string TaskSpecification::ConcurrencyGroupName() const {
  RAY_CHECK(IsActorTask());
  return message_->concurrency_group_name();
}

bool TaskSpecification::ExecuteOutOfOrder() const {
  return IsActorCreationTask() &&
         message_->actor_creation_task_spec().execute_out_of_order();
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
         << ", num_returns=" << NumReturns() << ", max_retries=" << MaxRetries()
         << ", depth=" << GetDepth() << ", attempt_number=" << AttemptNumber();

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
           << ", retry_exceptions=" << ShouldRetryExceptions() << "}";
  }

  // Print non-sensitive runtime env info.
  if (HasRuntimeEnv()) {
    const auto &runtime_env_info = RuntimeEnvInfo();
    stream << ", runtime_env_hash=" << GetRuntimeEnvHash();
    if (runtime_env_info.has_runtime_env_config()) {
      stream << ", eager_install="
             << runtime_env_info.runtime_env_config().eager_install();
      stream << ", setup_timeout_seconds="
             << runtime_env_info.runtime_env_config().setup_timeout_seconds();
    }
  }

  return stream.str();
}

std::string TaskSpecification::RuntimeEnvDebugString() const {
  std::ostringstream stream;
  if (HasRuntimeEnv()) {
    const auto &runtime_env_info = RuntimeEnvInfo();
    stream << "serialized_runtime_env=" << SerializedRuntimeEnv();
    const auto &uris = runtime_env_info.uris();
    if (!uris.working_dir_uri().empty() || uris.py_modules_uris().size() > 0) {
      stream << ", runtime_env_uris=";
      if (!uris.working_dir_uri().empty()) {
        stream << uris.working_dir_uri() << ":";
      }
      for (const auto &uri : uris.py_modules_uris()) {
        stream << uri << ":";
      }
      // Erase the last ":"
      stream.seekp(-1, std::ios_base::end);
    }
    stream << ", runtime_env_hash=" << GetRuntimeEnvHash();
    if (runtime_env_info.has_runtime_env_config()) {
      stream << ", eager_install="
             << runtime_env_info.runtime_env_config().eager_install();
      stream << ", setup_timeout_seconds="
             << runtime_env_info.runtime_env_config().setup_timeout_seconds();
    }
  }
  return stream.str();
}

bool TaskSpecification::IsRetriable() const {
  if (IsActorTask()) {
    return false;
  }
  if (IsActorCreationTask() && MaxActorRestarts() == 0) {
    return false;
  }
  if (IsNormalTask() && MaxRetries() == 0) {
    return false;
  }
  return true;
}

void TaskSpecification::EmitTaskMetrics() const {
  double duration_s = (GetMessage().lease_grant_timestamp_ms() -
                       GetMessage().dependency_resolution_timestamp_ms()) /
                      1000;

  if (IsActorCreationTask()) {
    stats::STATS_scheduler_placement_time_s.Record(duration_s,
                                                   {{"WorkloadType", "Actor"}});
  } else {
    stats::STATS_scheduler_placement_time_s.Record(duration_s,
                                                   {{"WorkloadType", "Task"}});
  }
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

int CalculateRuntimeEnvHash(const std::string &serialized_runtime_env) {
  if (IsRuntimeEnvEmpty(serialized_runtime_env)) {
    // It's useful to have the same predetermined value for both unspecified and empty
    // runtime envs.
    return 0;
  }
  size_t hash = std::hash<std::string>()(serialized_runtime_env);
  return static_cast<int>(hash);
}

std::vector<ConcurrencyGroup> TaskSpecification::ConcurrencyGroups() const {
  RAY_CHECK(IsActorCreationTask());
  std::vector<ConcurrencyGroup> concurrency_groups;
  auto &actor_creation_task_spec = message_->actor_creation_task_spec();
  const auto size = actor_creation_task_spec.concurrency_groups().size();

  for (auto i = 0; i < size; ++i) {
    auto &curr_group_message = actor_creation_task_spec.concurrency_groups(i);
    std::vector<ray::FunctionDescriptor> function_descriptors;
    const auto func_descriptors_size = curr_group_message.function_descriptors_size();
    function_descriptors.reserve(func_descriptors_size);
    for (auto j = 0; j < func_descriptors_size; ++j) {
      function_descriptors.push_back(FunctionDescriptorBuilder::FromProto(
          curr_group_message.function_descriptors(j)));
    }

    concurrency_groups.push_back(
        {std::string{curr_group_message.name()},
         static_cast<uint32_t>(curr_group_message.max_concurrency()),
         function_descriptors});
  }

  return concurrency_groups;
}

}  // namespace ray
