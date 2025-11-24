// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/lease/lease_spec.h"

#include "ray/common/function_descriptor.h"
#include "ray/common/runtime_env_common.h"

namespace ray {

using SchedulingClass = int;

LeaseSpecification::LeaseSpecification(const rpc::TaskSpec &task_spec)
    : MessageWrapper(std::make_shared<rpc::LeaseSpec>()) {
  RAY_CHECK(task_spec.type() == rpc::TaskType::NORMAL_TASK ||
            task_spec.type() == rpc::TaskType::ACTOR_CREATION_TASK);
  message_->set_job_id(task_spec.job_id());
  message_->mutable_caller_address()->CopyFrom(task_spec.caller_address());
  message_->mutable_required_resources()->insert(task_spec.required_resources().begin(),
                                                 task_spec.required_resources().end());
  message_->mutable_required_placement_resources()->insert(
      task_spec.required_placement_resources().begin(),
      task_spec.required_placement_resources().end());
  message_->mutable_scheduling_strategy()->CopyFrom(task_spec.scheduling_strategy());
  message_->mutable_label_selector()->CopyFrom(task_spec.label_selector());
  message_->mutable_fallback_strategy()->CopyFrom(task_spec.fallback_strategy());
  message_->set_depth(task_spec.depth());
  message_->set_parent_task_id(task_spec.parent_task_id());
  message_->mutable_dependencies()->Reserve(task_spec.args_size());
  for (size_t i = 0; i < static_cast<size_t>(task_spec.args_size()); ++i) {
    if (task_spec.args(i).has_object_ref() && !task_spec.args(i).is_inlined()) {
      message_->add_dependencies()->CopyFrom(task_spec.args(i).object_ref());
    }
  }
  message_->mutable_function_descriptor()->CopyFrom(task_spec.function_descriptor());
  message_->set_language(task_spec.language());
  message_->mutable_runtime_env_info()->CopyFrom(task_spec.runtime_env_info());
  message_->set_attempt_number(task_spec.attempt_number());
  message_->set_root_detached_actor_id(task_spec.root_detached_actor_id());
  message_->set_task_name(task_spec.name());
  message_->set_type(task_spec.type());
  if (IsActorCreationTask()) {
    message_->set_actor_id(task_spec.actor_creation_task_spec().actor_id());
    message_->set_is_detached_actor(task_spec.actor_creation_task_spec().is_detached());
    message_->set_max_actor_restarts(
        task_spec.actor_creation_task_spec().max_actor_restarts());
    for (const auto &option :
         task_spec.actor_creation_task_spec().dynamic_worker_options()) {
      message_->add_dynamic_worker_options(option);
    }
  } else {
    message_->set_max_retries(task_spec.max_retries());
  }
  ComputeResources();
}

LeaseID LeaseSpecification::LeaseId() const {
  return LeaseID::FromBinary(message_->lease_id());
}

JobID LeaseSpecification::JobId() const { return JobID::FromBinary(message_->job_id()); }

const rpc::Address &LeaseSpecification::CallerAddress() const {
  return message_->caller_address();
}

rpc::Language LeaseSpecification::GetLanguage() const { return message_->language(); }

bool LeaseSpecification::IsNormalTask() const {
  return message_->type() == rpc::TaskType::NORMAL_TASK;
}

bool LeaseSpecification::IsActorCreationTask() const {
  return message_->type() == rpc::TaskType::ACTOR_CREATION_TASK;
}

bool LeaseSpecification::IsNodeAffinitySchedulingStrategy() const {
  return GetSchedulingStrategy().scheduling_strategy_case() ==
         rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy;
}

NodeID LeaseSpecification::GetNodeAffinitySchedulingStrategyNodeId() const {
  if (!IsNodeAffinitySchedulingStrategy()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(
      GetSchedulingStrategy().node_affinity_scheduling_strategy().node_id());
}

bool LeaseSpecification::GetNodeAffinitySchedulingStrategySoft() const {
  if (!IsNodeAffinitySchedulingStrategy()) {
    return false;
  }
  return GetSchedulingStrategy().node_affinity_scheduling_strategy().soft();
}

std::vector<ObjectID> LeaseSpecification::GetDependencyIds() const {
  std::vector<ObjectID> ids;
  ids.reserve(dependencies_.size());
  for (const auto &ref : dependencies_) {
    ids.emplace_back(ObjectRefToId(ref));
  }
  return ids;
}

const std::vector<rpc::ObjectReference> &LeaseSpecification::GetDependencies() const {
  return dependencies_;
}

WorkerID LeaseSpecification::CallerWorkerId() const {
  return WorkerID::FromBinary(message_->caller_address().worker_id());
}

NodeID LeaseSpecification::CallerNodeId() const {
  return NodeID::FromBinary(message_->caller_address().node_id());
}

BundleID LeaseSpecification::PlacementGroupBundleId() const {
  if (GetSchedulingStrategy().scheduling_strategy_case() !=
      rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy) {
    return std::make_pair(PlacementGroupID::Nil(), -1);
  }
  const auto &pg = GetSchedulingStrategy().placement_group_scheduling_strategy();
  return std::make_pair(PlacementGroupID::FromBinary(pg.placement_group_id()),
                        pg.placement_group_bundle_index());
}

int64_t LeaseSpecification::MaxActorRestarts() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->max_actor_restarts();
}

int32_t LeaseSpecification::MaxRetries() const {
  RAY_CHECK(IsNormalTask());
  return message_->max_retries();
}

bool LeaseSpecification::IsRetriable() const {
  if (IsActorCreationTask() && MaxActorRestarts() == 0) {
    return false;
  }
  if (IsNormalTask() && MaxRetries() == 0) {
    return false;
  }
  return true;
}

int32_t LeaseSpecification::AttemptNumber() const { return message_->attempt_number(); }

bool LeaseSpecification::IsRetry() const { return AttemptNumber() > 0; }

std::string LeaseSpecification::GetTaskName() const { return message_->task_name(); }

std::string LeaseSpecification::GetFunctionOrActorName() const {
  if (IsActorCreationTask()) {
    return FunctionDescriptor()->ClassName();
  }
  return FunctionDescriptor()->CallString();
}

TaskID LeaseSpecification::ParentTaskId() const {
  // Set to Nil for driver tasks.
  if (message_->parent_task_id().empty()) {
    return TaskID::Nil();
  }
  return TaskID::FromBinary(message_->parent_task_id());
}

ActorID LeaseSpecification::ActorId() const {
  if (message_->actor_id().empty()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_->actor_id());
}

ActorID LeaseSpecification::RootDetachedActorId() const {
  if (message_->root_detached_actor_id().empty()) {
    return ActorID::Nil();
  }
  return ActorID::FromBinary(message_->root_detached_actor_id());
}

bool LeaseSpecification::IsDetachedActor() const { return message_->is_detached_actor(); }

int LeaseSpecification::GetRuntimeEnvHash() const { return runtime_env_hash_; }

std::string LeaseSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "Type=" << TaskType_Name(message_->type())
         << ", Language=" << Language_Name(message_->language());

  if (required_resources_ != nullptr) {
    stream << ", Resources: {";

    // Print resource description.
    for (const auto &entry : GetRequiredResources().GetResourceMap()) {
      stream << entry.first << ": " << entry.second << ", ";
    }
    stream << "}";
  }

  stream << ", function_descriptor=";

  // Print function descriptor.
  stream << FunctionDescriptor()->ToString();

  stream << ", lease_id=" << LeaseId() << ", task_name=" << GetTaskName()
         << ", job_id=" << JobId() << ", depth=" << GetDepth()
         << ", attempt_number=" << AttemptNumber();

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    stream << ", actor_creation_task_spec={actor_id=" << ActorId()
           << ", max_restarts=" << MaxActorRestarts()
           << ", is_detached=" << IsDetachedActor() << "}";
  } else {
    stream << ", normal_task_spec={max_retries=" << MaxRetries() << "}";
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

bool LeaseSpecification::HasRuntimeEnv() const {
  return !IsRuntimeEnvEmpty(SerializedRuntimeEnv());
}

const std::string &LeaseSpecification::SerializedRuntimeEnv() const {
  return message_->runtime_env_info().serialized_runtime_env();
}

const rpc::RuntimeEnvInfo &LeaseSpecification::RuntimeEnvInfo() const {
  return message_->runtime_env_info();
}

int64_t LeaseSpecification::GetDepth() const { return message_->depth(); }

const rpc::SchedulingStrategy &LeaseSpecification::GetSchedulingStrategy() const {
  return message_->scheduling_strategy();
}

const ResourceSet &LeaseSpecification::GetRequiredResources() const {
  return *required_resources_;
}

const ResourceSet &LeaseSpecification::GetRequiredPlacementResources() const {
  return *required_placement_resources_;
}

const LabelSelector &LeaseSpecification::GetLabelSelector() const {
  return *label_selector_;
}

const std::vector<FallbackOption> &LeaseSpecification::GetFallbackStrategy() const {
  return *fallback_strategy_;
}

ray::FunctionDescriptor LeaseSpecification::FunctionDescriptor() const {
  return ray::FunctionDescriptorBuilder::FromProto(message_->function_descriptor());
}

void LeaseSpecification::ComputeResources() {
  auto &required_resources = message_->required_resources();

  if (required_resources.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    required_resources_ = ResourceSet::Nil();
  } else {
    required_resources_ =
        std::make_shared<ResourceSet>(MapFromProtobuf(required_resources));
  }

  auto &required_placement_resources = message_->required_placement_resources().empty()
                                           ? required_resources
                                           : message_->required_placement_resources();

  if (required_placement_resources.empty()) {
    required_placement_resources_ = ResourceSet::Nil();
  } else {
    required_placement_resources_ =
        std::make_shared<ResourceSet>(MapFromProtobuf(required_placement_resources));
  }

  // Set LabelSelector required for scheduling if specified. Parses string map
  // from proto to LabelSelector data type.
  label_selector_ = std::make_shared<LabelSelector>(message_->label_selector());

  // Parse fallback strategy from proto to list of FallbackOption if specified.
  fallback_strategy_ = ParseFallbackStrategy(message_->fallback_strategy().options());

  // Copy dependencies from message
  dependencies_.reserve(message_->dependencies_size());
  for (int i = 0; i < message_->dependencies_size(); ++i) {
    dependencies_.push_back(message_->dependencies(i));
  }

  // There is no need to compute `SchedulingClass` for actor tasks since
  // the actor tasks need not be scheduled.
  const bool is_actor_creation_task = IsActorCreationTask();
  const bool should_report_placement_resources =
      RayConfig::instance().report_actor_placement_resources();
  const auto &resource_set = (is_actor_creation_task && should_report_placement_resources)
                                 ? GetRequiredPlacementResources()
                                 : GetRequiredResources();
  auto depth = GetDepth();
  auto label_selector = GetLabelSelector();
  auto fallback_strategy = GetFallbackStrategy();
  const auto &function_descriptor = FunctionDescriptor();
  auto sched_cls_desc = SchedulingClassDescriptor(resource_set,
                                                  label_selector,
                                                  function_descriptor,
                                                  depth,
                                                  GetSchedulingStrategy(),
                                                  fallback_strategy);
  // Map the scheduling class descriptor to an integer for performance.
  sched_cls_id_ = SchedulingClassToIds::GetSchedulingClass(sched_cls_desc);
  RAY_CHECK_GT(sched_cls_id_, 0);

  runtime_env_hash_ = CalculateRuntimeEnvHash(SerializedRuntimeEnv());
}

std::vector<std::string> LeaseSpecification::DynamicWorkerOptionsOrEmpty() const {
  if (!IsActorCreationTask()) {
    return {};
  }
  return VectorFromProtobuf(message_->dynamic_worker_options());
}

std::vector<std::string> LeaseSpecification::DynamicWorkerOptions() const {
  RAY_CHECK(IsActorCreationTask());
  return VectorFromProtobuf(message_->dynamic_worker_options());
}

size_t LeaseSpecification::DynamicWorkerOptionsSize() const {
  return message_->dynamic_worker_options_size();
}

const rpc::RuntimeEnvConfig &LeaseSpecification::RuntimeEnvConfig() const {
  return message_->runtime_env_info().runtime_env_config();
}

bool LeaseSpecification::IsSpreadSchedulingStrategy() const {
  return message_->scheduling_strategy().scheduling_strategy_case() ==
         rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy;
}

SchedulingClass LeaseSpecification::GetSchedulingClass() const { return sched_cls_id_; }

const rpc::LeaseSpec &LeaseSpecification::GetMessage() const { return *message_; }

}  // namespace ray
