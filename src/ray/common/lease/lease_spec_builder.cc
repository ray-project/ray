// Copyright 2019-2020 The Ray Authors.
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

#include "ray/common/lease/lease_spec_builder.h"

namespace ray {

LeaseSpecBuilder &LeaseSpecBuilder::BuildCommonLeaseSpec(
    const JobID &job_id,
    const rpc::Address &caller_address,
    const google::protobuf::Map<std::string, double> &required_resources,
    const google::protobuf::Map<std::string, double> &required_placement_resources,
    const rpc::SchedulingStrategy &scheduling_strategy,
    const google::protobuf::Map<std::string, std::string> &label_selector,
    int64_t depth,
    const std::vector<rpc::ObjectReference> &dependencies,
    const rpc::Language &language,
    const rpc::RuntimeEnvInfo &runtime_env_info,
    const TaskID &parent_task_id,
    const ray::rpc::FunctionDescriptor &function_descriptor,
    const std::string &task_name,
    uint64_t attempt_number,
    const ActorID &root_detached_actor_id) {
  // Set basic task information
  message_->set_job_id(job_id.Binary());
  message_->mutable_caller_address()->CopyFrom(caller_address);

  // Set resource requirements
  message_->mutable_required_resources()->insert(required_resources.begin(),
                                                 required_resources.end());
  message_->mutable_required_placement_resources()->insert(
      required_placement_resources.begin(), required_placement_resources.end());

  // Set scheduling information
  message_->mutable_scheduling_strategy()->CopyFrom(scheduling_strategy);
  message_->mutable_label_selector()->insert(label_selector.begin(),
                                             label_selector.end());
  message_->set_depth(depth);
  message_->set_parent_task_id(parent_task_id.Binary());
  // Set dependencies
  for (const auto &dep : dependencies) {
    rpc::ObjectReference *dependency = message_->add_dependencies();
    dependency->CopyFrom(dep);
  }

  // Set function descriptor
  message_->mutable_function_descriptor()->CopyFrom(function_descriptor);

  // Set language
  message_->set_language(language);

  // Set runtime environment info
  message_->mutable_runtime_env_info()->CopyFrom(runtime_env_info);

  message_->set_task_name(task_name);
  message_->set_attempt_number(attempt_number);
  message_->set_root_detached_actor_id(root_detached_actor_id.Binary());
  return *this;
}

LeaseSpecBuilder &LeaseSpecBuilder::SetActorCreationLeaseSpec(
    const ActorID &actor_id,
    int64_t max_restarts,
    bool is_detached_actor,
    const google::protobuf::RepeatedPtrField<std::string> &dynamic_worker_options) {
  // Set task type
  message_->set_type(TaskType::ACTOR_CREATION_TASK);

  // Set actor creation specific fields
  message_->set_actor_id(actor_id.Binary());

  // Create actor creation task spec to store actor-specific information
  message_->set_max_actor_restarts(max_restarts);
  message_->set_is_detached_actor(is_detached_actor);
  for (const auto &option : dynamic_worker_options) {
    message_->add_dynamic_worker_options(option);
  }
  return *this;
}

LeaseSpecBuilder &LeaseSpecBuilder::SetNormalLeaseSpec(int max_retries) {
  // Set task type
  message_->set_type(TaskType::NORMAL_TASK);

  // Set normal task specific fields
  message_->set_max_retries(max_retries);

  return *this;
}

}  // namespace ray
