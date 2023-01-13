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

#pragma once

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Stores the task failure reason.
struct TaskFailureEntry {
  /// The task failure details.
  rpc::RayErrorInfo ray_error_info;

  /// The creation time of this entry.
  std::chrono::steady_clock::time_point creation_time;

  /// Whether this task should be retried.
  bool should_retry;
  TaskFailureEntry(const rpc::RayErrorInfo &ray_error_info, bool should_retry)
      : ray_error_info(ray_error_info),
        creation_time(std::chrono::steady_clock::now()),
        should_retry(should_retry) {}
};

/// Argument of a task.
class TaskArg {
 public:
  virtual void ToProto(rpc::TaskArg *arg_proto) const = 0;
  virtual ~TaskArg(){};
};

class TaskArgByReference : public TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  TaskArgByReference(const ObjectID &object_id,
                     const rpc::Address &owner_address,
                     const std::string &call_site)
      : id_(object_id), owner_address_(owner_address), call_site_(call_site) {}

  void ToProto(rpc::TaskArg *arg_proto) const {
    auto ref = arg_proto->mutable_object_ref();
    ref->set_object_id(id_.Binary());
    ref->mutable_owner_address()->CopyFrom(owner_address_);
    ref->set_call_site(call_site_);
  }

 private:
  /// Id of the argument if passed by reference, otherwise nullptr.
  const ObjectID id_;
  const rpc::Address owner_address_;
  const std::string call_site_;
};

class TaskArgByValue : public TaskArg {
 public:
  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  explicit TaskArgByValue(const std::shared_ptr<RayObject> &value) : value_(value) {
    RAY_CHECK(value) << "Value can't be null.";
  }

  void ToProto(rpc::TaskArg *arg_proto) const {
    if (value_->HasData()) {
      const auto &data = value_->GetData();
      arg_proto->set_data(data->Data(), data->Size());
    }
    if (value_->HasMetadata()) {
      const auto &metadata = value_->GetMetadata();
      arg_proto->set_metadata(metadata->Data(), metadata->Size());
    }
    for (const auto &nested_ref : value_->GetNestedRefs()) {
      arg_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
    }
  }

 private:
  /// Value of the argument.
  const std::shared_ptr<RayObject> value_;
};

/// Helper class for building a `TaskSpecification` object.
class TaskSpecBuilder {
 public:
  TaskSpecBuilder() : message_(std::make_shared<rpc::TaskSpec>()) {}

  /// Build the `TaskSpecification` object.
  TaskSpecification Build() { return TaskSpecification(message_); }

  /// Get a reference to the internal protobuf message object.
  const rpc::TaskSpec &GetMessage() const { return *message_; }

  /// Set the common attributes of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetCommonTaskSpec(
      const TaskID &task_id,
      const std::string name,
      const Language &language,
      const ray::FunctionDescriptor &function_descriptor,
      const JobID &job_id,
      std::optional<rpc::JobConfig> job_config,
      const TaskID &parent_task_id,
      uint64_t parent_counter,
      const TaskID &caller_id,
      const rpc::Address &caller_address,
      uint64_t num_returns,
      bool returns_dynamic,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      const std::string &debugger_breakpoint,
      int64_t depth,
      const std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr,
      const std::string &concurrency_group_name = "") {
    message_->set_type(TaskType::NORMAL_TASK);
    message_->set_name(name);
    message_->set_language(language);
    *message_->mutable_function_descriptor() = function_descriptor->GetMessage();
    message_->set_job_id(job_id.Binary());
    if (job_config.has_value()) {
      message_->mutable_job_config()->CopyFrom(job_config.value());
    }
    message_->set_task_id(task_id.Binary());
    message_->set_parent_task_id(parent_task_id.Binary());
    message_->set_parent_counter(parent_counter);
    message_->set_caller_id(caller_id.Binary());
    message_->mutable_caller_address()->CopyFrom(caller_address);
    message_->set_num_returns(num_returns);
    message_->set_returns_dynamic(returns_dynamic);
    message_->mutable_required_resources()->insert(required_resources.begin(),
                                                   required_resources.end());
    message_->mutable_required_placement_resources()->insert(
        required_placement_resources.begin(), required_placement_resources.end());
    message_->set_debugger_breakpoint(debugger_breakpoint);
    message_->set_depth(depth);
    if (runtime_env_info) {
      message_->mutable_runtime_env_info()->CopyFrom(*runtime_env_info);
    }
    message_->set_concurrency_group_name(concurrency_group_name);
    return *this;
  }

  TaskSpecBuilder &SetNormalTaskSpec(
      int max_retries,
      bool retry_exceptions,
      const std::string &serialized_retry_exception_allowlist,
      const rpc::SchedulingStrategy &scheduling_strategy) {
    message_->set_max_retries(max_retries);
    message_->set_retry_exceptions(retry_exceptions);
    message_->set_serialized_retry_exception_allowlist(
        serialized_retry_exception_allowlist);
    message_->mutable_scheduling_strategy()->CopyFrom(scheduling_strategy);
    return *this;
  }

  /// Set the driver attributes of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetDriverTaskSpec(const TaskID &task_id,
                                     const Language &language,
                                     const JobID &job_id,
                                     const TaskID &parent_task_id,
                                     const TaskID &caller_id,
                                     const rpc::Address &caller_address) {
    message_->set_type(TaskType::DRIVER_TASK);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_parent_task_id(parent_task_id.Binary());
    message_->set_parent_counter(0);
    message_->set_caller_id(caller_id.Binary());
    message_->mutable_caller_address()->CopyFrom(caller_address);
    message_->set_num_returns(0);
    return *this;
  }

  /// Add an argument to the task.
  TaskSpecBuilder &AddArg(const TaskArg &arg) {
    auto ref = message_->add_args();
    arg.ToProto(ref);
    return *this;
  }

  /// Set the `ActorCreationTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorCreationTaskSpec(
      const ActorID &actor_id,
      const std::string &serialized_actor_handle,
      const rpc::SchedulingStrategy &scheduling_strategy,
      int64_t max_restarts = 0,
      int64_t max_task_retries = 0,
      const std::vector<std::string> &dynamic_worker_options = {},
      int max_concurrency = 1,
      bool is_detached = false,
      std::string name = "",
      std::string ray_namespace = "",
      bool is_asyncio = false,
      const std::vector<ConcurrencyGroup> &concurrency_groups = {},
      const std::string &extension_data = "",
      bool execute_out_of_order = false) {
    message_->set_type(TaskType::ACTOR_CREATION_TASK);
    auto actor_creation_spec = message_->mutable_actor_creation_task_spec();
    actor_creation_spec->set_actor_id(actor_id.Binary());
    actor_creation_spec->set_max_actor_restarts(max_restarts);
    actor_creation_spec->set_max_task_retries(max_task_retries);
    for (const auto &option : dynamic_worker_options) {
      actor_creation_spec->add_dynamic_worker_options(option);
    }
    actor_creation_spec->set_max_concurrency(max_concurrency);
    actor_creation_spec->set_is_detached(is_detached);
    actor_creation_spec->set_name(name);
    actor_creation_spec->set_ray_namespace(ray_namespace);
    actor_creation_spec->set_is_asyncio(is_asyncio);
    actor_creation_spec->set_extension_data(extension_data);
    actor_creation_spec->set_serialized_actor_handle(serialized_actor_handle);
    for (const auto &concurrency_group : concurrency_groups) {
      rpc::ConcurrencyGroup *group = actor_creation_spec->add_concurrency_groups();
      group->set_name(concurrency_group.name);
      group->set_max_concurrency(concurrency_group.max_concurrency);
      // Fill into function descriptor.
      for (auto &item : concurrency_group.function_descriptors) {
        rpc::FunctionDescriptor *fd = group->add_function_descriptors();
        *fd = item->GetMessage();
      }
    }
    actor_creation_spec->set_execute_out_of_order(execute_out_of_order);
    message_->mutable_scheduling_strategy()->CopyFrom(scheduling_strategy);
    return *this;
  }

  /// Set the `ActorTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorTaskSpec(const ActorID &actor_id,
                                    const ObjectID &actor_creation_dummy_object_id,
                                    const ObjectID &previous_actor_task_dummy_object_id,
                                    uint64_t actor_counter) {
    message_->set_type(TaskType::ACTOR_TASK);
    auto actor_spec = message_->mutable_actor_task_spec();
    actor_spec->set_actor_id(actor_id.Binary());
    actor_spec->set_actor_creation_dummy_object_id(
        actor_creation_dummy_object_id.Binary());
    actor_spec->set_previous_actor_task_dummy_object_id(
        previous_actor_task_dummy_object_id.Binary());
    actor_spec->set_actor_counter(actor_counter);
    return *this;
  }

 private:
  std::shared_ptr<rpc::TaskSpec> message_;
};

}  // namespace ray
