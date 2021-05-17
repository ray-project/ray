#pragma once

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

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
  TaskArgByReference(const ObjectID &object_id, const rpc::Address &owner_address)
      : id_(object_id), owner_address_(owner_address) {}

  void ToProto(rpc::TaskArg *arg_proto) const {
    auto ref = arg_proto->mutable_object_ref();
    ref->set_object_id(id_.Binary());
    ref->mutable_owner_address()->CopyFrom(owner_address_);
  }

 private:
  /// Id of the argument if passed by reference, otherwise nullptr.
  const ObjectID id_;
  const rpc::Address owner_address_;
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
    for (const auto &nested_id : value_->GetNestedIds()) {
      arg_proto->add_nested_inlined_ids(nested_id.Binary());
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
      const TaskID &task_id, const std::string name, const Language &language,
      const ray::FunctionDescriptor &function_descriptor, const JobID &job_id,
      const TaskID &parent_task_id, uint64_t parent_counter, const TaskID &caller_id,
      const rpc::Address &caller_address, uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      const BundleID &bundle_id, bool placement_group_capture_child_tasks,
      const std::string &debugger_breakpoint,
      const std::string &serialized_runtime_env = "{}",
      const std::unordered_map<std::string, std::string> &override_environment_variables =
          {}) {
    message_->set_type(TaskType::NORMAL_TASK);
    message_->set_name(name);
    message_->set_language(language);
    *message_->mutable_function_descriptor() = function_descriptor->GetMessage();
    message_->set_job_id(job_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_parent_task_id(parent_task_id.Binary());
    message_->set_parent_counter(parent_counter);
    message_->set_caller_id(caller_id.Binary());
    message_->mutable_caller_address()->CopyFrom(caller_address);
    message_->set_num_returns(num_returns);
    message_->mutable_required_resources()->insert(required_resources.begin(),
                                                   required_resources.end());
    message_->mutable_required_placement_resources()->insert(
        required_placement_resources.begin(), required_placement_resources.end());
    message_->set_placement_group_id(bundle_id.first.Binary());
    message_->set_placement_group_bundle_index(bundle_id.second);
    message_->set_placement_group_capture_child_tasks(
        placement_group_capture_child_tasks);
    message_->set_debugger_breakpoint(debugger_breakpoint);
    message_->set_serialized_runtime_env(serialized_runtime_env);
    for (const auto &env : override_environment_variables) {
      (*message_->mutable_override_environment_variables())[env.first] = env.second;
    }
    return *this;
  }

  /// Set the driver attributes of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetDriverTaskSpec(const TaskID &task_id, const Language &language,
                                     const JobID &job_id, const TaskID &parent_task_id,
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
      const ActorID &actor_id, int64_t max_restarts = 0, int64_t max_task_retries = 0,
      const std::vector<std::string> &dynamic_worker_options = {},
      int max_concurrency = 1, bool is_detached = false, std::string name = "",
      bool is_asyncio = false, const std::string &extension_data = "") {
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
    actor_creation_spec->set_is_asyncio(is_asyncio);
    actor_creation_spec->set_extension_data(extension_data);
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
