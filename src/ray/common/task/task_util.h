#pragma once

#include "ray/core_worker/common.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/protobuf/common.pb.h"

namespace ray {

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
      const TaskID &task_id, const Language &language,
      const ray::FunctionDescriptor &function_descriptor, const JobID &job_id,
      const TaskID &parent_task_id, uint64_t parent_counter, const TaskID &caller_id,
      const rpc::Address &caller_address, uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources) {
    message_->set_type(TaskType::NORMAL_TASK);
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
      const ActorID &actor_id, int64_t max_restarts = 0,
      const std::vector<std::string> &dynamic_worker_options = {},
      int max_concurrency = 1, bool is_detached = false, std::string name = "",
      bool is_asyncio = false, const std::string &extension_data = "") {
    message_->set_type(TaskType::ACTOR_CREATION_TASK);
    auto actor_creation_spec = message_->mutable_actor_creation_task_spec();
    actor_creation_spec->set_actor_id(actor_id.Binary());
    actor_creation_spec->set_max_actor_restarts(max_restarts);
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
