#ifndef RAY_COMMON_TASK_TASK_UTIL_H
#define RAY_COMMON_TASK_TASK_UTIL_H

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
      const Language &language, const std::vector<std::string> &function_descriptor,
      const JobID &job_id, const TaskID &parent_task_id, uint64_t parent_counter,
      uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources) {
    message_->set_type(TaskType::NORMAL_TASK);
    message_->set_language(language);
    for (const auto &fd : function_descriptor) {
      message_->add_function_descriptor(fd);
    }
    message_->set_job_id(job_id.Binary());
    message_->set_task_id(
        GenerateTaskId(job_id, parent_task_id, parent_counter).Binary());
    message_->set_parent_task_id(parent_task_id.Binary());
    message_->set_parent_counter(parent_counter);
    message_->set_num_returns(num_returns);
    message_->mutable_required_resources()->insert(required_resources.begin(),
                                                   required_resources.end());
    message_->mutable_required_placement_resources()->insert(
        required_placement_resources.begin(), required_placement_resources.end());
    return *this;
  }

  /// Add a by-reference argument to the task.
  ///
  /// \param arg_id Id of the argument.
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &AddByRefArg(const ObjectID &arg_id) {
    message_->add_args()->add_object_ids(arg_id.Binary());
    return *this;
  }

  /// Add a by-value argument to the task.
  ///
  /// \param data String object that contains the data.
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &AddByValueArg(const std::string &data) {
    message_->add_args()->set_data(data);
    return *this;
  }

  /// Add a by-value argument to the task.
  ///
  /// \param data Pointer to the data.
  /// \param size Size of the data.
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &AddByValueArg(const void *data, size_t size) {
    message_->add_args()->set_data(data, size);
    return *this;
  }

  /// Set the `ActorCreationTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorCreationTaskSpec(
      const ActorID &actor_id, uint64_t max_reconstructions = 0,
      const std::vector<std::string> &dynamic_worker_options = {}) {
    message_->set_type(TaskType::ACTOR_CREATION_TASK);
    auto actor_creation_spec = message_->mutable_actor_creation_task_spec();
    actor_creation_spec->set_actor_id(actor_id.Binary());
    actor_creation_spec->set_max_actor_reconstructions(max_reconstructions);
    for (const auto &option : dynamic_worker_options) {
      actor_creation_spec->add_dynamic_worker_options(option);
    }
    return *this;
  }

  /// Set the `ActorTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorTaskSpec(
      const ActorID &actor_id, const ActorHandleID &actor_handle_id,
      const ObjectID &actor_creation_dummy_object_id,
      const ObjectID &previous_actor_task_dummy_object_id, uint64_t actor_counter,
      const std::vector<ActorHandleID> &new_handle_ids = {}) {
    message_->set_type(TaskType::ACTOR_TASK);
    auto actor_spec = message_->mutable_actor_task_spec();
    actor_spec->set_actor_id(actor_id.Binary());
    actor_spec->set_actor_handle_id(actor_handle_id.Binary());
    actor_spec->set_actor_creation_dummy_object_id(
        actor_creation_dummy_object_id.Binary());
    actor_spec->set_previous_actor_task_dummy_object_id(
        previous_actor_task_dummy_object_id.Binary());
    actor_spec->set_actor_counter(actor_counter);
    for (const auto &id : new_handle_ids) {
      actor_spec->add_new_actor_handles(id.Binary());
    }
    return *this;
  }

 private:
  std::shared_ptr<rpc::TaskSpec> message_;
};

}  // namespace ray

#endif  // RAY_COMMON_TASK_TASK_UTIL_H
