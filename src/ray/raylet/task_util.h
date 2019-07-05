#ifndef RAY_RAYLET_TASK_UTIL_H
#define RAY_RAYLET_TASK_UTIL_H

#include "ray/protobuf/common.pb.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

/// Helper function for building the common fields of a `TaskSpec` message.
/// See `common.proto` for the documentation of the arguments.
///
/// \param[out] message The `TaskSpec` message to build.
void BuildCommonTaskSpec(
    rpc::TaskSpec *message, const Language &language,
    const std::vector<std::string> &function_descriptor, const JobID &job_id,
    const TaskID &parent_task_id, uint64_t parent_counter, uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources) {
  // Set common fields.
  message->set_type(rpc::TaskType::NORMAL_TASK);
  message->set_language(language);
  for (const auto &fd : function_descriptor) {
    message->add_function_descriptor(fd);
  }
  message->set_job_id(job_id.Binary());
  message->set_task_id(GenerateTaskId(job_id, parent_task_id, parent_counter).Binary());
  message->set_parent_task_id(parent_task_id.Binary());
  message->set_parent_counter(parent_counter);
  message->set_num_returns(num_returns);
  message->mutable_required_resources()->insert(required_resources.begin(),
                                                required_resources.end());
  message->mutable_required_placement_resources()->insert(
      required_placement_resources.begin(), required_placement_resources.end());
}

/// Helper function for building the `ActorCreactionSpec` in a `TaskSpec` message.
/// See `common.proto` for the documentation of the arguments.
///
/// \param[out] message The `TaskSpec` message to build.
void BuildActorCreationTaskSpec(
    rpc::TaskSpec *message, const ActorID &actor_id, uint64_t max_reconstructions = 0,
    const std::vector<std::string> &dynamic_worker_options = {}) {
  message->set_type(TaskType::ACTOR_CREATION_TASK);
  auto actor_creation_spec = message->mutable_actor_creation_task_spec();
  actor_creation_spec->set_actor_id(actor_id.Binary());
  actor_creation_spec->set_max_actor_reconstructions(max_reconstructions);
  for (const auto &option : dynamic_worker_options) {
    actor_creation_spec->add_dynamic_worker_options(option);
  }
}

/// Helper function for building the `ActorSpec` in a `TaskSpec` message.
/// See `common.proto` for the documentation of the arguments.
///
/// \param[out] message The `TaskSpec` message to build.
void BuildActorTaskSpec(rpc::TaskSpec *message, const ActorID &actor_id,
                        const ActorHandleID &actor_handle_id,
                        const ObjectID &actor_creation_dummy_object_id,
                        uint64_t actor_counter,
                        const std::vector<ActorHandleID> &new_handle_ids = {}) {
  message->set_type(TaskType::ACTOR_TASK);
  auto actor_spec = message->mutable_actor_task_spec();
  actor_spec->set_actor_id(actor_id.Binary());
  actor_spec->set_actor_handle_id(actor_handle_id.Binary());
  actor_spec->set_actor_creation_dummy_object_id(actor_creation_dummy_object_id.Binary());
  actor_spec->set_actor_counter(actor_counter);
  for (const auto &id : new_handle_ids) {
    actor_spec->add_new_actor_handles(id.Binary());
  }
}

}  // namespace raylet
}  // namespace ray

#endif  // RAY_RAYLET_TASK_UTIL_H
