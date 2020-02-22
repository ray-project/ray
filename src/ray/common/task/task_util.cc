#include "ray/common/task/task_util.h"

namespace ray {

TaskSpecBuilder::TaskSpecBuilder() : message_(std::make_shared<rpc::TaskSpec>()) {}

TaskSpecification TaskSpecBuilder::Build() { return TaskSpecification(message_); }

const rpc::TaskSpec &TaskSpecBuilder::GetMessage() const { return *message_; }

TaskSpecBuilder &TaskSpecBuilder::SetDriverTaskSpec(const TaskID &task_id,
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
  message_->set_is_direct_call(false);
  return *this;
}

TaskSpecBuilder &TaskSpecBuilder::AddByRefArg(const ObjectID &arg_id) {
  message_->add_args()->add_object_ids(arg_id.Binary());
  return *this;
}

TaskSpecBuilder &TaskSpecBuilder::AddByValueArg(const RayObject &value) {
  auto arg = message_->add_args();
  if (value.HasData()) {
    const auto &data = value.GetData();
    arg->set_data(data->Data(), data->Size());
  }
  if (value.HasMetadata()) {
    const auto &metadata = value.GetMetadata();
    arg->set_metadata(metadata->Data(), metadata->Size());
  }
  for (const auto &nested_id : value.GetNestedIds()) {
    arg->add_nested_inlined_ids(nested_id.Binary());
  }
  return *this;
}

TaskSpecBuilder &TaskSpecBuilder::SetActorTaskSpec(
    const ActorID &actor_id, const ObjectID &actor_creation_dummy_object_id,
    const ObjectID &previous_actor_task_dummy_object_id, uint64_t actor_counter) {
  message_->set_type(TaskType::ACTOR_TASK);
  auto actor_spec = message_->mutable_actor_task_spec();
  actor_spec->set_actor_id(actor_id.Binary());
  actor_spec->set_actor_creation_dummy_object_id(actor_creation_dummy_object_id.Binary());
  actor_spec->set_previous_actor_task_dummy_object_id(
      previous_actor_task_dummy_object_id.Binary());
  actor_spec->set_actor_counter(actor_counter);
  return *this;
}

}  // namespace ray
