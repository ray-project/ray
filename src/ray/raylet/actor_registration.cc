#include "ray/raylet/actor_registration.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data),
      execution_dependency_(ObjectID::nil()),
      frontier_() {}

const ClientID ActorRegistration::GetNodeManagerId() const {
  return ClientID::from_binary(actor_table_data_.node_manager_id);
}

const ObjectID ActorRegistration::GetActorCreationDependency() const {
  return ObjectID::from_binary(actor_table_data_.actor_creation_dummy_object_id);
}

const ObjectID ActorRegistration::GetExecutionDependency() const {
  return execution_dependency_;
}

const DriverID ActorRegistration::GetDriverId() const {
  return DriverID::from_binary(actor_table_data_.driver_id);
}

const int64_t ActorRegistration::GetMaxReconstructions() const {
  return actor_table_data_.max_reconstructions;
}

const int64_t ActorRegistration::GetRemainingReconstructions() const {
  return actor_table_data_.remaining_reconstructions;
}

const ActorState ActorRegistration::GetState() const { return actor_table_data_.state; }

const std::vector<ObjectID> &ActorRegistration::GetDummyObjects() const {
  return dummy_objects_;
}

const std::unordered_map<ActorHandleID, ActorRegistration::FrontierLeaf>
    &ActorRegistration::GetFrontier() const {
  return frontier_;
}

void ActorRegistration::ExtendFrontier(const ActorHandleID &handle_id,
                                       const ObjectID &execution_dependency) {
  auto &frontier_entry = frontier_[handle_id];
  frontier_entry.task_counter++;
  frontier_entry.execution_dependency = execution_dependency;
  execution_dependency_ = execution_dependency;
  dummy_objects_.push_back(execution_dependency);
}

}  // namespace raylet

}  // namespace ray
