#include "ray/raylet/actor_registration.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data),
      alive_(true),
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
}

bool ActorRegistration::IsAlive() const { return alive_; }

void ActorRegistration::MarkDead() { alive_ = false; }

}  // namespace raylet

}  // namespace ray
