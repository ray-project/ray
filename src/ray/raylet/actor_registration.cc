#include "ray/raylet/actor_registration.h"

#include <sstream>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data) {}

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

const std::unordered_map<ActorHandleID, ActorRegistration::FrontierLeaf>
    &ActorRegistration::GetFrontier() const {
  return frontier_;
}

ObjectID ActorRegistration::ExtendFrontier(const ActorHandleID &handle_id,
                                           const ObjectID &execution_dependency) {
  auto &frontier_entry = frontier_[handle_id];
  // Release the reference to the previous cursor for this
  // actor handle, if there was one.
  ObjectID object_to_release;
  if (!frontier_entry.execution_dependency.is_nil()) {
    auto it = dummy_objects_.find(frontier_entry.execution_dependency);
    RAY_CHECK(it != dummy_objects_.end());
    it->second--;
    RAY_CHECK(it->second >= 0);
    if (it->second == 0) {
      object_to_release = frontier_entry.execution_dependency;
      dummy_objects_.erase(it);
    }
  }

  frontier_entry.task_counter++;
  frontier_entry.execution_dependency = execution_dependency;
  execution_dependency_ = execution_dependency;
  // Add the reference to the new cursor for this actor handle.
  dummy_objects_[execution_dependency]++;
  return object_to_release;
}

void ActorRegistration::AddHandle(const ActorHandleID &handle_id,
                                  const ObjectID &execution_dependency) {
  if (frontier_.find(handle_id) == frontier_.end()) {
    auto &new_handle = frontier_[handle_id];
    new_handle.task_counter = 0;
    new_handle.execution_dependency = execution_dependency;
    dummy_objects_[execution_dependency]++;
  }
}

int ActorRegistration::NumHandles() const { return frontier_.size(); }

}  // namespace raylet

}  // namespace ray
