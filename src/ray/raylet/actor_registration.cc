#include "ray/raylet/actor_registration.h"

#include <sstream>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data) {}

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data,
                                     const ActorCheckpointDataT &checkpoint_data)
    : actor_table_data_(actor_table_data),
      execution_dependency_(ObjectID::from_binary(checkpoint_data.execution_dependency)) {
  // Restore `frontier_`.
  for (size_t i = 0; i < checkpoint_data.handle_ids.size(); i++) {
    auto handle_id = ActorHandleID::from_binary(checkpoint_data.handle_ids[i]);
    auto &frontier_entry = frontier_[handle_id];
    frontier_entry.task_counter = checkpoint_data.task_counters[i];
    frontier_entry.execution_dependency =
        ObjectID::from_binary(checkpoint_data.frontier_dependencies[i]);
  }
  // Restore `dummy_objects_`.
  for (size_t i = 0; i < checkpoint_data.unreleased_dummy_objects.size(); i++) {
    auto dummy = ObjectID::from_binary(checkpoint_data.unreleased_dummy_objects[i]);
    dummy_objects_[dummy] = checkpoint_data.num_dummy_object_dependencies[i];
  }
}

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

std::shared_ptr<ActorCheckpointDataT> ActorRegistration::GenerateCheckpointData(
    const ActorID &actor_id, const Task &task) {
  const auto actor_handle_id = task.GetTaskSpecification().ActorHandleId();
  const auto dummy_object = task.GetTaskSpecification().ActorDummyObject();
  // Make a copy of the actor registration, and extend its frontier to include
  // the most recent task.
  // Note(hchen): this is needed because this method is called before
  // `FinishAssignedTask`, which will be called when the worker tries to fetch
  // the next task.
  ActorRegistration copy = *this;
  copy.ExtendFrontier(actor_handle_id, dummy_object);

  // Use actor's current state to generate checkpoint data.
  auto checkpoint_data = std::make_shared<ActorCheckpointDataT>();
  checkpoint_data->actor_id = actor_id.binary();
  checkpoint_data->execution_dependency = copy.GetExecutionDependency().binary();
  for (const auto &frontier : copy.GetFrontier()) {
    checkpoint_data->handle_ids.push_back(frontier.first.binary());
    checkpoint_data->task_counters.push_back(frontier.second.task_counter);
    checkpoint_data->frontier_dependencies.push_back(
        frontier.second.execution_dependency.binary());
  }
  for (const auto &entry : copy.GetDummyObjects()) {
    checkpoint_data->unreleased_dummy_objects.push_back(entry.first.binary());
    checkpoint_data->num_dummy_object_dependencies.push_back(entry.second);
  }
  return checkpoint_data;
}

}  // namespace raylet

}  // namespace ray
