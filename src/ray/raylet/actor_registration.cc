// Copyright 2017 The Ray Authors.
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

#include "ray/raylet/actor_registration.h"

#include <sstream>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableData &actor_table_data)
    : actor_table_data_(actor_table_data) {
  // The first task submitted on each new actor handle will depend on the actor
  // creation object, so we always pin it.
  dummy_objects_[GetActorCreationDependency()]++;
}

ActorRegistration::ActorRegistration(const ActorTableData &actor_table_data,
                                     const ActorCheckpointData &checkpoint_data)
    : actor_table_data_(actor_table_data),
      execution_dependency_(
          ObjectID::FromBinary(checkpoint_data.execution_dependency())) {
  // Restore `frontier_`.
  for (int64_t i = 0; i < checkpoint_data.handle_ids_size(); i++) {
    auto caller_id = TaskID::FromBinary(checkpoint_data.handle_ids(i));
    auto &frontier_entry = frontier_[caller_id];
    frontier_entry.task_counter = checkpoint_data.task_counters(i);
    frontier_entry.execution_dependency =
        ObjectID::FromBinary(checkpoint_data.frontier_dependencies(i));
  }
  // Restore `dummy_objects_`.
  for (int64_t i = 0; i < checkpoint_data.unreleased_dummy_objects_size(); i++) {
    auto dummy = ObjectID::FromBinary(checkpoint_data.unreleased_dummy_objects(i));
    dummy_objects_[dummy] = checkpoint_data.num_dummy_object_dependencies(i);
  }
}

const ClientID ActorRegistration::GetNodeManagerId() const {
  return ClientID::FromBinary(actor_table_data_.address().raylet_id());
}

const ObjectID ActorRegistration::GetActorCreationDependency() const {
  return ObjectID::FromBinary(actor_table_data_.actor_creation_dummy_object_id());
}

const ObjectID ActorRegistration::GetExecutionDependency() const {
  return execution_dependency_;
}

const JobID ActorRegistration::GetJobId() const {
  return JobID::FromBinary(actor_table_data_.job_id());
}

const int64_t ActorRegistration::GetMaxRestarts() const {
  return actor_table_data_.max_restarts();
}

const int64_t ActorRegistration::GetRemainingRestarts() const {
  if (actor_table_data_.max_restarts() == -1) {
    return -1;
  }
  return actor_table_data_.max_restarts() - actor_table_data_.num_restarts();
}

const uint64_t ActorRegistration::GetNumRestarts() const {
  return actor_table_data_.num_restarts();
}

const std::unordered_map<TaskID, ActorRegistration::FrontierLeaf>
    &ActorRegistration::GetFrontier() const {
  return frontier_;
}

ObjectID ActorRegistration::ExtendFrontier(const TaskID &caller_id,
                                           const ObjectID &execution_dependency) {
  auto &frontier_entry = frontier_[caller_id];
  // Release the reference to the previous cursor for this
  // actor handle, if there was one.
  ObjectID object_to_release;
  if (!frontier_entry.execution_dependency.IsNil()) {
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

int ActorRegistration::NumHandles() const { return frontier_.size(); }

std::shared_ptr<ActorCheckpointData> ActorRegistration::GenerateCheckpointData(
    const ActorID &actor_id, const Task *task) {
  // Make a copy of the actor registration
  ActorRegistration copy = *this;
  if (task) {
    const auto actor_caller_id = task->GetTaskSpecification().CallerId();
    const auto dummy_object = task->GetTaskSpecification().ActorDummyObject();
    // Extend its frontier to include the most recent task.
    // NOTE(hchen): For non-direct-call actors, this is needed because this method is
    // called before `FinishAssignedTask`, which will be called when the worker tries to
    // fetch the next task. For direct-call actors, checkpoint data doesn't contain
    // frontier info, so we don't need to do `ExtendFrontier` here.
    copy.ExtendFrontier(actor_caller_id, dummy_object);
  }

  // Use actor's current state to generate checkpoint data.
  auto checkpoint_data = std::make_shared<ActorCheckpointData>();
  checkpoint_data->set_actor_id(actor_id.Binary());
  checkpoint_data->set_execution_dependency(copy.GetExecutionDependency().Binary());
  for (const auto &frontier : copy.GetFrontier()) {
    checkpoint_data->add_handle_ids(frontier.first.Binary());
    checkpoint_data->add_task_counters(frontier.second.task_counter);
    checkpoint_data->add_frontier_dependencies(
        frontier.second.execution_dependency.Binary());
  }
  for (const auto &entry : copy.GetDummyObjects()) {
    checkpoint_data->add_unreleased_dummy_objects(entry.first.Binary());
    checkpoint_data->add_num_dummy_object_dependencies(entry.second);
  }

  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();
  checkpoint_data->set_checkpoint_id(checkpoint_id.Binary());
  return checkpoint_data;
}

}  // namespace raylet

}  // namespace ray
