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

#include "ray/core_worker/transport/dependency_resolver.h"

namespace ray {
namespace core {

struct TaskState {
  TaskState(TaskSpecification t,
            absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> deps,
            std::vector<ActorID> actor_ids)
      : task(t),
        local_dependencies(std::move(deps)),
        actor_dependencies(std::move(actor_ids)),
        status(Status::OK()) {
    obj_dependencies_remaining = local_dependencies.size();
    actor_dependencies_remaining = actor_dependencies.size();
  }
  /// The task to be run.
  TaskSpecification task;
  /// The local dependencies to resolve for this task. Objects are nullptr if not yet
  /// resolved.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_dependencies;
  std::vector<ActorID> actor_dependencies;
  /// Number of local dependencies that aren't yet resolved (have nullptrs in the above
  /// map).
  size_t actor_dependencies_remaining;
  size_t obj_dependencies_remaining;
  Status status;
};

void InlineDependencies(
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> dependencies,
    TaskSpecification &task,
    std::vector<ObjectID> *inlined_dependency_ids,
    std::vector<ObjectID> *contained_ids) {
  auto &msg = task.GetMutableMessage();
  size_t found = 0;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      const auto &id = task.ArgId(i);
      const auto &it = dependencies.find(id);
      if (it != dependencies.end()) {
        RAY_CHECK(it->second);
        auto *mutable_arg = msg.mutable_args(i);
        if (!it->second->IsInPlasmaError()) {
          // The object has not been promoted to plasma. Inline the object by
          // clearing the reference and replacing it with the raw value.
          mutable_arg->clear_object_ref();
          if (it->second->HasData()) {
            const auto &data = it->second->GetData();
            mutable_arg->set_data(data->Data(), data->Size());
          }
          if (it->second->HasMetadata()) {
            const auto &metadata = it->second->GetMetadata();
            mutable_arg->set_metadata(metadata->Data(), metadata->Size());
          }
          for (const auto &nested_ref : it->second->GetNestedRefs()) {
            mutable_arg->add_nested_inlined_refs()->CopyFrom(nested_ref);
            contained_ids->push_back(ObjectID::FromBinary(nested_ref.object_id()));
          }
          inlined_dependency_ids->push_back(id);
        }
        found++;
      }
    }
  }
  // Each dependency could be inlined more than once.
  RAY_CHECK(found >= dependencies.size());
}

void LocalDependencyResolver::ResolveDependencies(
    TaskSpecification &task, std::function<void(Status)> on_complete) {
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_dependencies;
  std::vector<ActorID> actor_dependences;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      local_dependencies.emplace(task.ArgId(i), nullptr);
    }
    for (const auto &in : task.ArgInlinedRefs(i)) {
      auto object_id = ObjectID::FromBinary(in.object_id());
      if (ObjectID::IsActorID(object_id)) {
        auto actor_id = ObjectID::ToActorID(object_id);
        if (actor_creator_.IsActorInRegistering(actor_id)) {
          actor_dependences.emplace_back(ObjectID::ToActorID(object_id));
        }
      }
    }
  }
  if (local_dependencies.empty() && actor_dependences.empty()) {
    on_complete(Status::OK());
    return;
  }

  // This is deleted when the last dependency fetch callback finishes.
  std::shared_ptr<TaskState> state = std::make_shared<TaskState>(
      task, std::move(local_dependencies), std::move(actor_dependences));
  num_pending_ += 1;

  for (const auto &it : state->local_dependencies) {
    const ObjectID &obj_id = it.first;
    in_memory_store_.GetAsync(
        obj_id, [this, state, obj_id, on_complete](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);
          bool complete = false;
          std::vector<ObjectID> inlined_dependency_ids;
          std::vector<ObjectID> contained_ids;
          {
            absl::MutexLock lock(&mu_);
            state->local_dependencies[obj_id] = std::move(obj);
            if (--state->obj_dependencies_remaining == 0) {
              InlineDependencies(state->local_dependencies,
                                 state->task,
                                 &inlined_dependency_ids,
                                 &contained_ids);
              if (state->actor_dependencies_remaining == 0) {
                complete = true;
                num_pending_ -= 1;
              }
            }
          }
          if (inlined_dependency_ids.size() > 0) {
            task_finisher_.OnTaskDependenciesInlined(inlined_dependency_ids,
                                                     contained_ids);
          }
          if (complete) {
            on_complete(state->status);
          }
        });
  }

  for (const auto &actor_id : state->actor_dependencies) {
    actor_creator_.AsyncWaitForActorRegisterFinish(
        actor_id, [this, state, on_complete](const Status &status) {
          if (!status.ok()) {
            state->status = status;
          }
          if (--state->actor_dependencies_remaining == 0 &&
              state->obj_dependencies_remaining == 0) {
            num_pending_--;
            on_complete(state->status);
          }
        });
  }
}

}  // namespace core
}  // namespace ray
