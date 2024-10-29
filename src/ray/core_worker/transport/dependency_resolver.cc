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

void LocalDependencyResolver::CancelDependencyResolution(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  pending_tasks_.erase(task_id);
}

void LocalDependencyResolver::ResolveDependencies(
    TaskSpecification &task, std::function<void(Status)> on_dependencies_resolved) {
  const TaskID task_id = task.TaskId();
  std::unordered_set<ObjectID> local_dependency_ids;
  std::unordered_set<ActorID> actor_dependency_ids;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      local_dependency_ids.insert(task.ArgId(i));
    }
    for (const auto &in : task.ArgInlinedRefs(i)) {
      auto object_id = ObjectID::FromBinary(in.object_id());
      if (ObjectID::IsActorID(object_id)) {
        auto actor_id = ObjectID::ToActorID(object_id);
        if (actor_creator_.IsActorInRegistering(actor_id)) {
          actor_dependency_ids.insert(ObjectID::ToActorID(object_id));
        }
      }
    }
  }
  if (local_dependency_ids.empty() && actor_dependency_ids.empty()) {
    on_dependencies_resolved(Status::OK());
    return;
  }

  std::weak_ptr<TaskState> weak_task_state;
  {
    absl::MutexLock lock(&mu_);
    pending_tasks_[task_id].push_back(
        std::make_shared<TaskState>(task,
                                    local_dependency_ids,
                                    actor_dependency_ids,
                                    std::move(on_dependencies_resolved)));
    weak_task_state = pending_tasks_[task_id].back();
  }

  for (const auto &obj_id : local_dependency_ids) {
    in_memory_store_.GetAsync(
        obj_id, [this, weak_task_state, task_id, obj_id](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);

          std::vector<ObjectID> inlined_dependency_ids;
          std::vector<ObjectID> contained_ids;
          bool resolved = false;  // If all objects are resolved.
          std::shared_ptr<TaskState> state;
          {
            absl::MutexLock lock(&mu_);
            state = weak_task_state.lock();
            if (!state) {
              // Dependency resolution was cancelled.
              return;
            }

            auto &pending_tasks_for_id = pending_tasks_.at(task_id);
            auto it = std::find(
                pending_tasks_for_id.begin(), pending_tasks_for_id.end(), state);
            // Checks the dependency resolution still exists since it's not cancelled.
            RAY_CHECK(it != pending_tasks_for_id.end());

            state->local_dependencies[obj_id] = std::move(obj);
            if (--state->obj_dependencies_remaining == 0) {
              InlineDependencies(state->local_dependencies,
                                 state->task,
                                 &inlined_dependency_ids,
                                 &contained_ids);
              if (state->actor_dependencies_remaining == 0) {
                pending_tasks_for_id.erase(it);
                resolved = true;
              }
            }
          }

          if (inlined_dependency_ids.size() > 0) {
            task_finisher_.OnTaskDependenciesInlined(inlined_dependency_ids,
                                                     contained_ids);
          }
          if (resolved) {
            state->on_dependencies_resolved(state->status);
          }
        });
  }

  for (const auto &actor_id : actor_dependency_ids) {
    actor_creator_.AsyncWaitForActorRegisterFinish(
        actor_id,
        [this, weak_task_state, task_id, on_dependencies_resolved](const Status &status) {
          std::shared_ptr<TaskState> state;
          bool resolved = false;

          {
            absl::MutexLock lock(&mu_);
            state = weak_task_state.lock();
            auto &pending_tasks_for_id = pending_tasks_.at(task_id);
            auto it = std::find(
                pending_tasks_for_id.begin(), pending_tasks_for_id.end(), state);
            RAY_CHECK(it != pending_tasks_for_id.end());

            if (!status.ok()) {
              state->status = status;
            }
            if (--state->actor_dependencies_remaining == 0 &&
                state->obj_dependencies_remaining == 0) {
              resolved = true;
              pending_tasks_for_id.erase(it);
            }
          }
          if (resolved) {
            state->on_dependencies_resolved(state->status);
          }
        });
  }
}

}  // namespace core
}  // namespace ray
