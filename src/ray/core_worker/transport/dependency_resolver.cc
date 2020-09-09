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

struct TaskState {
  TaskState(TaskSpecification t,
            absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> deps)
      : task(t), local_dependencies(deps), dependencies_remaining(deps.size()) {}
  /// The task to be run.
  TaskSpecification task;
  /// The local dependencies to resolve for this task. Objects are nullptr if not yet
  /// resolved.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_dependencies;
  /// Number of local dependencies that aren't yet resolved (have nullptrs in the above
  /// map).
  size_t dependencies_remaining;
};

void InlineDependencies(
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> dependencies,
    TaskSpecification &task, std::vector<ObjectID> *inlined_dependency_ids,
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
          mutable_arg->mutable_object_ref()->Clear();
          if (it->second->HasData()) {
            const auto &data = it->second->GetData();
            mutable_arg->set_data(data->Data(), data->Size());
          }
          if (it->second->HasMetadata()) {
            const auto &metadata = it->second->GetMetadata();
            mutable_arg->set_metadata(metadata->Data(), metadata->Size());
          }
          for (const auto &nested_id : it->second->GetNestedIds()) {
            mutable_arg->add_nested_inlined_ids(nested_id.Binary());
            contained_ids->push_back(nested_id);
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

void LocalDependencyResolver::ResolveDependencies(TaskSpecification &task,
                                                  std::function<void()> on_complete) {
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_dependencies;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      local_dependencies.emplace(task.ArgId(i), nullptr);
    }
  }
  if (local_dependencies.empty()) {
    on_complete();
    return;
  }

  // This is deleted when the last dependency fetch callback finishes.
  std::shared_ptr<TaskState> state =
      std::make_shared<TaskState>(task, std::move(local_dependencies));
  num_pending_ += 1;

  for (const auto &it : state->local_dependencies) {
    const ObjectID &obj_id = it.first;
    in_memory_store_->GetAsync(obj_id, [this, state, obj_id,
                                        on_complete](std::shared_ptr<RayObject> obj) {
      RAY_CHECK(obj != nullptr);
      bool complete = false;
      std::vector<ObjectID> inlined_dependency_ids;
      std::vector<ObjectID> contained_ids;
      {
        absl::MutexLock lock(&mu_);
        state->local_dependencies[obj_id] = std::move(obj);
        if (--state->dependencies_remaining == 0) {
          InlineDependencies(state->local_dependencies, state->task,
                             &inlined_dependency_ids, &contained_ids);
          complete = true;
          num_pending_ -= 1;
        }
      }
      if (inlined_dependency_ids.size() > 0) {
        task_finisher_->OnTaskDependenciesInlined(inlined_dependency_ids, contained_ids);
      }
      if (complete) {
        on_complete();
      }
    });
  }
}

}  // namespace ray
