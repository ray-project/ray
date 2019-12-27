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
    TaskSpecification &task, std::vector<ObjectID> *inlined) {
  auto &msg = task.GetMutableMessage();
  size_t found = 0;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      const auto &id = task.ArgId(i, 0);
      const auto &it = dependencies.find(id);
      if (it != dependencies.end()) {
        RAY_CHECK(it->second);
        auto *mutable_arg = msg.mutable_args(i);
        mutable_arg->clear_object_ids();
        if (it->second->IsInPlasmaError()) {
          // Promote the object id to plasma.
          mutable_arg->add_object_ids(it->first.Binary());
        } else {
          // Inline the object value.
          if (it->second->HasData()) {
            const auto &data = it->second->GetData();
            mutable_arg->set_data(data->Data(), data->Size());
          }
          if (it->second->HasMetadata()) {
            const auto &metadata = it->second->GetMetadata();
            mutable_arg->set_metadata(metadata->Data(), metadata->Size());
          }
          inlined->push_back(id);
        }
        found++;
      } else {
        RAY_CHECK(!id.IsDirectCallType());
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
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      RAY_CHECK(count <= 1) << "multi args not implemented";
      const auto &id = task.ArgId(i, 0);
      if (id.IsDirectCallType()) {
        local_dependencies.emplace(id, nullptr);
      }
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
    in_memory_store_->GetAsync(
        obj_id, [this, state, obj_id, on_complete](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);
          bool complete = false;
          std::vector<ObjectID> inlined;
          {
            absl::MutexLock lock(&mu_);
            state->local_dependencies[obj_id] = std::move(obj);
            if (--state->dependencies_remaining == 0) {
              InlineDependencies(state->local_dependencies, state->task, &inlined);
              complete = true;
              num_pending_ -= 1;
            }
          }
          if (inlined.size() > 0) {
            task_finisher_->OnTaskDependenciesInlined(inlined);
          }
          if (complete) {
            on_complete();
          }
        });
  }
}

}  // namespace ray
