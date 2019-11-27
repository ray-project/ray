#include "ray/core_worker/transport/dependency_resolver.h"

namespace ray {

struct TaskState {
  TaskState(TaskSpecification t, absl::flat_hash_set<ObjectID> deps)
      : task(t), local_dependencies(deps) {}
  /// The task to be run.
  TaskSpecification task;
  /// The remaining dependencies to resolve for this task.
  absl::flat_hash_set<ObjectID> local_dependencies;
};

void DoInlineObjectValue(const ObjectID &obj_id, std::shared_ptr<RayObject> value,
                         TaskSpecification &task) {
  auto &msg = task.GetMutableMessage();
  bool found = false;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      const auto &id = task.ArgId(i, 0);
      if (id == obj_id) {
        auto *mutable_arg = msg.mutable_args(i);
        mutable_arg->clear_object_ids();
        if (value->IsInPlasmaError()) {
          // Promote the object id to plasma.
          mutable_arg->add_object_ids(
              obj_id.WithTransportType(TaskTransportType::RAYLET).Binary());
        } else {
          // Inline the object value.
          if (value->HasData()) {
            const auto &data = value->GetData();
            mutable_arg->set_data(data->Data(), data->Size());
          }
          if (value->HasMetadata()) {
            const auto &metadata = value->GetMetadata();
            mutable_arg->set_metadata(metadata->Data(), metadata->Size());
          }
        }
        found = true;
      }
    }
  }
  RAY_CHECK(found) << "obj id " << obj_id << " not found";
}

void LocalDependencyResolver::ResolveDependencies(TaskSpecification &task,
                                                  std::function<void()> on_complete) {
  absl::flat_hash_set<ObjectID> local_dependencies;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      RAY_CHECK(count <= 1) << "multi args not implemented";
      const auto &id = task.ArgId(i, 0);
      if (id.IsDirectCallType()) {
        local_dependencies.insert(id);
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

  for (const auto &obj_id : state->local_dependencies) {
    in_memory_store_->GetAsync(
        obj_id, [this, state, obj_id, on_complete](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);
          bool complete = false;
          {
            absl::MutexLock lock(&mu_);
            state->local_dependencies.erase(obj_id);
            DoInlineObjectValue(obj_id, obj, state->task);
            if (state->local_dependencies.empty()) {
              complete = true;
              num_pending_ -= 1;
            }
          }
          if (complete) {
            on_complete();
          }
        });
  }
}

}  // namespace ray
