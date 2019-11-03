#include "ray/core_worker/transport/direct_task_transport.h"

namespace ray {

void DoInlineObjectValue(
    const ObjectID& obj_id, std::shared_ptr<RayObject> obj,
    const TaskSpecification& task) {
  RAY_LOG(ERROR) << "inlining " << obj_id;
  auto msg = task.GetMutableMessage();
  for (size_t i=0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      const auto& id = task.ArgId(i, 0);
      if (id == obj_id) {
        msg.args(i).clear_object_ids();
        msg.args(i).set_data("");
        msg.args(i).set_metadata("");
        return;
      }
    }
  }
  RAY_CHECK(false) << "obj id " << obj_id << " not found";
}

void DependencyResolver::ResolveDependencies(
    const TaskSpecification& task, std::function<void()> on_complete) {
  absl::flat_hash_set<ObjectID> local_dependencies;
  absl::flat_hash_set<ObjectID> remote_dependencies;
  for (size_t i=0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      RAY_CHECK(count <= 1) << "multi args not implemented";
      const auto& id = task.ArgId(i, 0);
      if (id.IsDirectActorType()) {
        local_dependencies.insert(id);
      } else {
        remote_dependencies.insert(id);
      }
    }
  }
  if (local_dependencies.empty() && remote_dependencies.empty()) {
    on_complete();
    return;
  }

  TaskState* state = new TaskState{
    task, std::move(local_dependencies), std::move(remote_dependencies)};
  int64_t tag;
  {
    absl::MutexLock lock(&mu_);
    tag = next_request_id_++;
    pending_[tag] = std::unique_ptr<TaskState>(state);
  }

  if (!state->remote_dependencies.empty()) {
    // TODO(ekl) handle remote dep wait properly
    state->remote_dependencies.clear();
//    std::vector<ObjectID> deps;
//    deps.insert(deps.begin(), state->remote_dependencies.begin(), state->remote_dependencies.end());
//    raylet_client_.WaitForDirectActorCallArgs(deps, tag);
  }

  if (!state->local_dependencies.empty()) {
    for (const auto& obj_id : state->local_dependencies) {
      store_provider_.GetAsync(obj_id, [this, state, obj_id, tag, on_complete](
            std::shared_ptr<RayObject> obj) {
        RAY_CHECK(obj != nullptr);
        bool ok = false;
        {
          absl::MutexLock lock(&mu_);
          state->local_dependencies.erase(obj_id);
          DoInlineObjectValue(obj_id, obj, state->task);
          if (state->remote_dependencies.empty() && state->local_dependencies.empty()) {
            pending_.erase(tag);
            ok = true;
          }
        }
        if (ok) {
          on_complete();
        }
      });
    }
  }
}

Status CoreWorkerDirectTaskSubmitter::SubmitTask(const TaskSpecification &task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    // TODO(ekl) should have a queue per distinct resource type required
    RequestNewWorkerIfNeeded(task_spec);
    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    auto msg = task_spec.GetMutableMessage();
    request->mutable_task_spec()->Swap(&msg);
    {
      absl::MutexLock lock(&mu_);
      queued_tasks_.push_back(std::move(request));
    }
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(const std::string &address,
                                                             int port) {
  WorkerAddress addr = std::make_pair(address, port);

  // Setup client state for this worker.
  {
    absl::MutexLock lock(&mu_);
    worker_request_pending_ = false;

    auto it = client_cache_.find(addr);
    if (it == client_cache_.end()) {
      client_cache_[addr] =
          std::unique_ptr<rpc::DirectActorClient>(new rpc::DirectActorClient(
              address, port, direct_actor_submitter_.CallManager()));
      RAY_LOG(INFO) << "Connected to " << address << ":" << port;
    }
  }

  // Try to assign it work.
  WorkerIdle(addr);
}

void CoreWorkerDirectTaskSubmitter::WorkerIdle(const WorkerAddress &addr) {
  absl::MutexLock lock(&mu_);
  if (queued_tasks_.empty()) {
    RAY_CHECK_OK(raylet_client_.ReturnWorker(addr.second));
  } else {
    auto &client = *client_cache_[addr];
    PushTask(addr, client, std::move(queued_tasks_.front()));
    queued_tasks_.pop_front();
  }
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const TaskSpecification &resource_spec) {
  absl::MutexLock lock(&mu_);
  if (worker_request_pending_) {
    return;
  }
  RAY_CHECK_OK(raylet_client_.RequestWorkerLease(resource_spec));
  worker_request_pending_ = true;
}

void CoreWorkerDirectTaskSubmitter::PushTask(
    const WorkerAddress &addr, rpc::DirectActorClient &client,
    std::unique_ptr<rpc::PushTaskRequest> request) {
  auto status = client.PushTaskImmediate(
      std::move(request), [this, addr](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          RAY_LOG(FATAL) << "Task failed with error: " << status;
        }
        for (int i = 0; i < reply.return_objects_size(); i++) {
          const auto &return_object = reply.return_objects(i);
          ObjectID object_id = ObjectID::FromBinary(return_object.object_id());
          std::shared_ptr<LocalMemoryBuffer> data_buffer;
          if (return_object.data().size() > 0) {
            data_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.data().data())),
                return_object.data().size());
          }
          std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
          if (return_object.metadata().size() > 0) {
            metadata_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
                return_object.metadata().size());
          }
          RAY_CHECK_OK(
              store_provider_->Put(RayObject(data_buffer, metadata_buffer), object_id));
          WorkerIdle(addr);
        }
      });
  RAY_CHECK_OK(status);
}
};  // namespace ray
