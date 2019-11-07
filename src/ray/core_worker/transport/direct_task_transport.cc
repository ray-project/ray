#include "ray/core_worker/transport/direct_task_transport.h"

namespace ray {

// TODO(ekl) add unit tests for local dep resolver

void DoInlineObjectValue(const ObjectID &obj_id, std::shared_ptr<RayObject> value,
                         const TaskSpecification &task) {
  auto &msg = task.GetMutableMessage();
  bool found = false;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      const auto &id = task.ArgId(i, 0);
      if (id == obj_id) {
        auto *mutable_arg = msg.mutable_args(i);
        mutable_arg->clear_object_ids();
        if (value->HasData()) {
          const auto &data = value->GetData();
          mutable_arg->set_data(data->Data(), data->Size());
        }
        if (value->HasMetadata()) {
          const auto &metadata = value->GetMetadata();
          mutable_arg->set_metadata(metadata->Data(), metadata->Size());
        }
        found = true;
      }
    }
  }
  RAY_CHECK(found) << "obj id " << obj_id << " not found";
}

void LocalDependencyResolver::ResolveDependencies(const TaskSpecification &task,
                                                  std::function<void()> on_complete) {
  absl::flat_hash_set<ObjectID> local_dependencies;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      RAY_CHECK(count <= 1) << "multi args not implemented";
      const auto &id = task.ArgId(i, 0);
      if (id.IsDirectActorType()) {
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
      std::shared_ptr<TaskState>(new TaskState{task, std::move(local_dependencies)});

  for (const auto &obj_id : state->local_dependencies) {
    in_memory_store_.GetAsync(
        obj_id, [this, state, obj_id, on_complete](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);
          bool complete = false;
          {
            absl::MutexLock lock(&mu_);
            state->local_dependencies.erase(obj_id);
            DoInlineObjectValue(obj_id, obj, state->task);
            if (state->local_dependencies.empty()) {
              complete = true;
            }
          }
          if (complete) {
            on_complete();
          }
        });
  }
}

// TODO(ekl) add unit tests for direct task submitter

Status CoreWorkerDirectTaskSubmitter::SubmitTask(const TaskSpecification &task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    // TODO(ekl) should have a queue per distinct resource type required
    absl::MutexLock lock(&mu_);
    RequestNewWorkerIfNeeded(task_spec);
    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    auto msg = task_spec.GetMutableMessage();
    request->mutable_task_spec()->Swap(&msg);
    queued_tasks_.push_back(std::move(request));
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
      client_cache_[addr] = std::unique_ptr<rpc::CoreWorkerClient>(
          new rpc::CoreWorkerClient(address, port, client_call_manager_));
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
  if (worker_request_pending_) {
    return;
  }
  RAY_CHECK_OK(raylet_client_.RequestWorkerLease(resource_spec));
  worker_request_pending_ = true;
}

void CoreWorkerDirectTaskSubmitter::TreatTaskAsFailed(const TaskID &task_id,
                                                      int num_returns,
                                                      const rpc::ErrorType &error_type) {
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    RAY_CHECK_OK(in_memory_store_->Put(RayObject(nullptr, meta_buffer), object_id));
  }
}

void CoreWorkerDirectTaskSubmitter::PushTask(
    const WorkerAddress &addr, rpc::CoreWorkerClient &client,
    std::unique_ptr<rpc::PushTaskRequest> request) {
  auto task_id = TaskID::FromBinary(request->task_spec().task_id());
  auto num_returns = request->task_spec().num_returns();
  auto status = client.PushTaskImmediate(
      std::move(request),
      [this, task_id, num_returns, addr](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::WORKER_DIED);
          return;
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
              in_memory_store_->Put(RayObject(data_buffer, metadata_buffer), object_id));
          WorkerIdle(addr);
        }
      });
  RAY_CHECK_OK(status);
}
};  // namespace ray
