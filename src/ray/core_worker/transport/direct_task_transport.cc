#include "ray/core_worker/transport/direct_task_transport.h"

namespace ray {

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
      std::shared_ptr<TaskState>(new TaskState{task, std::move(local_dependencies)});
  num_pending_ += 1;

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
              num_pending_ -= 1;
            }
          }
          if (complete) {
            on_complete();
          }
        });
  }
}

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    // TODO(ekl) should have a queue per distinct resource type required
    absl::MutexLock lock(&mu_);
    RequestNewWorkerIfNeeded(task_spec);
    queued_tasks_.push_back(task_spec);
    // The task is now queued and will be picked up by the next leased or newly
    // idle worker. We are guaranteed a worker will show up since we called
    // RequestNewWorkerIfNeeded() earlier while holding mu_.
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(const WorkerAddress addr) {
  // Setup client state for this worker.
  {
    absl::MutexLock lock(&mu_);
    worker_request_pending_ = false;

    auto it = client_cache_.find(addr);
    if (it == client_cache_.end()) {
      client_cache_[addr] =
          std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(addr));
      RAY_LOG(INFO) << "Connected to " << addr.first << ":" << addr.second;
    }
  }

  // Try to assign it work.
  OnWorkerIdle(addr, /*error=*/false);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(const WorkerAddress &addr,
                                                 bool was_error) {
  absl::MutexLock lock(&mu_);
  if (queued_tasks_.empty() || was_error) {
    RAY_CHECK_OK(lease_client_.ReturnWorker(addr.second));
  } else {
    auto &client = *client_cache_[addr];
    PushNormalTask(addr, client, queued_tasks_.front());
    queued_tasks_.pop_front();
  }
  // We have a queue of tasks, try to request more workers.
  if (!queued_tasks_.empty()) {
    RequestNewWorkerIfNeeded(queued_tasks_.front());
  }
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const TaskSpecification &resource_spec) {
  if (worker_request_pending_) {
    return;
  }
  RAY_CHECK_OK(lease_client_.RequestWorkerLease(resource_spec));
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
    RAY_CHECK_OK(in_memory_store_.Put(RayObject(nullptr, meta_buffer), object_id));
  }
}

// TODO(ekl) consider reconsolidating with DirectActorTransport.
void CoreWorkerDirectTaskSubmitter::PushNormalTask(const WorkerAddress &addr,
                                                   rpc::CoreWorkerClientInterface &client,
                                                   TaskSpecification &task_spec) {
  auto task_id = task_spec.TaskId();
  auto num_returns = task_spec.NumReturns();
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  auto status = client.PushNormalTask(
      std::move(request),
      [this, task_id, num_returns, addr](Status status, const rpc::PushTaskReply &reply) {
        OnWorkerIdle(addr, /*error=*/!status.ok());
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
              in_memory_store_.Put(RayObject(data_buffer, metadata_buffer), object_id));
        }
      });
  RAY_CHECK_OK(status);
}
};  // namespace ray
