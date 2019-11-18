#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

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

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    // TODO(ekl) should have a queue per distinct resource type required
    absl::MutexLock lock(&mu_);
    queued_tasks_.push_back(task_spec);
    RequestNewWorkerIfNeeded(task_spec);
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(
    const WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client) {
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
    worker_to_lease_client_[addr] = std::move(lease_client);
  }

  // Try to assign it work.
  OnWorkerIdle(addr, /*error=*/false);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(const WorkerAddress &addr,
                                                 bool was_error) {
  absl::MutexLock lock(&mu_);
  if (queued_tasks_.empty() || was_error) {
    auto lease_client = std::move(worker_to_lease_client_[addr]);
    worker_to_lease_client_.erase(addr);
    RAY_CHECK_OK(lease_client->ReturnWorker(addr.second));
  } else {
    auto &client = *client_cache_[addr];
    PushNormalTask(addr, client, queued_tasks_.front());
    queued_tasks_.pop_front();
  }
  RequestNewWorkerIfNeeded(queued_tasks_.front());
}

std::shared_ptr<WorkerLeaseInterface>
CoreWorkerDirectTaskSubmitter::GetOrConnectLeaseClient(
    const rpc::Address *raylet_address) {
  std::shared_ptr<WorkerLeaseInterface> lease_client;
  if (raylet_address) {
    // Connect to raylet.
    ClientID raylet_id = ClientID::FromBinary(raylet_address->raylet_id());
    auto it = remote_lease_clients_.find(raylet_id);
    if (it == remote_lease_clients_.end()) {
      RAY_LOG(DEBUG) << "Connecting to raylet " << raylet_id;
      it =
          remote_lease_clients_.emplace(raylet_id, lease_client_factory_(*raylet_address))
              .first;
    }
    lease_client = it->second;
  } else {
    lease_client = local_lease_client_;
  }

  return lease_client;
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const TaskSpecification &resource_spec, const rpc::Address *raylet_address) {
  if (worker_request_pending_) {
    return;
  }
  if (queued_tasks_.empty()) {
    // We don't have any tasks to run, so no need to request a worker.
    return;
  }

  // NOTE(swang): We must copy the resource spec here because the resource spec
  // may get swapped out by the time the callback fires. If we change this so
  // that we associate the granted worker with the requested resource spec,
  // then we can just pass the ref instead of copying.
  TaskSpecification resource_spec_copy(resource_spec.GetMessage());
  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  RAY_CHECK_OK(lease_client->RequestWorkerLease(
      resource_spec_copy,
      [this, resource_spec_copy, lease_client](
          const Status &status, const rpc::WorkerLeaseReply &reply) mutable {
        if (status.ok()) {
          if (!reply.worker_address().raylet_id().empty()) {
            RAY_LOG(DEBUG) << "Lease granted " << resource_spec_copy.TaskId();
            HandleWorkerLeaseGranted(
                {reply.worker_address().ip_address(), reply.worker_address().port()},
                std::move(lease_client));
          } else {
            absl::MutexLock lock(&mu_);
            worker_request_pending_ = false;
            RequestNewWorkerIfNeeded(resource_spec_copy,
                                     &reply.retry_at_raylet_address());
          }
        } else {
          RAY_LOG(DEBUG) << "Retrying lease request " << resource_spec_copy.TaskId();
          absl::MutexLock lock(&mu_);
          worker_request_pending_ = false;
          if (lease_client != local_lease_client_) {
            // A remote request failed. Retry the worker lease request locally
            // if it's still in the queue.
            // TODO(swang): Fail after some number of retries?
            RAY_LOG(ERROR) << "Retrying attempt to schedule task at remote node. Error: "
                           << status.ToString();
            RequestNewWorkerIfNeeded(resource_spec_copy);
          } else {
            RAY_LOG(FATAL) << "Lost connection with local raylet. Error: "
                           << status.ToString();
          }
        }
      }));
  worker_request_pending_ = true;
}

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
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::WORKER_DIED,
                            in_memory_store_);
          return;
        }
        WriteObjectsToMemoryStore(reply, in_memory_store_);
      });
  if (!status.ok()) {
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::WORKER_DIED,
                      in_memory_store_);
  }
}
};  // namespace ray
