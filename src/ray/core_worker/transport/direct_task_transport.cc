#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

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
    const rpc::WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client) {
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

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(const rpc::WorkerAddress &addr,
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

void CoreWorkerDirectTaskSubmitter::PushNormalTask(const rpc::WorkerAddress &addr,
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
