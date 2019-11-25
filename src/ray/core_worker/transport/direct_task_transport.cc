#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    absl::MutexLock lock(&mu_);
    // Required to populate the task spec's scheduling class.
    const SchedulingClass scheduling_class = task_spec.GetSchedulingClass();
    auto it = task_queues_.find(scheduling_class);
    if (it == task_queues_.end()) {
      task_queues_.emplace(scheduling_class, std::deque<TaskSpecification>(1, task_spec));
    } else {
      it->second.push_back(task_spec);
    }
    RequestNewWorkerIfNeeded(scheduling_class);
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(
    const rpc::WorkerAddress &addr, const SchedulingClass &scheduling_class,
    std::shared_ptr<WorkerLeaseInterface> lease_client) {
  // Setup client state for this worker.
  {
    absl::MutexLock lock(&mu_);
    pending_lease_requests_.erase(scheduling_class);

    auto it = client_cache_.find(addr);
    if (it == client_cache_.end()) {
      client_cache_[addr] =
          std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(addr));
      RAY_LOG(INFO) << "Connected to " << addr.first << ":" << addr.second;
    }
    int64_t expiration = current_time_ms() + lease_timeout_ms_;
    worker_to_lease_client_.emplace(addr,
                                    std::make_pair(std::move(lease_client), expiration));
  }

  // Try to assign it work.
  OnWorkerIdle(addr, scheduling_class, /*error=*/false);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(const rpc::WorkerAddress &addr,
                                                 const SchedulingClass &scheduling_class,
                                                 bool was_error) {
  absl::MutexLock lock(&mu_);
  auto lease_entry = worker_to_lease_client_[addr];
  auto queue_entry = task_queues_.find(scheduling_class);
  if (was_error || queue_entry == task_queues_.end() ||
      current_time_ms() > lease_entry.second) {
    RAY_CHECK_OK(lease_entry.first->ReturnWorker(addr.second, was_error));
    worker_to_lease_client_.erase(addr);
  } else {
    auto &client = *client_cache_[addr];
    PushNormalTask(addr, client, scheduling_class, queue_entry->second.front());
    queue_entry->second.pop_front();
    if (queue_entry->second.empty()) {
      task_queues_.erase(queue_entry->first);
    }
  }
  RequestNewWorkerIfNeeded(scheduling_class);
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
    const SchedulingClass &scheduling_class, const rpc::Address *raylet_address) {
  if (pending_lease_requests_.find(scheduling_class) != pending_lease_requests_.end()) {
    return;
  }
  auto it = task_queues_.find(scheduling_class);
  if (it == task_queues_.end()) {
    // We don't have any tasks to run, so no need to request a worker.
    return;
  }

  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  TaskSpecification &resource_spec = it->second.front();
  TaskID task_id = resource_spec.TaskId();
  RAY_CHECK_OK(lease_client->RequestWorkerLease(
      resource_spec,
      [this, lease_client, task_id, scheduling_class](
          const Status &status, const rpc::WorkerLeaseReply &reply) mutable {
        if (status.ok()) {
          if (!reply.worker_address().raylet_id().empty()) {
            RAY_LOG(DEBUG) << "Lease granted " << task_id;
            HandleWorkerLeaseGranted(
                {reply.worker_address().ip_address(), reply.worker_address().port()},
                scheduling_class, std::move(lease_client));
          } else {
            absl::MutexLock lock(&mu_);
            pending_lease_requests_.erase(scheduling_class);
            RequestNewWorkerIfNeeded(scheduling_class, &reply.retry_at_raylet_address());
          }
        } else {
          RAY_LOG(DEBUG) << "Retrying lease request " << task_id;
          absl::MutexLock lock(&mu_);
          pending_lease_requests_.erase(scheduling_class);
          if (lease_client != local_lease_client_) {
            // A remote request failed. Retry the worker lease request locally
            // if it's still in the queue.
            // TODO(swang): Fail after some number of retries?
            RAY_LOG(ERROR) << "Retrying attempt to schedule task at remote node. Error: "
                           << status.ToString();
            RequestNewWorkerIfNeeded(scheduling_class);
          } else {
            RAY_LOG(FATAL) << "Lost connection with local raylet. Error: "
                           << status.ToString();
          }
        }
      }));
  pending_lease_requests_.insert(scheduling_class);
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(
    const rpc::WorkerAddress &addr, rpc::CoreWorkerClientInterface &client,
    const SchedulingClass &scheduling_class, TaskSpecification &task_spec) {
  auto task_id = task_spec.TaskId();
  auto num_returns = task_spec.NumReturns();
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  auto status = client.PushNormalTask(
      std::move(request), [this, task_id, scheduling_class, num_returns, addr](
                              Status status, const rpc::PushTaskReply &reply) {
        OnWorkerIdle(addr, scheduling_class, /*error=*/!status.ok());
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
