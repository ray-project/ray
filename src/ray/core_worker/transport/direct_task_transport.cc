#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submit task " << task_spec.TaskId();
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    RAY_LOG(DEBUG) << "Task dependencies resolved " << task_spec.TaskId();
    absl::MutexLock lock(&mu_);
    // Note that the dependencies in the task spec are mutated to only contain
    // plasma dependencies after ResolveDependencies finishes.
    const SchedulingKey scheduling_key(task_spec.GetSchedulingClass(),
                                       task_spec.GetDependencies());
    auto it = task_queues_.find(scheduling_key);
    if (it == task_queues_.end()) {
      it = task_queues_.emplace(scheduling_key, std::deque<TaskSpecification>()).first;
    }
    it->second.push_back(task_spec);
    RequestNewWorkerIfNeeded(scheduling_key);
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::AddWorkerLeaseClient(
    const rpc::WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client) {
  auto it = client_cache_.find(addr);
  if (it == client_cache_.end()) {
    client_cache_[addr] = std::shared_ptr<rpc::CoreWorkerClientInterface>(
        client_factory_(addr.ip_address, addr.port));
    RAY_LOG(INFO) << "Connected to " << addr.ip_address << ":" << addr.port;
  }
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  worker_to_lease_client_.emplace(addr,
                                  std::make_pair(std::move(lease_client), expiration));
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr, const SchedulingKey &scheduling_key, bool was_error,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto lease_entry = worker_to_lease_client_[addr];
  auto queue_entry = task_queues_.find(scheduling_key);
  // Return the worker if there was an error executing the previous task,
  // the previous task is an actor creation task,
  // there are no more applicable queued tasks, or the lease is expired.
  if (was_error || queue_entry == task_queues_.end() ||
      current_time_ms() > lease_entry.second) {
    auto status = lease_entry.first->ReturnWorker(addr.port, addr.worker_id, was_error);
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
    }
    worker_to_lease_client_.erase(addr);
  } else {
    auto &client = *client_cache_[addr];
    PushNormalTask(addr, client, scheduling_key, queue_entry->second.front(),
                   assigned_resources);
    queue_entry->second.pop_front();
    // Delete the queue if it's now empty. Note that the queue cannot already be empty
    // because this is the only place tasks are removed from it.
    if (queue_entry->second.empty()) {
      task_queues_.erase(queue_entry);
    }
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

std::shared_ptr<WorkerLeaseInterface>
CoreWorkerDirectTaskSubmitter::GetOrConnectLeaseClient(
    const rpc::Address *raylet_address) {
  std::shared_ptr<WorkerLeaseInterface> lease_client;
  if (raylet_address &&
      ClientID::FromBinary(raylet_address->raylet_id()) != local_raylet_id_) {
    // A remote raylet was specified. Connect to the raylet if needed.
    ClientID raylet_id = ClientID::FromBinary(raylet_address->raylet_id());
    auto it = remote_lease_clients_.find(raylet_id);
    if (it == remote_lease_clients_.end()) {
      RAY_LOG(DEBUG) << "Connecting to raylet " << raylet_id;
      it = remote_lease_clients_
               .emplace(raylet_id, lease_client_factory_(raylet_address->ip_address(),
                                                         raylet_address->port()))
               .first;
    }
    lease_client = it->second;
  } else {
    lease_client = local_lease_client_;
  }

  return lease_client;
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const SchedulingKey &scheduling_key, const rpc::Address *raylet_address) {
  if (pending_lease_requests_.find(scheduling_key) != pending_lease_requests_.end()) {
    // There's already an outstanding lease request for this type of task.
    return;
  }
  auto it = task_queues_.find(scheduling_key);
  if (it == task_queues_.end()) {
    // We don't have any of this type of task to run.
    return;
  }

  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  TaskSpecification &resource_spec = it->second.front();
  TaskID task_id = resource_spec.TaskId();
  auto status = lease_client->RequestWorkerLease(
      resource_spec,
      [this, lease_client, task_id, scheduling_key](
          const Status &status, const rpc::WorkerLeaseReply &reply) mutable {
        absl::MutexLock lock(&mu_);
        pending_lease_requests_.erase(scheduling_key);
        if (status.ok()) {
          if (!reply.worker_address().raylet_id().empty()) {
            // We got a lease for a worker. Add the lease client state and try to
            // assign work to the worker.
            RAY_LOG(DEBUG) << "Lease granted " << task_id;
            rpc::WorkerAddress addr = {
                reply.worker_address().ip_address(), reply.worker_address().port(),
                WorkerID::FromBinary(reply.worker_address().worker_id()),
                ClientID::FromBinary(reply.worker_address().raylet_id())};
            AddWorkerLeaseClient(addr, std::move(lease_client));
            auto resources_copy = reply.resource_mapping();
            OnWorkerIdle(addr, scheduling_key,
                         /*error=*/false, resources_copy);
          } else {
            // The raylet redirected us to a different raylet to retry at.
            RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
          }
        } else {
          RetryLeaseRequest(status, lease_client, scheduling_key);
        }
      });
  if (!status.ok()) {
    RetryLeaseRequest(status, lease_client, scheduling_key);
  }
  pending_lease_requests_.insert(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::RetryLeaseRequest(
    Status status, std::shared_ptr<WorkerLeaseInterface> lease_client,
    const SchedulingKey &scheduling_key) {
  if (lease_client != local_lease_client_) {
    // A lease request to a remote raylet failed. Retry locally if the lease is
    // still needed.
    // TODO(swang): Fail after some number of retries?
    RAY_LOG(ERROR) << "Retrying attempt to schedule task at remote node. Error: "
                   << status.ToString();
    RequestNewWorkerIfNeeded(scheduling_key);
  } else {
    // A local request failed. This shouldn't happen if the raylet is still alive
    // and we don't currently handle raylet failures, so treat it as a fatal
    // error.
    RAY_LOG(FATAL) << "Lost connection with local raylet. Error: " << status.ToString();
  }
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(
    const rpc::WorkerAddress &addr, rpc::CoreWorkerClientInterface &client,
    const SchedulingKey &scheduling_key, const TaskSpecification &task_spec,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto task_id = task_spec.TaskId();
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  bool is_actor = task_spec.IsActorTask();
  bool is_actor_creation = task_spec.IsActorCreationTask();

  RAY_LOG(DEBUG) << "Pushing normal task " << task_spec.TaskId();
  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request->mutable_resource_mapping()->CopyFrom(assigned_resources);
  request->set_intended_worker_id(addr.worker_id.Binary());
  auto status = client.PushNormalTask(
      std::move(request),
      [this, task_id, is_actor, is_actor_creation, scheduling_key, addr,
       assigned_resources](Status status, const rpc::PushTaskReply &reply) {
        // Successful actor creation leases the worker indefinitely from the raylet.
        if (!status.ok() || !is_actor_creation) {
          absl::MutexLock lock(&mu_);
          OnWorkerIdle(addr, scheduling_key, /*error=*/!status.ok(), assigned_resources);
        }
        if (!status.ok()) {
          // TODO: It'd be nice to differentiate here between process vs node
          // failure (e.g., by contacting the raylet). If it was a process
          // failure, it may have been an application-level error and it may
          // not make sense to retry the task.
          task_finisher_->PendingTaskFailed(
              task_id,
              is_actor ? rpc::ErrorType::ACTOR_DIED : rpc::ErrorType::WORKER_DIED,
              &status);
        } else {
          rpc::Address proto = addr.ToProto();
          task_finisher_->CompletePendingTask(task_id, reply, &proto);
        }
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error pushing task to worker: " << status.ToString();
    {
      absl::MutexLock lock(&mu_);
      OnWorkerIdle(addr, scheduling_key, /*error=*/true, assigned_resources);
    }
    task_finisher_->PendingTaskFailed(
        task_id, is_actor ? rpc::ErrorType::ACTOR_DIED : rpc::ErrorType::WORKER_DIED,
        &status);
  }
}
};  // namespace ray
