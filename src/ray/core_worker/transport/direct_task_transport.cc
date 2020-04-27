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

#include "ray/core_worker/transport/direct_task_transport.h"

#include "ray/core_worker/transport/dependency_resolver.h"

namespace ray {

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submit task " << task_spec.TaskId();
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    RAY_LOG(DEBUG) << "Task dependencies resolved " << task_spec.TaskId();
    if (actor_create_callback_ && task_spec.IsActorCreationTask()) {
      // If gcs actor management is enabled, the actor creation task will be sent to
      // gcs server directly after the in-memory dependent objects are resolved. For
      // more details please see the protocol of actor management based on gcs.
      // https://docs.google.com/document/d/1EAWide-jy05akJp6OMtDn58XOK7bUyruWMia4E-fV28/edit?usp=sharing
      auto actor_id = task_spec.ActorCreationId();
      auto task_id = task_spec.TaskId();
      RAY_LOG(INFO) << "Submitting actor creation task to GCS: " << actor_id;
      auto status =
          actor_create_callback_(task_spec, [this, actor_id, task_id](Status status) {
            // If GCS is failed, GcsRpcClient may receive IOError status but it will
            // not trigger this callback, because GcsRpcClient has retry logic at the
            // bottom. So if this callback is invoked with an error there must be
            // something wrong with the protocol of gcs-based actor management.
            // So just check `status.ok()` here.
            RAY_CHECK_OK(status);
            RAY_LOG(INFO) << "Actor creation task submitted to GCS: " << actor_id;
            task_finisher_->CompletePendingTask(task_id, rpc::PushTaskReply(),
                                                rpc::Address());
          });
      RAY_CHECK_OK(status);
      return;
    }

    bool keep_executing = true;
    {
      absl::MutexLock lock(&mu_);
      if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end()) {
        cancelled_tasks_.erase(task_spec.TaskId());
        keep_executing = false;
      }
      if (keep_executing) {
        // Note that the dependencies in the task spec are mutated to only contain
        // plasma dependencies after ResolveDependencies finishes.
        const SchedulingKey scheduling_key(
            task_spec.GetSchedulingClass(), task_spec.GetDependencies(),
            task_spec.IsActorCreationTask() ? task_spec.ActorCreationId()
                                            : ActorID::Nil());
        auto it = task_queues_.find(scheduling_key);
        if (it == task_queues_.end()) {
          it =
              task_queues_.emplace(scheduling_key, std::deque<TaskSpecification>()).first;
        }
        it->second.push_back(task_spec);
        RequestNewWorkerIfNeeded(scheduling_key);
      }
    }
    if (!keep_executing) {
      task_finisher_->PendingTaskFailed(task_spec.TaskId(),
                                        rpc::ErrorType::TASK_CANCELLED, nullptr);
    }
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::AddWorkerLeaseClient(
    const rpc::WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client) {
  auto it = client_cache_.find(addr);
  if (it == client_cache_.end()) {
    client_cache_[addr] =
        std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(addr.ToProto()));
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
  RAY_CHECK(lease_entry.first);
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
    auto task_spec = queue_entry->second.front();
    PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
    executing_tasks_.emplace(task_spec.TaskId(), addr);
    queue_entry->second.pop_front();
    // Delete the queue if it's now empty. Note that the queue cannot already be empty
    // because this is the only place tasks are removed from it.
    if (queue_entry->second.empty()) {
      task_queues_.erase(queue_entry);
      RAY_LOG(DEBUG) << "Task queue empty, canceling lease request";
      CancelWorkerLeaseIfNeeded(scheduling_key);
    }
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::CancelWorkerLeaseIfNeeded(
    const SchedulingKey &scheduling_key) {
  auto queue_entry = task_queues_.find(scheduling_key);
  if (queue_entry != task_queues_.end()) {
    // There are still pending tasks, so let the worker lease request succeed.
    return;
  }

  auto it = pending_lease_requests_.find(scheduling_key);
  if (it != pending_lease_requests_.end()) {
    // There is an in-flight lease request. Cancel it.
    auto &lease_client = it->second.first;
    auto &lease_id = it->second.second;
    RAY_LOG(DEBUG) << "Canceling lease request " << lease_id;
    RAY_UNUSED(lease_client->CancelWorkerLease(
        lease_id, [this, scheduling_key](const Status &status,
                                         const rpc::CancelWorkerLeaseReply &reply) {
          absl::MutexLock lock(&mu_);
          if (status.ok() && !reply.success()) {
            // The cancellation request can fail if the raylet does not have
            // the request queued. This can happen if: a) due to message
            // reordering, the raylet has not yet received the worker lease
            // request, or b) we have already returned the worker lease
            // request. In the former case, we should try the cancellation
            // request again. In the latter case, the in-flight lease request
            // should already have been removed from our local state, so we no
            // longer need to cancel.
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
        }));
  }
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
  RAY_LOG(DEBUG) << "Lease requested " << task_id;
  RAY_UNUSED(lease_client->RequestWorkerLease(
      resource_spec, [this, scheduling_key](const Status &status,
                                            const rpc::RequestWorkerLeaseReply &reply) {
        absl::MutexLock lock(&mu_);

        auto it = pending_lease_requests_.find(scheduling_key);
        RAY_CHECK(it != pending_lease_requests_.end());
        auto lease_client = std::move(it->second.first);
        const auto task_id = it->second.second;
        pending_lease_requests_.erase(it);

        if (status.ok()) {
          if (reply.canceled()) {
            RAY_LOG(DEBUG) << "Lease canceled " << task_id;
            RequestNewWorkerIfNeeded(scheduling_key);
          } else if (!reply.worker_address().raylet_id().empty()) {
            // We got a lease for a worker. Add the lease client state and try to
            // assign work to the worker.
            RAY_LOG(DEBUG) << "Lease granted " << task_id;
            rpc::WorkerAddress addr(reply.worker_address());
            AddWorkerLeaseClient(addr, std::move(lease_client));
            auto resources_copy = reply.resource_mapping();
            OnWorkerIdle(addr, scheduling_key,
                         /*error=*/false, resources_copy);
          } else {
            // The raylet redirected us to a different raylet to retry at.
            RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
          }
        } else if (lease_client != local_lease_client_) {
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
          RAY_LOG(FATAL) << status.ToString();
        }
      }));
  RAY_CHECK(pending_lease_requests_
                .emplace(scheduling_key, std::make_pair(lease_client, task_id))
                .second);
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
  request->mutable_caller_address()->CopyFrom(rpc_address_);
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request->mutable_resource_mapping()->CopyFrom(assigned_resources);
  request->set_intended_worker_id(addr.worker_id.Binary());
  RAY_UNUSED(client.PushNormalTask(
      std::move(request),
      [this, task_id, is_actor, is_actor_creation, scheduling_key, addr,
       assigned_resources](Status status, const rpc::PushTaskReply &reply) {
        {
          absl::MutexLock lock(&mu_);
          executing_tasks_.erase(task_id);
        }
        if (reply.worker_exiting()) {
          // The worker is draining and will shutdown after it is done. Don't return
          // it to the Raylet since that will kill it early.
          absl::MutexLock lock(&mu_);
          worker_to_lease_client_.erase(addr);
        } else if (!status.ok() || !is_actor_creation) {
          // Successful actor creation leases the worker indefinitely from the raylet.
          absl::MutexLock lock(&mu_);
          OnWorkerIdle(addr, scheduling_key,
                       /*error=*/!status.ok(), assigned_resources);
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
          task_finisher_->CompletePendingTask(task_id, reply, addr.ToProto());
        }
      }));
}

Status CoreWorkerDirectTaskSubmitter::CancelTask(TaskSpecification task_spec,
                                                 bool force_kill) {
  RAY_LOG(INFO) << "Killing task: " << task_spec.TaskId();
  const SchedulingKey scheduling_key(
      task_spec.GetSchedulingClass(), task_spec.GetDependencies(),
      task_spec.IsActorCreationTask() ? task_spec.ActorCreationId() : ActorID::Nil());
  std::shared_ptr<rpc::CoreWorkerClientInterface> client = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end() ||
        !task_finisher_->MarkTaskCanceled(task_spec.TaskId())) {
      return Status::OK();
    }

    auto scheduled_tasks = task_queues_.find(scheduling_key);
    // This cancels tasks that have completed dependencies and are awaiting
    // a worker lease.
    if (scheduled_tasks != task_queues_.end()) {
      for (auto spec = scheduled_tasks->second.begin();
           spec != scheduled_tasks->second.end(); spec++) {
        if (spec->TaskId() == task_spec.TaskId()) {
          scheduled_tasks->second.erase(spec);

          if (scheduled_tasks->second.empty()) {
            task_queues_.erase(scheduling_key);
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
          task_finisher_->PendingTaskFailed(task_spec.TaskId(),
                                            rpc::ErrorType::TASK_CANCELLED);
          return Status::OK();
        }
      }
    }
    // This will get removed either when the RPC call to cancel is returned
    // or when all dependencies are resolved.
    RAY_CHECK(cancelled_tasks_.emplace(task_spec.TaskId()).second);
    auto rpc_client = executing_tasks_.find(task_spec.TaskId());
    // Looks for an RPC handle for the worker executing the task.
    if (rpc_client != executing_tasks_.end() &&
        client_cache_.find(rpc_client->second) != client_cache_.end()) {
      client = client_cache_.find(rpc_client->second)->second;
    }
  }

  // This case is reached for tasks that have unresolved dependencies.
  if (client == nullptr) {
    return Status::OK();
  }

  auto request = rpc::CancelTaskRequest();
  request.set_intended_task_id(task_spec.TaskId().Binary());
  request.set_force_kill(force_kill);
  RAY_UNUSED(client->CancelTask(
      request, [this, task_spec, force_kill](const Status &status,
                                             const rpc::CancelTaskReply &reply) {
        absl::MutexLock lock(&mu_);
        cancelled_tasks_.erase(task_spec.TaskId());
        if (status.ok() && !reply.attempt_succeeded()) {
          if (cancel_retry_timer_.has_value()) {
            if (cancel_retry_timer_->expiry().time_since_epoch() <=
                std::chrono::high_resolution_clock::now().time_since_epoch()) {
              cancel_retry_timer_->expires_after(boost::asio::chrono::milliseconds(
                  RayConfig::instance().cancellation_retry_ms()));
            }
            cancel_retry_timer_->async_wait(boost::bind(
                &CoreWorkerDirectTaskSubmitter::CancelTask, this, task_spec, force_kill));
          }
        }
        // Retry is not attempted if !status.ok() because force-kill may kill the worker
        // before the reply is sent.
      }));
  return Status::OK();
}
};  // namespace ray
