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
#include "ray/gcs/pb_util.h"

namespace ray {
namespace core {

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submit task " << task_spec.TaskId();
  num_tasks_submitted_++;

  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) {
    task_finisher_->MarkDependenciesResolved(task_spec.TaskId());
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Resolving task dependencies failed " << status.ToString();
      RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status));
      return;
    }
    RAY_LOG(DEBUG) << "Task dependencies resolved " << task_spec.TaskId();
    if (task_spec.IsActorCreationTask()) {
      // If gcs actor management is enabled, the actor creation task will be sent to
      // gcs server directly after the in-memory dependent objects are resolved. For
      // more details please see the protocol of actor management based on gcs.
      // https://docs.google.com/document/d/1EAWide-jy05akJp6OMtDn58XOK7bUyruWMia4E-fV28/edit?usp=sharing
      auto actor_id = task_spec.ActorCreationId();
      auto task_id = task_spec.TaskId();
      RAY_LOG(DEBUG) << "Creating actor via GCS actor id = : " << actor_id;
      RAY_CHECK_OK(actor_creator_->AsyncCreateActor(
          task_spec,
          [this, actor_id, task_id](Status status, const rpc::CreateActorReply &reply) {
            if (status.ok()) {
              RAY_LOG(DEBUG) << "Created actor, actor id = " << actor_id;
              // Copy the actor's reply to the GCS for ref counting purposes.
              rpc::PushTaskReply push_task_reply;
              push_task_reply.mutable_borrowed_refs()->CopyFrom(reply.borrowed_refs());
              task_finisher_->CompletePendingTask(task_id,
                                                  push_task_reply,
                                                  reply.actor_address(),
                                                  /*is_application_error=*/false);
            } else {
              rpc::RayErrorInfo ray_error_info;
              if (status.IsSchedulingCancelled()) {
                RAY_LOG(DEBUG) << "Actor creation cancelled, actor id = " << actor_id;
                task_finisher_->MarkTaskCanceled(task_id);
                if (reply.has_death_cause()) {
                  ray_error_info.mutable_actor_died_error()->CopyFrom(
                      reply.death_cause());
                }
              } else {
                RAY_LOG(INFO) << "Failed to create actor " << actor_id
                              << " with status: " << status.ToString();
              }
              RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
                  task_id,
                  rpc::ErrorType::ACTOR_CREATION_FAILED,
                  &status,
                  ray_error_info.has_actor_died_error() ? &ray_error_info : nullptr));
            }
          }));
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
        const SchedulingKey scheduling_key(task_spec.GetSchedulingClass(),
                                           task_spec.GetDependencyIds(),
                                           task_spec.IsActorCreationTask()
                                               ? task_spec.ActorCreationId()
                                               : ActorID::Nil(),
                                           task_spec.GetRuntimeEnvHash());
        auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
        scheduling_key_entry.task_queue.push_back(task_spec);
        scheduling_key_entry.resource_spec = task_spec;

        if (!scheduling_key_entry.AllWorkersBusy()) {
          // There are idle workers, so we don't need more
          // workers.

          for (auto active_worker_addr : scheduling_key_entry.active_workers) {
            RAY_CHECK(worker_to_lease_entry_.find(active_worker_addr) !=
                      worker_to_lease_entry_.end());
            auto &lease_entry = worker_to_lease_entry_[active_worker_addr];
            if (!lease_entry.is_busy) {
              OnWorkerIdle(active_worker_addr,
                           scheduling_key,
                           /*was_error*/ false,
                           /*worker_exiting*/ false,
                           lease_entry.assigned_resources);
              break;
            }
          }
        }
        RequestNewWorkerIfNeeded(scheduling_key);
      }
    }
    if (!keep_executing) {
      RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::TASK_CANCELLED, nullptr));
    }
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::AddWorkerLeaseClient(
    const rpc::WorkerAddress &addr,
    std::shared_ptr<WorkerLeaseInterface> lease_client,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
    const SchedulingKey &scheduling_key,
    const TaskID &task_id) {
  client_cache_->GetOrConnect(addr.ToProto());
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  LeaseEntry new_lease_entry = LeaseEntry(
      std::move(lease_client), expiration, assigned_resources, scheduling_key, task_id);
  worker_to_lease_entry_.emplace(addr, new_lease_entry);

  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.emplace(addr).second);
  RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
}

void CoreWorkerDirectTaskSubmitter::ReturnWorker(const rpc::WorkerAddress addr,
                                                 bool was_error,
                                                 bool worker_exiting,
                                                 const SchedulingKey &scheduling_key) {
  RAY_LOG(DEBUG) << "Returning worker " << addr.worker_id << " to raylet "
                 << addr.raylet_id;
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
  auto &lease_entry = worker_to_lease_entry_[addr];
  RAY_CHECK(lease_entry.lease_client);
  RAY_CHECK(!lease_entry.is_busy);

  // Decrement the number of active workers consuming tasks from the queue associated
  // with the current scheduling_key
  scheduling_key_entry.active_workers.erase(addr);
  if (scheduling_key_entry.CanDelete()) {
    // We can safely remove the entry keyed by scheduling_key from the
    // scheduling_key_entries_ hashmap.
    scheduling_key_entries_.erase(scheduling_key);
  }

  auto status = lease_entry.lease_client->ReturnWorker(
      addr.port, addr.worker_id, was_error, worker_exiting);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
  }
  worker_to_lease_entry_.erase(addr);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr,
    const SchedulingKey &scheduling_key,
    bool was_error,
    bool worker_exiting,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto &lease_entry = worker_to_lease_entry_[addr];
  if (!lease_entry.lease_client) {
    return;
  }

  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &current_queue = scheduling_key_entry.task_queue;
  // Return the worker if there was an error executing the previous task,
  // the lease is expired; Return the worker if there are no more applicable
  // queued tasks.
  if ((was_error || worker_exiting ||
       current_time_ms() > lease_entry.lease_expiration_time) ||
      current_queue.empty()) {
    RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);

    // Return the worker only if there are no tasks to do.
    if (!lease_entry.is_busy) {
      ReturnWorker(addr, was_error, worker_exiting, scheduling_key);
    }
  } else {
    auto &client = *client_cache_->GetOrConnect(addr.ToProto());

    while (!current_queue.empty() && !lease_entry.is_busy) {
      auto task_spec = current_queue.front();
      lease_entry.is_busy = true;

      // Increment the total number of tasks in flight to any worker associated with the
      // current scheduling_key

      RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
      scheduling_key_entry.num_busy_workers++;

      executing_tasks_.emplace(task_spec.TaskId(), addr);
      PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
      current_queue.pop_front();
    }

    CancelWorkerLeaseIfNeeded(scheduling_key);
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::CancelWorkerLeaseIfNeeded(
    const SchedulingKey &scheduling_key) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &task_queue = scheduling_key_entry.task_queue;
  if (!task_queue.empty()) {
    // There are still pending tasks so let the worker lease request succeed.
    return;
  }

  RAY_LOG(DEBUG) << "Task queue is empty; canceling lease request";

  for (auto &pending_lease_request : scheduling_key_entry.pending_lease_requests) {
    // There is an in-flight lease request. Cancel it.
    auto lease_client = GetOrConnectLeaseClient(&pending_lease_request.second);
    auto &task_id = pending_lease_request.first;
    RAY_LOG(DEBUG) << "Canceling lease request " << task_id;
    lease_client->CancelWorkerLease(
        task_id,
        [this, scheduling_key](const Status &status,
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
        });
  }
}

std::shared_ptr<WorkerLeaseInterface>
CoreWorkerDirectTaskSubmitter::GetOrConnectLeaseClient(
    const rpc::Address *raylet_address) {
  std::shared_ptr<WorkerLeaseInterface> lease_client;
  RAY_CHECK(raylet_address != nullptr);
  if (NodeID::FromBinary(raylet_address->raylet_id()) != local_raylet_id_) {
    // A remote raylet was specified. Connect to the raylet if needed.
    NodeID raylet_id = NodeID::FromBinary(raylet_address->raylet_id());
    auto it = remote_lease_clients_.find(raylet_id);
    if (it == remote_lease_clients_.end()) {
      RAY_LOG(INFO) << "Connecting to raylet " << raylet_id;
      it = remote_lease_clients_
               .emplace(raylet_id,
                        lease_client_factory_(raylet_address->ip_address(),
                                              raylet_address->port()))
               .first;
    }
    lease_client = it->second;
  } else {
    lease_client = local_lease_client_;
  }

  return lease_client;
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklog() {
  absl::MutexLock lock(&mu_);
  ReportWorkerBacklogInternal();
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklogInternal() {
  absl::flat_hash_map<SchedulingClass, std::pair<TaskSpecification, int64_t>> backlogs;
  for (auto &scheduling_key_and_entry : scheduling_key_entries_) {
    const SchedulingClass scheduling_class = std::get<0>(scheduling_key_and_entry.first);
    if (backlogs.find(scheduling_class) == backlogs.end()) {
      backlogs[scheduling_class].first = scheduling_key_and_entry.second.resource_spec;
      backlogs[scheduling_class].second = 0;
    }
    // We report backlog size per scheduling class not per scheduling key
    // so we need to aggregate backlog sizes of different scheduling keys
    // with the same scheduling class
    backlogs[scheduling_class].second += scheduling_key_and_entry.second.BacklogSize();
    scheduling_key_and_entry.second.last_reported_backlog_size =
        scheduling_key_and_entry.second.BacklogSize();
  }

  std::vector<rpc::WorkerBacklogReport> backlog_reports;
  for (const auto &backlog : backlogs) {
    rpc::WorkerBacklogReport backlog_report;
    backlog_report.mutable_resource_spec()->CopyFrom(backlog.second.first.GetMessage());
    backlog_report.set_backlog_size(backlog.second.second);
    backlog_reports.emplace_back(backlog_report);
  }
  local_lease_client_->ReportWorkerBacklog(WorkerID::FromBinary(rpc_address_.worker_id()),
                                           backlog_reports);
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklogIfNeeded(
    const SchedulingKey &scheduling_key) {
  const auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  if (scheduling_key_entry.last_reported_backlog_size !=
      scheduling_key_entry.BacklogSize()) {
    ReportWorkerBacklogInternal();
  }
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const SchedulingKey &scheduling_key, const rpc::Address *raylet_address) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  if (scheduling_key_entry.pending_lease_requests.size() ==
      max_pending_lease_requests_per_scheduling_category_) {
    RAY_LOG(DEBUG) << "Exceeding the pending request limit "
                   << max_pending_lease_requests_per_scheduling_category_;
    return;
  }
  RAY_CHECK(scheduling_key_entry.pending_lease_requests.size() <
            max_pending_lease_requests_per_scheduling_category_);

  if (!scheduling_key_entry.AllWorkersBusy()) {
    // There are idle workers, so we don't need more.
    return;
  }

  const auto &task_queue = scheduling_key_entry.task_queue;
  if (task_queue.empty()) {
    if (scheduling_key_entry.CanDelete()) {
      // We can safely remove the entry keyed by scheduling_key from the
      // scheduling_key_entries_ hashmap.
      scheduling_key_entries_.erase(scheduling_key);
    }
    return;
  } else if (scheduling_key_entry.task_queue.size() <=
             scheduling_key_entry.pending_lease_requests.size()) {
    // All tasks have corresponding pending leases, no need to request more
    return;
  }

  num_leases_requested_++;
  // Create a TaskSpecification with an overwritten TaskID to make sure we don't reuse the
  // same TaskID to request a worker
  auto resource_spec_msg = scheduling_key_entry.resource_spec.GetMutableMessage();
  resource_spec_msg.set_task_id(TaskID::FromRandom(job_id_).Binary());
  const TaskSpecification resource_spec = TaskSpecification(resource_spec_msg);
  rpc::Address best_node_address;
  const bool is_spillback = (raylet_address != nullptr);
  bool is_selected_based_on_locality = false;
  if (raylet_address == nullptr) {
    // If no raylet address is given, find the best worker for our next lease request.
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForTask(resource_spec);
    raylet_address = &best_node_address;
  }

  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  const TaskID task_id = resource_spec.TaskId();
  const std::string task_name = resource_spec.GetName();
  RAY_LOG(DEBUG) << "Requesting lease from raylet "
                 << NodeID::FromBinary(raylet_address->raylet_id()) << " for task "
                 << task_id;

  lease_client->RequestWorkerLease(
      resource_spec.GetMessage(),
      /*grant_or_reject=*/is_spillback,
      [this,
       scheduling_key,
       task_id,
       task_name,
       is_spillback,
       raylet_address = *raylet_address](const Status &status,
                                         const rpc::RequestWorkerLeaseReply &reply) {
        std::deque<TaskSpecification> tasks_to_fail;
        rpc::RayErrorInfo error_info;
        ray::Status error_status;
        rpc::ErrorType error_type = rpc::ErrorType::WORKER_DIED;
        {
          absl::MutexLock lock(&mu_);

          auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
          auto lease_client = GetOrConnectLeaseClient(&raylet_address);
          scheduling_key_entry.pending_lease_requests.erase(task_id);

          if (status.ok()) {
            if (reply.canceled()) {
              RAY_LOG(DEBUG) << "Lease canceled for task: " << task_id
                             << ", canceled type: "
                             << rpc::RequestWorkerLeaseReply::SchedulingFailureType_Name(
                                    reply.failure_type());
              if (reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::
                          SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED ||
                  reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::
                          SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED ||
                  reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE) {
                // We need to actively fail all of the pending tasks in the queue when the
                // placement group was removed or the runtime env failed to be set up.
                // Such an operation is straightforward for the scenario of placement
                // group removal as all tasks in the queue are associated with the same
                // placement group, but in the case of runtime env setup failed, This
                // makes an implicit assumption that runtime_env failures are not
                // transient -- we may consider adding some retries in the future.
                if (reply.failure_type() ==
                    rpc::RequestWorkerLeaseReply::
                        SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED) {
                  error_type = rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED;
                  error_info.mutable_runtime_env_setup_failed_error()->set_error_message(
                      reply.scheduling_failure_message());
                } else if (reply.failure_type() ==
                           rpc::RequestWorkerLeaseReply::
                               SCHEDULING_CANCELLED_UNSCHEDULABLE) {
                  error_type = rpc::ErrorType::TASK_UNSCHEDULABLE_ERROR;
                  *(error_info.mutable_error_message()) =
                      reply.scheduling_failure_message();
                } else {
                  error_type = rpc::ErrorType::TASK_PLACEMENT_GROUP_REMOVED;
                }
                tasks_to_fail = std::move(scheduling_key_entry.task_queue);
                scheduling_key_entry.task_queue.clear();
                if (scheduling_key_entry.CanDelete()) {
                  scheduling_key_entries_.erase(scheduling_key);
                }
              } else {
                RequestNewWorkerIfNeeded(scheduling_key);
              }
            } else if (reply.rejected()) {
              RAY_LOG(DEBUG) << "Lease rejected " << task_id;
              // It might happen when the first raylet has a stale view
              // of the spillback raylet resources.
              // Retry the request at the first raylet since the resource view may be
              // refreshed.
              RAY_CHECK(is_spillback);
              RequestNewWorkerIfNeeded(scheduling_key);
            } else if (!reply.worker_address().raylet_id().empty()) {
              // We got a lease for a worker. Add the lease client state and try to
              // assign work to the worker.
              rpc::WorkerAddress addr(reply.worker_address());
              RAY_LOG(DEBUG) << "Lease granted to task " << task_id << " from raylet "
                             << addr.raylet_id << " with worker " << addr.worker_id;

              auto resources_copy = reply.resource_mapping();

              AddWorkerLeaseClient(
                  addr, std::move(lease_client), resources_copy, scheduling_key, task_id);
              RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
              OnWorkerIdle(addr,
                           scheduling_key,
                           /*error=*/false,
                           /*worker_exiting=*/false,
                           resources_copy);
            } else {
              // The raylet redirected us to a different raylet to retry at.
              RAY_CHECK(!is_spillback);
              RAY_LOG(DEBUG) << "Redirect lease for task " << task_id << " from raylet "
                             << NodeID::FromBinary(raylet_address.raylet_id())
                             << " to raylet "
                             << NodeID::FromBinary(
                                    reply.retry_at_raylet_address().raylet_id());

              RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
            }
          } else if (lease_client != local_lease_client_) {
            // A lease request to a remote raylet failed. Retry locally if the lease is
            // still needed.
            // TODO(swang): Fail after some number of retries?
            RAY_LOG_EVERY_MS(INFO, 30 * 1000)
                << "Retrying attempt to schedule task (id: " << task_id
                << " name: " << task_name
                << ") at remote node (id: " << raylet_address.raylet_id()
                << " ip: " << raylet_address.ip_address()
                << "). Try again "
                   "on a local node. Error: "
                << status.ToString();

            RequestNewWorkerIfNeeded(scheduling_key);

          } else {
            if (status.IsGrpcUnavailable()) {
              RAY_LOG(WARNING)
                  << "The worker failed to receive a response from the local "
                  << "raylet because the raylet is unavailable (crashed). "
                  << "Error: " << status;
              if (worker_type_ == WorkerType::WORKER) {
                // Exit the worker so that caller can retry somewhere else.
                RAY_LOG(WARNING) << "Terminating the worker due to local raylet death";
                QuickExit();
              }
              RAY_CHECK(worker_type_ == WorkerType::DRIVER);
              error_type = rpc::ErrorType::LOCAL_RAYLET_DIED;
              error_status = status;
              tasks_to_fail = std::move(scheduling_key_entry.task_queue);
              scheduling_key_entry.task_queue.clear();
              if (scheduling_key_entry.CanDelete()) {
                scheduling_key_entries_.erase(scheduling_key);
              }
            } else {
              RAY_LOG(WARNING)
                  << "The worker failed to receive a response from the local raylet, but "
                     "raylet is still alive. Try again on a local node. Error: "
                  << status;
              // TODO(sang): Maybe we should raise FATAL error if it happens too many
              // times.
              RequestNewWorkerIfNeeded(scheduling_key);
            }
          }
        }

        while (!tasks_to_fail.empty()) {
          auto &task_spec = tasks_to_fail.front();
          if (task_spec.IsActorCreationTask() &&
              error_type == rpc::ErrorType::TASK_PLACEMENT_GROUP_REMOVED) {
            RAY_UNUSED(task_finisher_->FailPendingTask(
                task_spec.TaskId(),
                rpc::ErrorType::ACTOR_PLACEMENT_GROUP_REMOVED,
                &error_status,
                &error_info));
          } else {
            RAY_UNUSED(task_finisher_->FailPendingTask(
                task_spec.TaskId(), error_type, &error_status, &error_info));
          }
          tasks_to_fail.pop_front();
        }
      },
      task_queue.size(),
      is_selected_based_on_locality);
  scheduling_key_entry.pending_lease_requests.emplace(task_id, *raylet_address);
  ReportWorkerBacklogIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(
    const rpc::WorkerAddress &addr,
    rpc::CoreWorkerClientInterface &client,
    const SchedulingKey &scheduling_key,
    const TaskSpecification &task_spec,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  RAY_LOG(DEBUG) << "Pushing task " << task_spec.TaskId() << " to worker "
                 << addr.worker_id << " of raylet " << addr.raylet_id;
  auto task_id = task_spec.TaskId();
  auto request = std::make_unique<rpc::PushTaskRequest>();
  bool is_actor = task_spec.IsActorTask();
  bool is_actor_creation = task_spec.IsActorCreationTask();

  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request->mutable_resource_mapping()->CopyFrom(assigned_resources);
  request->set_intended_worker_id(addr.worker_id.Binary());
  task_finisher_->MarkTaskWaitingForExecution(task_id, addr.raylet_id);
  client.PushNormalTask(
      std::move(request),
      [this,
       task_spec,
       task_id,
       is_actor,
       is_actor_creation,
       scheduling_key,
       addr,
       assigned_resources](Status status, const rpc::PushTaskReply &reply) {
        {
          RAY_LOG(DEBUG) << "Task " << task_id << " finished from worker "
                         << addr.worker_id << " of raylet " << addr.raylet_id;
          absl::MutexLock lock(&mu_);
          executing_tasks_.erase(task_id);

          // Decrement the number of tasks in flight to the worker
          auto &lease_entry = worker_to_lease_entry_[addr];
          RAY_CHECK(lease_entry.is_busy);
          lease_entry.is_busy = false;

          // Decrement the total number of tasks in flight to any worker with the current
          // scheduling_key.
          auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
          RAY_CHECK_GE(scheduling_key_entry.active_workers.size(), 1u);
          RAY_CHECK_GE(scheduling_key_entry.num_busy_workers, 1u);
          scheduling_key_entry.num_busy_workers--;

          if (!status.ok()) {
            RAY_LOG(DEBUG) << "Getting error from raylet for task " << task_id;
            const ray::rpc::ClientCallback<ray::rpc::GetTaskFailureCauseReply> callback =
                [this, status, is_actor, task_id, addr](
                    const Status &get_task_failure_cause_reply_status,
                    const rpc::GetTaskFailureCauseReply &get_task_failure_cause_reply) {
                  HandleGetTaskFailureCause(status,
                                            is_actor,
                                            task_id,
                                            addr,
                                            get_task_failure_cause_reply_status,
                                            get_task_failure_cause_reply);
                };
            auto &lease_entry = worker_to_lease_entry_[addr];
            RAY_CHECK(lease_entry.lease_client);
            lease_entry.lease_client->GetTaskFailureCause(lease_entry.task_id, callback);
          }

          if (!status.ok() || !is_actor_creation || reply.worker_exiting()) {
            // Successful actor creation leases the worker indefinitely from the raylet.
            OnWorkerIdle(addr,
                         scheduling_key,
                         /*error=*/!status.ok(),
                         /*worker_exiting=*/reply.worker_exiting(),
                         assigned_resources);
          }
        }
        if (status.ok()) {
          if (reply.was_cancelled_before_running()) {
            RAY_LOG(DEBUG) << "Task " << task_id
                           << " was cancelled before it started running.";
            RAY_UNUSED(
                task_finisher_->FailPendingTask(task_id, rpc::ErrorType::TASK_CANCELLED));
          } else if (!task_spec.GetMessage().retry_exceptions() ||
                     !reply.is_retryable_error() ||
                     !task_finisher_->RetryTaskIfPossible(
                         task_id,
                         /*task_failed_due_to_oom*/ false)) {
            task_finisher_->CompletePendingTask(
                task_id, reply, addr.ToProto(), reply.is_application_error());
          }
        }
      });
}

void CoreWorkerDirectTaskSubmitter::HandleGetTaskFailureCause(
    const Status &task_execution_status,
    const bool is_actor,
    const TaskID &task_id,
    const rpc::WorkerAddress &addr,
    const Status &get_task_failure_cause_reply_status,
    const rpc::GetTaskFailureCauseReply &get_task_failure_cause_reply) {
  rpc::ErrorType task_error_type = rpc::ErrorType::WORKER_DIED;
  std::unique_ptr<rpc::RayErrorInfo> error_info;
  bool fail_immediately = false;
  if (get_task_failure_cause_reply_status.ok()) {
    RAY_LOG(DEBUG) << "Task failure cause for task " << task_id << ": "
                   << ray::gcs::RayErrorInfoToString(
                          get_task_failure_cause_reply.failure_cause());
    if (get_task_failure_cause_reply.has_failure_cause()) {
      task_error_type = get_task_failure_cause_reply.failure_cause().error_type();
      error_info = std::make_unique<rpc::RayErrorInfo>(
          get_task_failure_cause_reply.failure_cause());
      // TODO(clarng): track and append task retry history to the error message.
    }
    fail_immediately = get_task_failure_cause_reply.fail_task_immediately();
  } else {
    RAY_LOG(DEBUG) << "Failed to fetch task result with status "
                   << get_task_failure_cause_reply_status.ToString()
                   << " node id: " << addr.raylet_id << " ip: " << addr.ip_address;
    task_error_type = rpc::ErrorType::NODE_DIED;
    std::stringstream buffer;
    buffer << "Task failed due to the node dying.\n\nThe node (IP: " << addr.ip_address
           << ", node ID: " << addr.raylet_id
           << ") where this task was running crashed unexpectedly. "
           << "This can happen if: (1) the instance where the node was running failed, "
              "(2) raylet crashes unexpectedly (OOM, preempted node, etc).\n\n"
           << "To see more information about the crash, use `ray logs raylet.out -ip "
           << addr.ip_address << "`";
    error_info = std::make_unique<rpc::RayErrorInfo>();
    error_info->set_error_message(buffer.str());
    error_info->set_error_type(rpc::ErrorType::NODE_DIED);
  }
  RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
      task_id,
      is_actor ? rpc::ErrorType::ACTOR_DIED : task_error_type,
      &task_execution_status,
      error_info.get(),
      /*mark_task_object_failed*/ true,
      fail_immediately));
}

Status CoreWorkerDirectTaskSubmitter::CancelTask(TaskSpecification task_spec,
                                                 bool force_kill,
                                                 bool recursive) {
  RAY_LOG(INFO) << "Cancelling a task: " << task_spec.TaskId()
                << " force_kill: " << force_kill << " recursive: " << recursive;
  const SchedulingKey scheduling_key(
      task_spec.GetSchedulingClass(),
      task_spec.GetDependencyIds(),
      task_spec.IsActorCreationTask() ? task_spec.ActorCreationId() : ActorID::Nil(),
      task_spec.GetRuntimeEnvHash());
  std::shared_ptr<rpc::CoreWorkerClientInterface> client = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end() ||
        !task_finisher_->MarkTaskCanceled(task_spec.TaskId())) {
      return Status::OK();
    }

    auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
    auto &scheduling_tasks = scheduling_key_entry.task_queue;
    // This cancels tasks that have completed dependencies and are awaiting
    // a worker lease.
    if (!scheduling_tasks.empty()) {
      for (auto spec = scheduling_tasks.begin(); spec != scheduling_tasks.end(); spec++) {
        if (spec->TaskId() == task_spec.TaskId()) {
          scheduling_tasks.erase(spec);

          if (scheduling_tasks.empty()) {
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
          RAY_UNUSED(task_finisher_->FailPendingTask(task_spec.TaskId(),
                                                     rpc::ErrorType::TASK_CANCELLED));
          return Status::OK();
        }
      }
    }

    // This will get removed either when the RPC call to cancel is returned
    // or when all dependencies are resolved.
    RAY_CHECK(cancelled_tasks_.emplace(task_spec.TaskId()).second);
    auto rpc_client = executing_tasks_.find(task_spec.TaskId());

    if (rpc_client == executing_tasks_.end()) {
      // This case is reached for tasks that have unresolved dependencies.
      // No executing tasks, so cancelling is a noop.
      if (scheduling_key_entry.CanDelete()) {
        // We can safely remove the entry keyed by scheduling_key from the
        // scheduling_key_entries_ hashmap.
        scheduling_key_entries_.erase(scheduling_key);
      }
      return Status::OK();
    }
    // Looks for an RPC handle for the worker executing the task.
    auto maybe_client = client_cache_->GetByID(rpc_client->second.worker_id);
    if (!maybe_client.has_value()) {
      // If we don't have a connection to that worker, we can't cancel it.
      // This case is reached for tasks that have unresolved dependencies.
      return Status::OK();
    }
    client = maybe_client.value();
  }

  RAY_CHECK(client != nullptr);

  auto request = rpc::CancelTaskRequest();
  request.set_intended_task_id(task_spec.TaskId().Binary());
  request.set_force_kill(force_kill);
  request.set_recursive(recursive);
  client->CancelTask(
      request,
      [this, task_spec, scheduling_key, force_kill, recursive](
          const Status &status, const rpc::CancelTaskReply &reply) {
        absl::MutexLock lock(&mu_);
        RAY_LOG(DEBUG) << "CancelTask RPC response received for " << task_spec.TaskId()
                       << " with status " << status.ToString();
        cancelled_tasks_.erase(task_spec.TaskId());

        // Retry is not attempted if !status.ok() because force-kill may kill the worker
        // before the reply is sent.
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Failed to cancel a task due to " << status.ToString();
          return;
        }

        if (!reply.attempt_succeeded()) {
          if (reply.requested_task_running()) {
            // Retry cancel request if failed.
            if (cancel_retry_timer_.has_value()) {
              if (cancel_retry_timer_->expiry().time_since_epoch() <=
                  std::chrono::high_resolution_clock::now().time_since_epoch()) {
                cancel_retry_timer_->expires_after(boost::asio::chrono::milliseconds(
                    RayConfig::instance().cancellation_retry_ms()));
              }
              cancel_retry_timer_->async_wait(
                  boost::bind(&CoreWorkerDirectTaskSubmitter::CancelTask,
                              this,
                              task_spec,
                              force_kill,
                              recursive));
            } else {
              RAY_LOG(DEBUG)
                  << "Failed to cancel a task which is running. Stop retrying.";
            }
          } else {
            RAY_LOG(DEBUG) << "Attempt to cancel task " << task_spec.TaskId()
                           << " in a worker that doesn't have this task.";
          }
        }
      });
  return Status::OK();
}

Status CoreWorkerDirectTaskSubmitter::CancelRemoteTask(const ObjectID &object_id,
                                                       const rpc::Address &worker_addr,
                                                       bool force_kill,
                                                       bool recursive) {
  auto maybe_client = client_cache_->GetByID(rpc::WorkerAddress(worker_addr).worker_id);

  if (!maybe_client.has_value()) {
    return Status::Invalid("No remote worker found");
  }
  auto client = maybe_client.value();
  auto request = rpc::RemoteCancelTaskRequest();
  request.set_force_kill(force_kill);
  request.set_recursive(recursive);
  request.set_remote_object_id(object_id.Binary());
  client->RemoteCancelTask(request, nullptr);
  return Status::OK();
}

}  // namespace core
}  // namespace ray
