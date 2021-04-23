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

  if (task_spec.IsActorCreationTask()) {
    // Synchronously register the actor to GCS server.
    // Previously, we asynchronously registered the actor after all its dependencies were
    // resolved. This caused a problem: if the owner of the actor dies before dependencies
    // are resolved, the actor will never be created. But the actor handle may already be
    // passed to other workers. In this case, the actor tasks will hang forever.
    // So we fixed this issue by synchronously registering the actor. If the owner dies
    // before dependencies are resolved, GCS will notice this and mark the actor as dead.
    auto status = actor_creator_->RegisterActor(task_spec);
    if (!status.ok()) {
      return status;
    }
  }

  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    RAY_LOG(DEBUG) << "Task dependencies resolved " << task_spec.TaskId();
    if (task_spec.IsActorCreationTask()) {
      // If gcs actor management is enabled, the actor creation task will be sent to
      // gcs server directly after the in-memory dependent objects are resolved. For
      // more details please see the protocol of actor management based on gcs.
      // https://docs.google.com/document/d/1EAWide-jy05akJp6OMtDn58XOK7bUyruWMia4E-fV28/edit?usp=sharing
      auto actor_id = task_spec.ActorCreationId();
      auto task_id = task_spec.TaskId();
      RAY_LOG(INFO) << "Creating actor via GCS actor id = : " << actor_id;
      RAY_CHECK_OK(actor_creator_->AsyncCreateActor(
          task_spec, [this, actor_id, task_id](Status status) {
            if (status.ok()) {
              RAY_LOG(DEBUG) << "Created actor, actor id = " << actor_id;
              task_finisher_->CompletePendingTask(task_id, rpc::PushTaskReply(),
                                                  rpc::Address());
            } else {
              RAY_LOG(ERROR) << "Failed to create actor " << actor_id
                             << " with status: " << status.ToString();
              RAY_UNUSED(task_finisher_->PendingTaskFailed(
                  task_id, rpc::ErrorType::ACTOR_CREATION_FAILED, &status));
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
        const SchedulingKey scheduling_key(
            task_spec.GetSchedulingClass(), task_spec.GetDependencyIds(),
            task_spec.IsActorCreationTask() ? task_spec.ActorCreationId()
                                            : ActorID::Nil());
        auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
        scheduling_key_entry.task_queue.push_back(task_spec);
        scheduling_key_entry.resource_spec = task_spec;

        if (!scheduling_key_entry.AllPipelinesToWorkersFull(
                max_tasks_in_flight_per_worker_)) {
          // The pipelines to the current workers are not full yet, so we don't need more
          // workers.

          // Find a worker with a number of tasks in flight that is less than the maximum
          // value (max_tasks_in_flight_per_worker_) and call OnWorkerIdle to send tasks
          // to that worker
          for (auto active_worker_addr : scheduling_key_entry.active_workers) {
            RAY_CHECK(worker_to_lease_entry_.find(active_worker_addr) !=
                      worker_to_lease_entry_.end());
            auto &lease_entry = worker_to_lease_entry_[active_worker_addr];
            if (!lease_entry.PipelineToWorkerFull(max_tasks_in_flight_per_worker_)) {
              OnWorkerIdle(active_worker_addr, scheduling_key, false,
                           lease_entry.assigned_resources);
              // If we find a worker with a non-full pipeline, all we need to do is to
              // submit the new task to the worker in question by calling OnWorkerIdle
              // once. We don't need to worry about other tasks in the queue because the
              // queue cannot have other tasks in it if there are active workers with
              // non-full pipelines.
              break;
            }
          }
        }
        RequestNewWorkerIfNeeded(scheduling_key);
      }
    }
    if (!keep_executing) {
      RAY_UNUSED(task_finisher_->PendingTaskFailed(
          task_spec.TaskId(), rpc::ErrorType::TASK_CANCELLED, nullptr));
    }
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::AddWorkerLeaseClient(
    const rpc::WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> lease_client,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
    const SchedulingKey &scheduling_key) {
  client_cache_->GetOrConnect(addr.ToProto());
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  LeaseEntry new_lease_entry = LeaseEntry(std::move(lease_client), expiration, 0, false,
                                          0, assigned_resources, scheduling_key);
  worker_to_lease_entry_.emplace(addr, new_lease_entry);

  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.emplace(addr).second);
  RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
}

void CoreWorkerDirectTaskSubmitter::ReturnWorker(const rpc::WorkerAddress addr,
                                                 bool was_error,
                                                 const SchedulingKey &scheduling_key) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
  auto &lease_entry = worker_to_lease_entry_[addr];
  RAY_CHECK(lease_entry.lease_client);
  RAY_CHECK(lease_entry.tasks_in_flight == 0);
  RAY_CHECK(lease_entry.WorkerIsStealing() == false);

  // Decrement the number of active workers consuming tasks from the queue associated
  // with the current scheduling_key
  scheduling_key_entry.active_workers.erase(addr);
  if (scheduling_key_entry.CanDelete()) {
    // We can safely remove the entry keyed by scheduling_key from the
    // scheduling_key_entries_ hashmap.
    scheduling_key_entries_.erase(scheduling_key);
  }

  auto status =
      lease_entry.lease_client->ReturnWorker(addr.port, addr.worker_id, was_error);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
  }
  worker_to_lease_entry_.erase(addr);
}

bool CoreWorkerDirectTaskSubmitter::FindOptimalVictimForStealing(
    const SchedulingKey &scheduling_key, rpc::WorkerAddress thief_addr,
    rpc::Address *victim_raw_addr) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  // Check that there is at least one worker (other than the thief) with the current
  // SchedulingKey
  if (scheduling_key_entry.active_workers.size() <= 1) {
    return false;
  }
  RAY_CHECK(scheduling_key_entry.active_workers.size() > 1);

  // Iterate through the active workers with the relevant SchedulingKey, and select the
  // best one for stealing by updating the victim_it iterator (pointing to the designated
  // victim) every time we find a candidate that is better than the incumbent. A candidate
  // is better if: (1) the incumbent victim is the thief -- because this choice would be
  // illegal (thief cannot steal from itself), so any alternative choice is better (2) the
  // candidate is not the thief (otherwise, again, it cannot be designated as the victim),
  // and it has more stealable tasks than the incumbent victim
  auto victim_it = scheduling_key_entry.active_workers.begin();

  for (auto candidate_it = scheduling_key_entry.active_workers.begin();
       candidate_it != scheduling_key_entry.active_workers.end(); candidate_it++) {
    *victim_raw_addr = victim_it->ToProto();
    rpc::WorkerAddress victim_addr = rpc::WorkerAddress(*victim_raw_addr);
    rpc::WorkerAddress candidate_addr = *candidate_it;
    RAY_CHECK(worker_to_lease_entry_.find(victim_addr) != worker_to_lease_entry_.end());
    auto &victim_entry = worker_to_lease_entry_[victim_addr];
    auto &candidate_entry = worker_to_lease_entry_[candidate_addr];

    // Update the designated victim if the alternative candidate is a better choice than
    // the incumbent victim
    if (victim_addr.worker_id == thief_addr.worker_id ||
        ((candidate_entry.tasks_in_flight > victim_entry.tasks_in_flight) &&
         candidate_addr.worker_id != thief_addr.worker_id)) {
      victim_it = candidate_it;
    }
  }
  *victim_raw_addr = victim_it->ToProto();
  rpc::WorkerAddress victim_addr = rpc::WorkerAddress(*victim_raw_addr);

  // Double check to make sure that we didn't pick the thief as the designated victim
  RAY_CHECK(!(victim_addr == thief_addr) &&
            victim_addr.worker_id != thief_addr.worker_id);

  auto &victim_entry = worker_to_lease_entry_[victim_addr];
  // Double check that the victim has the correct SchedulingKey
  RAY_CHECK(victim_entry.scheduling_key == scheduling_key);

  RAY_LOG(DEBUG) << "Victim is worker " << victim_addr.worker_id << " and has "
                 << victim_entry.tasks_in_flight << " tasks in flight, "
                 << " among which we estimate that " << victim_entry.tasks_in_flight / 2
                 << " are available for stealing";
  RAY_CHECK(scheduling_key_entry.total_tasks_in_flight >= victim_entry.tasks_in_flight);

  if ((victim_entry.tasks_in_flight / 2) < 1) {
    RAY_LOG(DEBUG) << "The designated victim does not have enough tasks to steal.";
    return false;
  }

  return true;
}

void CoreWorkerDirectTaskSubmitter::StealTasksIfNeeded(
    const rpc::WorkerAddress &thief_addr, bool was_error,
    const SchedulingKey &scheduling_key,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  // Check if work stealing is enabled
  if (max_tasks_in_flight_per_worker_ == 1 || !work_stealing_) {
    RAY_LOG(DEBUG) << "Work stealing is not enabled, so we return the worker "
                   << thief_addr.worker_id << " without stealing";
    ReturnWorker(thief_addr, was_error, scheduling_key);
    return;
  }

  RAY_LOG(DEBUG) << "Beginning to steal work now! Thief is worker: "
                 << thief_addr.worker_id;

  auto &thief_entry = worker_to_lease_entry_[thief_addr];
  // Check that the thief still retains its lease_client, and it has no tasks in flights
  RAY_CHECK(thief_entry.lease_client);
  RAY_CHECK(thief_entry.tasks_in_flight == 0);
  RAY_CHECK(thief_entry.WorkerIsStealing() == false);

  // Search for a suitable victim
  rpc::Address victim_raw_addr;
  if (!FindOptimalVictimForStealing(scheduling_key, thief_addr, &victim_raw_addr)) {
    RAY_LOG(DEBUG) << "Could not find a suitable victim for stealing! Returning worker "
                   << thief_addr.worker_id;
    ReturnWorker(thief_addr, was_error, scheduling_key);
    return;
  }
  rpc::WorkerAddress victim_addr = rpc::WorkerAddress(victim_raw_addr);
  RAY_CHECK(worker_to_lease_entry_.find(victim_addr) != worker_to_lease_entry_.end());

  thief_entry.SetWorkerIsStealing();

  // By this point, we have ascertained that the victim is available for stealing, so we
  // can go ahead with the RPC
  RAY_LOG(DEBUG) << "Executing StealTasks RPC!";
  auto request = std::unique_ptr<rpc::StealTasksRequest>(new rpc::StealTasksRequest);
  request->mutable_thief_addr()->CopyFrom(thief_addr.ToProto());
  auto &victim_client = *client_cache_->GetOrConnect(victim_addr.ToProto());
  auto victim_wid = victim_addr.worker_id;

  RAY_UNUSED(victim_client.StealTasks(
      std::move(request), [this, scheduling_key, victim_wid, victim_addr, thief_addr,
                           was_error](Status status, const rpc::StealTasksReply &reply) {
        absl::MutexLock lock(&mu_);

        // Obtain the thief's lease entry (after ensuring that it still exists)
        RAY_CHECK(worker_to_lease_entry_.find(thief_addr) !=
                  worker_to_lease_entry_.end());

        auto &thief_entry = worker_to_lease_entry_[thief_addr];
        RAY_CHECK(thief_entry.WorkerIsStealing());

        // Compute number of tasks stolen
        int64_t number_of_tasks_stolen = reply.number_of_tasks_stolen();
        int n_stolen_task_ids = reply.stolen_tasks_ids_size();
        RAY_CHECK((int64_t)n_stolen_task_ids == number_of_tasks_stolen);

        RAY_LOG(DEBUG) << "We stole " << number_of_tasks_stolen << " tasks "
                       << "from worker: " << victim_wid;

        // If we didn't steal anything, we can return the worker to the Raylet
        if (number_of_tasks_stolen == 0) {
          RAY_LOG(DEBUG) << "No tasks were actually stolen from victim: "
                         << victim_addr.worker_id;

          thief_entry.SetWorkerDoneStealing();

          if (thief_entry.tasks_in_flight == 0) {
            RAY_LOG(DEBUG)
                << "Thief " << thief_addr.worker_id
                << " has no tasks in flight now, so we return it to the Raylet!";
            ReturnWorker(thief_addr, was_error, scheduling_key);
          }
        } else {
          bool found_pseudo_thief = false;
          rpc::Address pseudo_thief = thief_addr.ToProto();

          auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

          for (int64_t i = 0; i < number_of_tasks_stolen; i++) {
            // Get the task_id of the stolen task, and obtain the corresponding task_spec
            // from the TaskManager
            TaskID stolen_task_id = TaskID::FromBinary(reply.stolen_tasks_ids(i));
            auto stolen_task_spec = *(task_finisher_->GetTaskSpec(stolen_task_id));
            assert(stolen_task_spec);

            // delete the stolen task from the executing_tasks map if it is still there.
            executing_tasks_.erase(stolen_task_id);

            // Add the task to the queue
            RAY_LOG(DEBUG) << "Adding stolen task " << stolen_task_spec.TaskId()
                           << " back to the queue (of current size="
                           << scheduling_key_entry.task_queue.size() << ")!";
            scheduling_key_entry.task_queue.push_back(stolen_task_spec);

            // Ordinarily, the thief's pipeline does not get filled between the moment
            // when stealing starts and the moment when the victim responds with the
            // stolen tasks. However, the thief's pipeline can fill if the owner's task
            // queue receives new tasks after one of its workers has started stealing, and
            // these tasks are sent to the thief. In this case, we find another worker
            // (which we will designate as a pseudo-thief) whose pipeline is not full, and
            // send the stolen tasks to that worker instead.
            if (!thief_entry.PipelineToWorkerFull(max_tasks_in_flight_per_worker_)) {
              // call OnWorkerIdle to ship the task to the thief
              OnWorkerIdle(thief_addr, scheduling_key, /*error=*/false,
                           thief_entry.assigned_resources);
            } else {
              // if the thief's pipeline is full, either find a new pseudo-thief, or send
              // the stolen task to most recently used.

              // If we have already found a pseudo thief, no need to look for another one
              if (found_pseudo_thief &&
                  worker_to_lease_entry_.find(rpc::WorkerAddress(pseudo_thief)) !=
                      worker_to_lease_entry_.end()) {
                auto &recipient_entry =
                    worker_to_lease_entry_[rpc::WorkerAddress(pseudo_thief)];
                if (!recipient_entry.PipelineToWorkerFull(
                        max_tasks_in_flight_per_worker_)) {
                  OnWorkerIdle(rpc::WorkerAddress(pseudo_thief), scheduling_key, false,
                               recipient_entry.assigned_resources);
                } else {
                  // If the pseudo-thief's pipeline has become full, we will need to look
                  // for a new one at the next iteration
                  found_pseudo_thief = false;
                }
              } else {
                // Find a worker with a number of tasks in flight that is less than the
                // maximum value (max_tasks_in_flight_per_worker_) and call OnWorkerIdle
                // to send tasks to that worker
                for (auto active_worker_addr : scheduling_key_entry.active_workers) {
                  RAY_CHECK(worker_to_lease_entry_.find(active_worker_addr) !=
                            worker_to_lease_entry_.end());
                  auto &recipient_entry = worker_to_lease_entry_[active_worker_addr];
                  if (!recipient_entry.PipelineToWorkerFull(
                          max_tasks_in_flight_per_worker_)) {
                    found_pseudo_thief = true;
                    pseudo_thief = active_worker_addr.ToProto();
                    OnWorkerIdle(active_worker_addr, scheduling_key, false,
                                 recipient_entry.assigned_resources);
                    // If we find a worker with a non-full pipeline, all we need to do is
                    // to submit the new task to the worker in question by calling
                    // OnWorkerIdle once. We don't need to worry about other tasks in the
                    // queue because the queue cannot have other tasks in it if there are
                    // active workers with non-full pipelines.
                    break;
                  }
                }
              }
            }
          }
          thief_entry.SetWorkerDoneStealing();
        }
      }));
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr, const SchedulingKey &scheduling_key, bool was_error,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto &lease_entry = worker_to_lease_entry_[addr];
  if (!lease_entry.lease_client) {
    return;
  }

  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &current_queue = scheduling_key_entry.task_queue;
  // Return the worker if there was an error executing the previous task,
  // the previous task is an actor creation task,
  // there are no more applicable queued tasks, or the lease is expired.
  if (!lease_entry.WorkerIsStealing() &&
      (was_error || current_queue.empty() ||
       current_time_ms() > lease_entry.lease_expiration_time)) {
    RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);

    // Return the worker only if there are no tasks in flight
    if (lease_entry.tasks_in_flight == 0) {
      RAY_LOG(DEBUG) << "Number of tasks in flight == 0, calling StealTasksIfNeeded!";
      StealTasksIfNeeded(addr, was_error, scheduling_key, assigned_resources);
    }
  } else {
    auto &client = *client_cache_->GetOrConnect(addr.ToProto());

    while (!current_queue.empty() &&
           !lease_entry.PipelineToWorkerFull(max_tasks_in_flight_per_worker_)) {
      auto task_spec = current_queue.front();
      // Increment the number of tasks in flight to the worker
      lease_entry.tasks_in_flight++;

      // Increment the total number of tasks in flight to any worker associated with the
      // current scheduling_key

      RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
      scheduling_key_entry.total_tasks_in_flight++;

      executing_tasks_.emplace(task_spec.TaskId(), addr);
      PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
      current_queue.pop_front();
    }

    // Delete the queue if it's now empty. Note that the queue cannot already be empty
    // because this is the only place tasks are removed from it.
    if (current_queue.empty()) {
      RAY_LOG(INFO) << "Task queue empty, canceling lease request";
      CancelWorkerLeaseIfNeeded(scheduling_key);
    }
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::CancelWorkerLeaseIfNeeded(
    const SchedulingKey &scheduling_key) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &task_queue = scheduling_key_entry.task_queue;
  if (!task_queue.empty()) {
    // There are still pending tasks, so let the worker lease request succeed.
    return;
  }

  auto &pending_lease_request = scheduling_key_entry.pending_lease_request;
  if (pending_lease_request.first) {
    // There is an in-flight lease request. Cancel it.
    auto &lease_client = pending_lease_request.first;
    auto &lease_id = pending_lease_request.second;
    RAY_LOG(DEBUG) << "Canceling lease request " << lease_id;
    lease_client->CancelWorkerLease(
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
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &pending_lease_request = scheduling_key_entry.pending_lease_request;

  if (pending_lease_request.first) {
    // There's already an outstanding lease request for this type of task.
    return;
  }

  // Check whether we really need a new worker or whether we have
  // enough room in an existing worker's pipeline to send the new tasks. If the pipelines
  // are not full, we do not request a new worker (unless work stealing is enabled, in
  // which case we can request a worker under the Eager Worker Requesting mode)
  if (!scheduling_key_entry.AllPipelinesToWorkersFull(max_tasks_in_flight_per_worker_) &&
      !work_stealing_) {
    // The pipelines to the current workers are not full yet, so we don't need more
    // workers.
    return;
  }

  auto &task_queue = scheduling_key_entry.task_queue;
  // Check if the task queue is empty. If that is the case, it only makes sense to
  // consider requesting a new worker if work stealing is enabled, and there is at least a
  // worker with stealable tasks
  if (task_queue.empty()) {
    if (!work_stealing_ || scheduling_key_entry.total_tasks_in_flight <=
                               scheduling_key_entry.active_workers.size()) {
      if (scheduling_key_entry.CanDelete()) {
        // We can safely remove the entry keyed by scheduling_key from the
        // scheduling_key_entries_ hashmap.
        scheduling_key_entries_.erase(scheduling_key);
      }
      return;
    }
  }

  // Create a TaskSpecification with an overwritten TaskID to make sure we don't reuse the
  // same TaskID to request a worker
  auto resource_spec_msg = scheduling_key_entry.resource_spec.GetMutableMessage();
  resource_spec_msg.set_task_id(TaskID::ForFakeTask().Binary());
  TaskSpecification resource_spec = TaskSpecification(resource_spec_msg);

  rpc::Address best_node_address;
  if (raylet_address == nullptr) {
    // If no raylet address is given, find the best worker for our next lease request.
    best_node_address = lease_policy_->GetBestNodeForTask(resource_spec);
    raylet_address = &best_node_address;
  }

  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  TaskID task_id = resource_spec.TaskId();
  // Subtract 1 so we don't double count the task we are requesting for.
  int64_t queue_size = task_queue.size() - 1;

  lease_client->RequestWorkerLease(
      resource_spec,
      [this, scheduling_key](const Status &status,
                             const rpc::RequestWorkerLeaseReply &reply) {
        absl::MutexLock lock(&mu_);

        auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
        auto &pending_lease_request = scheduling_key_entry.pending_lease_request;
        RAY_CHECK(pending_lease_request.first);
        auto lease_client = std::move(pending_lease_request.first);
        const auto task_id = pending_lease_request.second;
        pending_lease_request = std::make_pair(nullptr, TaskID::Nil());

        if (status.ok()) {
          if (reply.canceled()) {
            RAY_LOG(DEBUG) << "Lease canceled " << task_id;
            RequestNewWorkerIfNeeded(scheduling_key);
          } else if (!reply.worker_address().raylet_id().empty()) {
            // We got a lease for a worker. Add the lease client state and try to
            // assign work to the worker.
            RAY_LOG(DEBUG) << "Lease granted " << task_id;
            rpc::WorkerAddress addr(reply.worker_address());

            auto resources_copy = reply.resource_mapping();

            AddWorkerLeaseClient(addr, std::move(lease_client), resources_copy,
                                 scheduling_key);
            RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
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
          RAY_LOG(ERROR) << "The worker failed to receive a response from the local "
                            "raylet. This is most "
                            "likely because the local raylet has crahsed.";
          RAY_LOG(FATAL) << status.ToString();
        }
      },
      queue_size);
  pending_lease_request = std::make_pair(lease_client, task_id);
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(
    const rpc::WorkerAddress &addr, rpc::CoreWorkerClientInterface &client,
    const SchedulingKey &scheduling_key, const TaskSpecification &task_spec,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
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
  client.PushNormalTask(
      std::move(request),
      [this, task_spec, task_id, is_actor, is_actor_creation, scheduling_key, addr,
       assigned_resources](Status status, const rpc::PushTaskReply &reply) {
        {
          absl::MutexLock lock(&mu_);
          executing_tasks_.erase(task_id);

          // Decrement the number of tasks in flight to the worker
          auto &lease_entry = worker_to_lease_entry_[addr];
          RAY_CHECK(lease_entry.tasks_in_flight > 0);
          lease_entry.tasks_in_flight--;

          // Decrement the total number of tasks in flight to any worker with the current
          // scheduling_key.
          auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
          RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
          RAY_CHECK(scheduling_key_entry.total_tasks_in_flight >= 1);
          scheduling_key_entry.total_tasks_in_flight--;

          if (reply.worker_exiting()) {
            RAY_LOG(DEBUG) << "Worker " << addr.worker_id
                           << " replied that it is exiting.";
            // The worker is draining and will shutdown after it is done. Don't return
            // it to the Raylet since that will kill it early.
            worker_to_lease_entry_.erase(addr);
            auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
            scheduling_key_entry.active_workers.erase(addr);
            if (scheduling_key_entry.CanDelete()) {
              // We can safely remove the entry keyed by scheduling_key from the
              // scheduling_key_entries_ hashmap.
              scheduling_key_entries_.erase(scheduling_key);
            }
          } else if (!reply.task_stolen() && (!status.ok() || !is_actor_creation)) {
            // Successful actor creation leases the worker indefinitely from the raylet.
            OnWorkerIdle(addr, scheduling_key,
                         /*error=*/!status.ok(), assigned_resources);
          }

          if (reply.task_stolen()) {
            return;
          }
        }
        if (!status.ok()) {
          // TODO: It'd be nice to differentiate here between process vs node
          // failure (e.g., by contacting the raylet). If it was a process
          // failure, it may have been an application-level error and it may
          // not make sense to retry the task.
          RAY_UNUSED(task_finisher_->PendingTaskFailed(
              task_id,
              is_actor ? rpc::ErrorType::ACTOR_DIED : rpc::ErrorType::WORKER_DIED,
              &status));
        } else {
          task_finisher_->CompletePendingTask(task_id, reply, addr.ToProto());
        }
      });
}

Status CoreWorkerDirectTaskSubmitter::CancelTask(TaskSpecification task_spec,
                                                 bool force_kill, bool recursive) {
  RAY_LOG(INFO) << "Killing task: " << task_spec.TaskId();
  const SchedulingKey scheduling_key(
      task_spec.GetSchedulingClass(), task_spec.GetDependencyIds(),
      task_spec.IsActorCreationTask() ? task_spec.ActorCreationId() : ActorID::Nil());
  std::shared_ptr<rpc::CoreWorkerClientInterface> client = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end() ||
        !task_finisher_->MarkTaskCanceled(task_spec.TaskId())) {
      return Status::OK();
    }

    auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
    auto &scheduled_tasks = scheduling_key_entry.task_queue;
    // This cancels tasks that have completed dependencies and are awaiting
    // a worker lease.
    if (!scheduled_tasks.empty()) {
      for (auto spec = scheduled_tasks.begin(); spec != scheduled_tasks.end(); spec++) {
        if (spec->TaskId() == task_spec.TaskId()) {
          scheduled_tasks.erase(spec);

          if (scheduled_tasks.empty()) {
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
          RAY_UNUSED(task_finisher_->PendingTaskFailed(
              task_spec.TaskId(), rpc::ErrorType::TASK_CANCELLED, nullptr));
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
      request, [this, task_spec, scheduling_key, force_kill, recursive](
                   const Status &status, const rpc::CancelTaskReply &reply) {
        absl::MutexLock lock(&mu_);
        cancelled_tasks_.erase(task_spec.TaskId());

        if (status.ok() && !reply.attempt_succeeded()) {
          if (cancel_retry_timer_.has_value()) {
            if (cancel_retry_timer_->expiry().time_since_epoch() <=
                std::chrono::high_resolution_clock::now().time_since_epoch()) {
              cancel_retry_timer_->expires_after(boost::asio::chrono::milliseconds(
                  RayConfig::instance().cancellation_retry_ms()));
            }
            cancel_retry_timer_->async_wait(
                boost::bind(&CoreWorkerDirectTaskSubmitter::CancelTask, this, task_spec,
                            force_kill, recursive));
          }
        }
        // Retry is not attempted if !status.ok() because force-kill may kill the worker
        // before the reply is sent.
      });
  return Status::OK();
}

Status CoreWorkerDirectTaskSubmitter::CancelRemoteTask(const ObjectID &object_id,
                                                       const rpc::Address &worker_addr,
                                                       bool force_kill, bool recursive) {
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

};  // namespace ray
