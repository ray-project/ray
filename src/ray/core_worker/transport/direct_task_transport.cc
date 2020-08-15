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
              RAY_LOG(INFO) << "Created actor, actor id = " << actor_id;
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
        auto it = task_queues_.find(scheduling_key);
        if (it == task_queues_.end()) {
          it =
              task_queues_.emplace(scheduling_key, std::deque<TaskSpecification>()).first;
        }
        it->second.push_back(task_spec);

        auto it2 = submissible_tasks_.find(scheduling_key);
        if (it2 == submissible_tasks_.end()) {
          it2 =
              submissible_tasks_.emplace(scheduling_key, std::deque<TaskSpecification>())
                  .first;
        }
        it2->second.push_back(task_spec);

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
    const SchedulingKey &scheduling_key) {
  client_cache_.GetOrConnect(addr.ToProto());
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  LeaseEntry new_lease_entry =
      LeaseEntry(std::move(lease_client), expiration, 0, scheduling_key);
  worker_to_lease_entry_.emplace(addr, new_lease_entry);
}

void CoreWorkerDirectTaskSubmitter::ReturnWorker(const rpc::WorkerAddress addr,
                                                 bool was_error) {
  /*struct timespec return_worker_time;
  clock_gettime(CLOCK_REALTIME, &return_worker_time);
  long double time_elapsed = (long double)(return_worker_time.tv_sec -
  initial_time_.tv_sec) + (long double)((return_worker_time.tv_nsec -
  initial_time_.tv_nsec) / (long double) 1000000000.0); RAY_LOG(INFO) << "RETURN_WORKER
  placeholder" << " " << time_elapsed;*/

  auto &lease_entry = worker_to_lease_entry_[addr];
  if (!lease_entry.lease_client_) {
    return;
  }
  RAY_CHECK(lease_entry.lease_client_);
  auto status =
      lease_entry.lease_client_->ReturnWorker(addr.port, addr.worker_id, was_error);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
  }
  worker_to_lease_entry_.erase(addr);
}

void CoreWorkerDirectTaskSubmitter::StealWorkIfNeeded(
    const rpc::WorkerAddress &thief_addr, bool was_error,
    const SchedulingKey &scheduling_key,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  bool work_stealing_enabled = RayConfig::instance().work_stealing_enabled();
  bool work_stealing_and_eager_workers_requesting_enabled =
      RayConfig::instance().work_stealing_and_eager_workers_requesting_enabled();

  if (!work_stealing_enabled && !work_stealing_and_eager_workers_requesting_enabled) {
    RAY_LOG(INFO) << "Work stealing is not enabled, so we return the worker "
                  << thief_addr.worker_id << " without stealing";
    ReturnWorker(thief_addr, was_error);
    return;
  }

  RAY_LOG(INFO) << "Stealing work now! Thief is worker: " << thief_addr.worker_id;

  // Look for a suitable victim
  absl::flat_hash_map<rpc::WorkerAddress, LeaseEntry>::iterator victim =
      worker_to_lease_entry_.begin();
  for (absl::flat_hash_map<rpc::WorkerAddress, LeaseEntry>::iterator candidate =
           worker_to_lease_entry_.begin();
       candidate != worker_to_lease_entry_.end(); ++candidate) {
    RAY_LOG(DEBUG) << "Current victim: " << victim->first.worker_id << " with "
                   << victim->second.tasks_in_flight_ << " tasks in flight";
    RAY_LOG(DEBUG) << "Current candidate: " << candidate->first.worker_id << " with "
                   << candidate->second.tasks_in_flight_ << " tasks in flight";

    if (candidate->second.current_scheduling_key_ != scheduling_key) {
      continue;
    }
    if (victim->second.current_scheduling_key_ != scheduling_key ||
        victim->first.worker_id == thief_addr.worker_id ||
        ((candidate->second.stealable_tasks_.size() >
          victim->second.stealable_tasks_.size()) &&
         candidate->first.worker_id != thief_addr.worker_id)) {
      victim = candidate;
    }
  }

  // Check that the victim is a suitable one
  if (victim->second.current_scheduling_key_ != scheduling_key ||
      victim->first.worker_id == thief_addr.worker_id) {
    RAY_LOG(INFO) << "Could not find a suitable victim for stealing! Returning worker "
                  << thief_addr.worker_id;

    ReturnWorker(thief_addr, was_error);
    return;
  }

  RAY_CHECK(victim->second.current_scheduling_key_ == scheduling_key);
  RAY_CHECK(victim->first.worker_id != thief_addr.worker_id);

  RAY_LOG(DEBUG) << "Victim is worker " << victim->first.worker_id << " and has "
                 << victim->second.tasks_in_flight_ << " tasks in flight.";

  // Make sure that the victim has more than 1 task in flight
  RAY_LOG(DEBUG) << "victim->second.tasks_in_flight_: " << victim->second.tasks_in_flight_
                 << " victim->second.stealable_tasks_.size(): "
                 << victim->second.stealable_tasks_.size();
  RAY_CHECK(victim->second.tasks_in_flight_ >= victim->second.stealable_tasks_.size());

  if (victim->second.stealable_tasks_.size() <= 1) {
    RAY_LOG(DEBUG)
        << "The designated victim had <= 1 tasks in flight, so we don't steal.";

    ReturnWorker(thief_addr, was_error);
    return;
  }

  auto request = std::unique_ptr<rpc::StealWorkRequest>(new rpc::StealWorkRequest);
  RAY_LOG(DEBUG)
      << "Calling Steal Work RPC! Maximum number of tasks requested (to steal) = "
      << victim->second.tasks_in_flight_;
  request->set_max_tasks_to_steal(victim->second.tasks_in_flight_);

  auto &victim_client = *client_cache_.GetOrConnect(victim->first.ToProto());
  auto victim_wid = victim->first.worker_id;
  auto victim_addr = victim->first;

  RAY_UNUSED(victim_client.StealWork(
      std::move(request),
      [this, scheduling_key, victim_wid, victim_addr, thief_addr, was_error,
       assigned_resources](Status status, const rpc::StealWorkReply &reply) {
        absl::MutexLock lock(&mu_);

        auto &victim_entry = worker_to_lease_entry_[victim_addr];

        auto &lease_entry = worker_to_lease_entry_[thief_addr];
        int number_of_tasks_stolen = reply.number_of_tasks_stolen();
        RAY_CHECK(number_of_tasks_stolen == reply.tasks_stolen_size());

        RAY_LOG(DEBUG) << "We stole " << number_of_tasks_stolen << " tasks "
                       << "from worker: " << victim_wid;
        /*struct timespec tasks_stolen_time;
        clock_gettime(CLOCK_REALTIME, &tasks_stolen_time);
        long double time_elapsed = (long double)(tasks_stolen_time.tv_sec -
        initial_time_.tv_sec) + (long double)((tasks_stolen_time.tv_nsec -
        initial_time_.tv_nsec) / (long double) 1000000000.0); RAY_LOG(INFO) <<
        "TASK_STOLEN " << number_of_tasks_stolen << " " << time_elapsed;*/

        for (int i = 0; i < reply.tasks_stolen_size(); i++) {
          const TaskSpecification task_spec(reply.tasks_stolen(i));
          RAY_LOG(DEBUG) << "Thief " << thief_addr.worker_id << " Stole task "
                         << task_spec.TaskId() << "from worker: " << victim_wid;

          const SchedulingKey scheduling_key_check(
              task_spec.GetSchedulingClass(), task_spec.GetDependencyIds(),
              task_spec.IsActorCreationTask() ? task_spec.ActorCreationId()
                                              : ActorID::Nil());
          RAY_CHECK(scheduling_key_check == scheduling_key);

          executing_tasks_.erase(task_spec.TaskId());
          victim_entry.stealable_tasks_.erase(task_spec.TaskId());

          auto &client = *client_cache_.GetOrConnect(thief_addr.ToProto());
          lease_entry.tasks_in_flight_++;  // Increment the number of tasks in flight to
                                           // the worker
          auto res = lease_entry.stealable_tasks_.insert(task_spec.TaskId());
          RAY_CHECK(res.second);
          executing_tasks_.emplace(task_spec.TaskId(), thief_addr);
          PushNormalTask(thief_addr, client, scheduling_key, task_spec,
                         assigned_resources);
        }

        if (number_of_tasks_stolen == 0) {
          ReturnWorker(thief_addr, was_error);
        }
      }));
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr, const SchedulingKey &scheduling_key, bool was_error,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto &lease_entry = worker_to_lease_entry_[addr];
  if (!lease_entry.lease_client_) {
    return;
  }
  RAY_CHECK(lease_entry.lease_client_);

  auto queue_entry = task_queues_.find(scheduling_key);
  // Return the worker if there was an error executing the previous task,
  // the previous task is an actor creation task,
  // there are no more applicable queued tasks, or the lease is expired.
  if (was_error || queue_entry == task_queues_.end() ||
      current_time_ms() > lease_entry.lease_expiration_time_) {
    /*struct timespec onw1_time;
    clock_gettime(CLOCK_REALTIME, &onw1_time);
    long double time_elapsed = (long double)(onw1_time.tv_sec - initial_time_.tv_sec) +
    (long double)((onw1_time.tv_nsec - initial_time_.tv_nsec) / (long double)
    1000000000.0); RAY_LOG(INFO) << "ONW1 placeholder" << " " << time_elapsed;*/

    // Return the worker only if there are no tasks in flight
    if (lease_entry.tasks_in_flight_ == 0) {
      RAY_LOG(DEBUG) << "Number of tasks in flight == 0, calling StealWorkIfNeeded!";
      StealWorkIfNeeded(addr, was_error, scheduling_key, assigned_resources);
    } else {
      RAY_LOG(DEBUG) << "Number of tasks in flight: " << lease_entry.tasks_in_flight_;
    }

    RequestNewWorkerIfNeeded(scheduling_key);

  } else {
    /*struct timespec onw2_time;
    clock_gettime(CLOCK_REALTIME, &onw2_time);
    long double time_elapsed = (long double)(onw2_time.tv_sec - initial_time_.tv_sec) +
    (long double)((onw2_time.tv_nsec - initial_time_.tv_nsec) / (long double)
    1000000000.0); RAY_LOG(INFO) << "ONW2 placeholder" << " " << time_elapsed;*/
    auto &client = *client_cache_.GetOrConnect(addr.ToProto());

    int tasks_submitted = 0;
    while (!queue_entry->second.empty() &&
           lease_entry.tasks_in_flight_ < max_tasks_in_flight_per_worker_) {
      auto task_spec = queue_entry->second.front();
      lease_entry
          .tasks_in_flight_++;  // Increment the number of tasks in flight to the worker

      auto res = lease_entry.stealable_tasks_.insert(task_spec.TaskId());
      RAY_CHECK(res.second);

      executing_tasks_.emplace(task_spec.TaskId(), addr);
      PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
      queue_entry->second.pop_front();

      tasks_submitted++;
    }

    // Delete the queue if it's now empty. Note that the queue cannot already be empty
    // because this is the only place tasks are removed from it.
    if (queue_entry->second.empty()) {
      task_queues_.erase(queue_entry);
      RAY_LOG(DEBUG) << "Task queue empty, canceling lease request";
      CancelWorkerLeaseIfNeeded(scheduling_key);
    }

    RequestNewWorkerIfNeeded(scheduling_key, tasks_submitted);
  }
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
    RAY_CHECK(!it->second.empty());
    auto &pending_lease_req_entry = it->second.front();

    auto &lease_client = pending_lease_req_entry.first;
    auto &lease_id = pending_lease_req_entry.second;
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
    const SchedulingKey &scheduling_key, int n_requests,
    const rpc::Address *raylet_address) {
  /*struct timespec rw_time;
  clock_gettime(CLOCK_REALTIME, &rw_time);
  long double time_elapsed = (long double)(rw_time.tv_sec - initial_time_.tv_sec) +
  (long double)((rw_time.tv_nsec - initial_time_.tv_nsec) / (long double) 1000000000.0);
  RAY_LOG(INFO) << "RW placeholder" << " " << time_elapsed;*/

  if (pending_lease_requests_.find(scheduling_key) != pending_lease_requests_.end()) {
    // There's already an outstanding lease request for this type of task.
    return;
  }

  bool work_stealing_and_eager_workers_requesting_enabled =
      RayConfig::instance().work_stealing_and_eager_workers_requesting_enabled();
  int n = work_stealing_and_eager_workers_requesting_enabled ? n_requests : 1;
  for (int request_number = 0; request_number < n; request_number++) {
    /*struct timespec requesting_worker_time;
    clock_gettime(CLOCK_REALTIME, &requesting_worker_time);
    time_elapsed = (long double)(requesting_worker_time.tv_sec - initial_time_.tv_sec) +
    (long double)((requesting_worker_time.tv_nsec - initial_time_.tv_nsec) / (long double)
    1000000000.0); RAY_LOG(INFO) << "REQUESTING_WORKER placeholder" << " " <<
    time_elapsed;*/

    auto it = task_queues_.find(scheduling_key);
    bool found = false;
    TaskSpecification candidate_task_spec;
    if (it == task_queues_.end()) {
      if (!work_stealing_and_eager_workers_requesting_enabled) {
        RAY_LOG(INFO) << "eager_workers_requesting NOT enabled, returning without "
                         "requesting new workers.";
        return;
      }

      for (auto it2 : worker_to_lease_entry_) {
        if (it2.second.current_scheduling_key_ != scheduling_key) {
          continue;
        }

        if (it2.second.stealable_tasks_.size() >= 1) {
          found = true;
          break;
        }
      }

      if (!found) {
        RAY_LOG(DEBUG) << "No need for a new worker, returning!";
        return;
      }
    }

    if (!work_stealing_and_eager_workers_requesting_enabled) {
      candidate_task_spec = it->second.front();
    } else {
      auto submissible_queue_entry = submissible_tasks_.find(scheduling_key);
      if (submissible_queue_entry != submissible_tasks_.end()) {
        candidate_task_spec = submissible_queue_entry->second.front();
      } else {
        return;
      }
    }

    auto lease_client = GetOrConnectLeaseClient(raylet_address);
    TaskSpecification &resource_spec = candidate_task_spec;
    TaskID task_id = resource_spec.TaskId();
    RAY_LOG(DEBUG) << "Lease requested " << task_id;
    // RAY_LOG(INFO) << "LEASE_REQUESTED " << task_id;

    auto submitted_task_entry = submissible_tasks_.find(scheduling_key);
    RAY_CHECK(submitted_task_entry != submissible_tasks_.end());
    submitted_task_entry->second.pop_front();
    if (submitted_task_entry->second.empty()) {
      submissible_tasks_.erase(submitted_task_entry);
    }

    lease_client->RequestWorkerLease(
        resource_spec, [this, scheduling_key](const Status &status,
                                              const rpc::RequestWorkerLeaseReply &reply) {
          absl::MutexLock lock(&mu_);

          auto it = pending_lease_requests_.find(scheduling_key);
          RAY_CHECK(it != pending_lease_requests_.end());
          RAY_CHECK(!it->second.empty());
          auto pending_lease_req_entry = std::move(it->second.front());
          it->second.pop_front();
          auto lease_client = std::move(pending_lease_req_entry.first);
          const auto task_id = pending_lease_req_entry.second;
          if (it->second.empty()) {
            pending_lease_requests_.erase(it);
          }

          if (status.ok()) {
            if (reply.canceled()) {
              RAY_LOG(DEBUG) << "Lease canceled " << task_id;
              RequestNewWorkerIfNeeded(scheduling_key);
            } else if (!reply.worker_address().raylet_id().empty()) {
              // We got a lease for a worker. Add the lease client state and try to
              // assign work to the worker.
              RAY_LOG(DEBUG) << "Lease granted " << task_id;
              rpc::WorkerAddress addr(reply.worker_address());

              /*struct timespec lease_granted_time_;
              clock_gettime(CLOCK_REALTIME, &lease_granted_time_);
              long double time_elapsed = (long double)(lease_granted_time_.tv_sec -
              initial_time_.tv_sec) + (long double)((lease_granted_time_.tv_nsec -
              initial_time_.tv_nsec) / (long double) 1000000000.0); RAY_LOG(INFO) <<
              "LEASE_GRANTED " << addr.worker_id << " " << time_elapsed;*/

              AddWorkerLeaseClient(addr, std::move(lease_client), scheduling_key);
              auto resources_copy = reply.resource_mapping();
              OnWorkerIdle(addr, scheduling_key,
                           /*error=*/false, resources_copy);
            } else {
              // The raylet redirected us to a different raylet to retry at.
              RequestNewWorkerIfNeeded(scheduling_key, 1,
                                       &reply.retry_at_raylet_address());
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
        });

    auto new_pending_lease_req_entry = std::make_pair(lease_client, task_id);

    auto ptr = pending_lease_requests_.find(scheduling_key);
    if (ptr == pending_lease_requests_.end()) {
      auto ret = pending_lease_requests_.emplace(
          scheduling_key,
          std::deque<std::pair<std::shared_ptr<WorkerLeaseInterface>, TaskID>>());
      RAY_CHECK(ret.second);
      ptr = ret.first;
    }
    ptr->second.push_back(new_pending_lease_req_entry);
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
  client.PushNormalTask(std::move(request), [this, task_id, is_actor, is_actor_creation,
                                             scheduling_key, addr, assigned_resources](
                                                Status status,
                                                const rpc::PushTaskReply &reply) {
    absl::MutexLock lock(&mu_);
    RAY_LOG(DEBUG) << "Entering PushNormalTask callback for " << task_id
                   << " wrk: " << addr.worker_id;

    // Decrement the number of tasks in flight to the worker
    auto &lease_entry = worker_to_lease_entry_[addr];
    RAY_CHECK(lease_entry.tasks_in_flight_ > 0);
    lease_entry.tasks_in_flight_--;
    lease_entry.stealable_tasks_.erase(task_id);

    bool stolen = false;
    if (!reply.task_stolen()) {
      executing_tasks_.erase(task_id);
    } else {
      RAY_LOG(DEBUG) << "PushNormalTask received TaskStolen reply! task: " << task_id
                     << " wrk: " << addr.worker_id;
      stolen = true;
    }

    if (reply.worker_exiting()) {
      // The worker is draining and will shutdown after it is done. Don't return
      // it to the Raylet since that will kill it early.
      worker_to_lease_entry_.erase(addr);
    } else if (!status.ok() || !is_actor_creation) {
      // Successful actor creation leases the worker indefinitely from the raylet.
      OnWorkerIdle(addr, scheduling_key,
                   /*error=*/!status.ok(), assigned_resources);
    }

    if (stolen) {
      return;
    }
    RAY_CHECK(stolen == false);

    if (!status.ok()) {
      // TODO: It'd be nice to differentiate here between process vs node
      // failure (e.g., by contacting the raylet). If it was a process
      // failure, it may have been an application-level error and it may
      // not make sense to retry the task.
      RAY_UNUSED(task_finisher_->PendingTaskFailed(
          task_id, is_actor ? rpc::ErrorType::ACTOR_DIED : rpc::ErrorType::WORKER_DIED,
          &status));
    } else {
      /*struct timespec complete_pending_task_time;
      clock_gettime(CLOCK_REALTIME, &complete_pending_task_time);
      long double time_elapsed = (long double)(complete_pending_task_time.tv_sec -
      initial_time_.tv_sec) + (long double)((complete_pending_task_time.tv_nsec -
      initial_time_.tv_nsec) / (long double) 1000000000.0); RAY_LOG(INFO) <<
      "COMPLETE_PENDING_TASK " << task_id << " " << time_elapsed;*/
      task_finisher_->CompletePendingTask(task_id, reply, addr.ToProto());
    }
  });
}

Status CoreWorkerDirectTaskSubmitter::CancelTask(TaskSpecification task_spec,
                                                 bool force_kill) {
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
          RAY_UNUSED(task_finisher_->PendingTaskFailed(task_spec.TaskId(),
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
      return Status::OK();
    }
    // Looks for an RPC handle for the worker executing the task.
    auto maybe_client = client_cache_.GetByID(rpc_client->second.worker_id);
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
  client->CancelTask(
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
      });
  return Status::OK();
}

Status CoreWorkerDirectTaskSubmitter::CancelRemoteTask(const ObjectID &object_id,
                                                       const rpc::Address &worker_addr,
                                                       bool force_kill) {
  auto maybe_client = client_cache_.GetByID(rpc::WorkerAddress(worker_addr).worker_id);

  if (!maybe_client.has_value()) {
    return Status::Invalid("No remote worker found");
  }
  auto client = maybe_client.value();
  auto request = rpc::RemoteCancelTaskRequest();
  request.set_force_kill(force_kill);
  request.set_remote_object_id(object_id.Binary());
  client->RemoteCancelTask(request, nullptr);
  return Status::OK();
}

};  // namespace ray
