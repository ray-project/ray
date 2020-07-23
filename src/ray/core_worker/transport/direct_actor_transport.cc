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

#include "ray/core_worker/transport/direct_actor_transport.h"

#include <thread>

#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

void CoreWorkerDirectActorTaskSubmitter::AddActorQueueIfNotExists(
    const ActorID &actor_id) {
  absl::MutexLock lock(&mu_);
  // No need to check whether the insert was successful, since it is possible
  // for this worker to have multiple references to the same actor.
  client_queues_.emplace(actor_id, ClientQueue());
}

void CoreWorkerDirectActorTaskSubmitter::KillActor(const ActorID &actor_id,
                                                   bool force_kill, bool no_restart) {
  absl::MutexLock lock(&mu_);
  rpc::KillActorRequest request;
  request.set_intended_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);

  auto it = client_queues_.find(actor_id);
  // The language frontend can only kill actors that it has a reference to.
  RAY_CHECK(it != client_queues_.end());

  if (!it->second.pending_force_kill) {
    it->second.pending_force_kill = request;
  } else if (force_kill) {
    // Overwrite the previous request to kill the actor if the new request is a
    // force kill.
    it->second.pending_force_kill->set_force_kill(true);
    if (no_restart) {
      // Overwrite the previous request to disable restart if the new request's
      // no_restart flag is set to true.
      it->second.pending_force_kill->set_no_restart(true);
    }
  }

  SendPendingTasks(actor_id);
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submitting task " << task_spec.TaskId();
  RAY_CHECK(task_spec.IsActorTask());

  bool task_queued = false;
  uint64_t send_pos = 0;
  {
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(task_spec.ActorId());
    RAY_CHECK(queue != client_queues_.end());
    if (queue->second.state != rpc::ActorTableData::DEAD) {
      // We must fix the send order prior to resolving dependencies, which may
      // complete out of order. This ensures that we will not deadlock due to
      // backpressure. The receiving actor will execute the tasks according to
      // this sequence number.
      send_pos = task_spec.ActorCounter();
      auto inserted =
          queue->second.requests.emplace(send_pos, std::make_pair(task_spec, false));
      RAY_CHECK(inserted.second);
      task_queued = true;
    }
  }

  if (task_queued) {
    const auto actor_id = task_spec.ActorId();
    // We must release the lock before resolving the task dependencies since
    // the callback may get called in the same call stack.
    resolver_.ResolveDependencies(task_spec, [this, send_pos, actor_id]() {
      absl::MutexLock lock(&mu_);
      auto queue = client_queues_.find(actor_id);
      RAY_CHECK(queue != client_queues_.end());
      auto it = queue->second.requests.find(send_pos);
      // Only dispatch tasks if the submitted task is still queued. The task
      // may have been dequeued if the actor has since failed.
      if (it != queue->second.requests.end()) {
        it->second.second = true;
        SendPendingTasks(actor_id);
      }
    });
  } else {
    // Do not hold the lock while calling into task_finisher_.
    task_finisher_->MarkTaskCanceled(task_spec.TaskId());
    auto status = Status::IOError("cancelling all pending tasks of dead actor");
    // No need to increment the number of completed tasks since the actor is
    // dead.
    RAY_UNUSED(!task_finisher_->PendingTaskFailed(task_spec.TaskId(),
                                                  rpc::ErrorType::ACTOR_DIED, &status));
  }

  // If the task submission subsequently fails, then the client will receive
  // the error in a callback.
  return Status::OK();
}

void CoreWorkerDirectActorTaskSubmitter::DisconnectRpcClient(ClientQueue &queue) {
  queue.rpc_client = nullptr;
  queue.worker_id.clear();
  queue.pending_force_kill.reset();
}

void CoreWorkerDirectActorTaskSubmitter::ConnectActor(const ActorID &actor_id,
                                                      const rpc::Address &address,
                                                      int64_t num_restarts) {
  RAY_LOG(DEBUG) << "Connecting to actor " << actor_id << " at worker "
                 << WorkerID::FromBinary(address.worker_id());
  absl::MutexLock lock(&mu_);

  auto queue = client_queues_.find(actor_id);
  RAY_CHECK(queue != client_queues_.end());
  if (num_restarts <= queue->second.num_restarts) {
    // This message is about an old version of the actor and the actor has
    // already restarted since then. Skip the connection.
    return;
  }

  if (queue->second.state == rpc::ActorTableData::DEAD) {
    // This message is about an old version of the actor and the actor has
    // already died since then. Skip the connection.
    return;
  }

  queue->second.num_restarts = num_restarts;
  if (queue->second.rpc_client) {
    // Clear the client to the old version of the actor.
    DisconnectRpcClient(queue->second);
  }

  queue->second.state = rpc::ActorTableData::ALIVE;
  // Update the mapping so new RPCs go out with the right intended worker id.
  queue->second.worker_id = address.worker_id();
  // Create a new connection to the actor.
  queue->second.rpc_client =
      std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(address));
  // TODO(swang): This assumes that all replies from the previous incarnation
  // of the actor have been received. Fix this by setting an epoch for each
  // actor task, so we can ignore completed tasks from old epochs.
  RAY_LOG(INFO) << "Resetting caller starts at for actor " << actor_id << " from "
                << queue->second.caller_starts_at << " to "
                << queue->second.num_completed_tasks;
  queue->second.caller_starts_at = queue->second.num_completed_tasks;
  SendPendingTasks(actor_id);
}

void CoreWorkerDirectActorTaskSubmitter::DisconnectActor(const ActorID &actor_id,
                                                         int64_t num_restarts,
                                                         bool dead) {
  RAY_LOG(DEBUG) << "Disconnecting from actor " << actor_id;
  absl::MutexLock lock(&mu_);
  auto queue = client_queues_.find(actor_id);
  RAY_CHECK(queue != client_queues_.end());
  if (num_restarts < queue->second.num_restarts && !dead) {
    // This message is about an old version of the actor that has already been
    // restarted successfully. Skip the message handling.
    return;
  }

  // The actor failed, so erase the client for now. Either the actor is
  // permanently dead or the new client will be inserted once the actor is
  // restarted.
  DisconnectRpcClient(queue->second);

  if (dead) {
    queue->second.state = rpc::ActorTableData::DEAD;
    // If there are pending requests, treat the pending tasks as failed.
    RAY_LOG(INFO) << "Failing pending tasks for actor " << actor_id;
    auto &requests = queue->second.requests;
    auto head = requests.begin();
    while (head != requests.end()) {
      const auto &task_spec = head->second.first;
      task_finisher_->MarkTaskCanceled(task_spec.TaskId());
      auto status = Status::IOError("cancelling all pending tasks of dead actor");
      // No need to increment the number of completed tasks since the actor is
      // dead.
      RAY_UNUSED(!task_finisher_->PendingTaskFailed(task_spec.TaskId(),
                                                    rpc::ErrorType::ACTOR_DIED, &status));
      head = requests.erase(head);
    }

    // No need to clean up tasks that have been sent and are waiting for
    // replies. They will be treated as failed once the connection dies.
    // We retain the sequencing information so that we can properly fail
    // any tasks submitted after the actor death.
  } else if (queue->second.state != rpc::ActorTableData::DEAD) {
    // Only update the actor's state if it is not permanently dead. The actor
    // will eventually get restarted or marked as permanently dead.
    queue->second.state = rpc::ActorTableData::RESTARTING;
    queue->second.num_restarts = num_restarts;
  }
}

void CoreWorkerDirectActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  if (!it->second.rpc_client) {
    return;
  }

  // Check if there is a pending force kill. If there is, send it and disconnect the
  // client.
  if (it->second.pending_force_kill) {
    RAY_LOG(INFO) << "Sending KillActor request to actor " << actor_id;
    // It's okay if this fails because this means the worker is already dead.
    RAY_UNUSED(it->second.rpc_client->KillActor(*it->second.pending_force_kill, nullptr));
    it->second.pending_force_kill.reset();
  }

  // Submit all pending requests.
  auto &requests = it->second.requests;
  auto head = requests.begin();
  while (head != requests.end() && head->first <= it->second.next_send_position &&
         head->second.second) {
    // If the task has been sent before, skip the other tasks in the send
    // queue.
    bool skip_queue = head->first < it->second.next_send_position;
    auto task_spec = std::move(head->second.first);
    head = requests.erase(head);

    RAY_CHECK(!it->second.worker_id.empty());
    PushActorTask(it->second, task_spec, skip_queue);
    it->second.next_send_position++;
  }
}

void CoreWorkerDirectActorTaskSubmitter::PushActorTask(const ClientQueue &queue,
                                                       const TaskSpecification &task_spec,
                                                       bool skip_queue) {
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest());
  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());

  request->set_intended_worker_id(queue.worker_id);
  RAY_CHECK(task_spec.ActorCounter() >= queue.caller_starts_at)
      << "actor counter " << task_spec.ActorCounter() << " " << queue.caller_starts_at;
  request->set_sequence_number(task_spec.ActorCounter() - queue.caller_starts_at);

  const auto task_id = task_spec.TaskId();
  const auto actor_id = task_spec.ActorId();
  const auto counter = task_spec.ActorCounter();
  RAY_LOG(DEBUG) << "Pushing task " << task_id << " to actor " << actor_id
                 << " actor counter " << counter << " seq no "
                 << request->sequence_number();
  rpc::Address addr(queue.rpc_client->Addr());
  RAY_UNUSED(queue.rpc_client->PushActorTask(
      std::move(request), skip_queue,
      [this, addr, task_id, actor_id](Status status, const rpc::PushTaskReply &reply) {
        bool increment_completed_tasks = true;
        if (!status.ok()) {
          bool will_retry = task_finisher_->PendingTaskFailed(
              task_id, rpc::ErrorType::ACTOR_DIED, &status);
          if (will_retry) {
            increment_completed_tasks = false;
          }
        } else {
          task_finisher_->CompletePendingTask(task_id, reply, addr);
        }

        if (increment_completed_tasks) {
          absl::MutexLock lock(&mu_);
          auto queue = client_queues_.find(actor_id);
          RAY_CHECK(queue != client_queues_.end());
          queue->second.num_completed_tasks++;
        }
      }));
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto iter = client_queues_.find(actor_id);
  return (iter != client_queues_.end() && iter->second.rpc_client);
}

void CoreWorkerDirectTaskReceiver::Init(
    rpc::ClientFactoryFn client_factory, rpc::Address rpc_address,
    std::shared_ptr<DependencyWaiter> dependency_waiter) {
  waiter_ = std::move(dependency_waiter);
  rpc_address_ = rpc_address;
  client_factory_ = client_factory;
}

void CoreWorkerDirectTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  const TaskSpecification task_spec(request.task_spec());

  // If GCS server is restarted after sending an actor creation task to this core worker,
  // the restarted GCS server will send the same actor creation task to the core worker
  // again. We just need to ignore it and reply ok.
  if (task_spec.IsActorCreationTask() &&
      worker_context_.GetCurrentActorID() == task_spec.ActorCreationId()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    RAY_LOG(INFO) << "Ignoring duplicate actor creation task for actor "
                  << task_spec.ActorCreationId()
                  << ". This is likely due to a GCS server restart.";
    return;
  }

  // Only assign resources for non-actor tasks. Actor tasks inherit the resources
  // assigned at initial actor creation time.
  std::shared_ptr<ResourceMappingType> resource_ids;
  if (!task_spec.IsActorTask()) {
    resource_ids.reset(new ResourceMappingType());
    for (const auto &mapping : request.resource_mapping()) {
      std::vector<std::pair<int64_t, double>> rids;
      for (const auto &ids : mapping.resource_ids()) {
        rids.push_back(std::make_pair(ids.index(), ids.quantity()));
      }
      (*resource_ids)[mapping.name()] = rids;
    }
  }

  auto accept_callback = [this, reply, send_reply_callback, task_spec, resource_ids]() {
    auto num_returns = task_spec.NumReturns();
    if (task_spec.IsActorCreationTask() || task_spec.IsActorTask()) {
      // Decrease to account for the dummy object id.
      num_returns--;
    }
    RAY_CHECK(num_returns >= 0);

    std::vector<std::shared_ptr<RayObject>> return_objects;
    auto status = task_handler_(task_spec, resource_ids, &return_objects,
                                reply->mutable_borrowed_refs());

    bool objects_valid = return_objects.size() == num_returns;
    if (objects_valid) {
      for (size_t i = 0; i < return_objects.size(); i++) {
        auto return_object = reply->add_return_objects();
        ObjectID id = ObjectID::ForTaskReturn(task_spec.TaskId(), /*index=*/i + 1);
        return_object->set_object_id(id.Binary());

        // The object is nullptr if it already existed in the object store.
        const auto &result = return_objects[i];
        return_object->set_size(result->GetSize());
        if (result->GetData() != nullptr && result->GetData()->IsPlasmaBuffer()) {
          return_object->set_in_plasma(true);
        } else {
          if (result->GetData() != nullptr) {
            return_object->set_data(result->GetData()->Data(), result->GetData()->Size());
          }
          if (result->GetMetadata() != nullptr) {
            return_object->set_metadata(result->GetMetadata()->Data(),
                                        result->GetMetadata()->Size());
          }
          for (const auto &nested_id : result->GetNestedIds()) {
            return_object->add_nested_inlined_ids(nested_id.Binary());
          }
        }
      }
      if (task_spec.IsActorCreationTask()) {
        RAY_LOG(INFO) << "Actor creation task finished, task_id: " << task_spec.TaskId()
                      << ", actor_id: " << task_spec.ActorCreationId();
        // Tell raylet that an actor creation task has finished execution, so that
        // raylet can publish actor creation event to GCS, and mark this worker as
        // actor, thus if this worker dies later raylet will restart the actor.
        RAY_CHECK_OK(task_done_());
      }
    }
    if (status.IsSystemExit()) {
      // Don't allow the worker to be reused, even though the reply status is OK.
      // The worker will be shutting down shortly.
      reply->set_worker_exiting(true);
      if (objects_valid) {
        // This happens when max_calls is hit. We still need to return the objects.
        send_reply_callback(Status::OK(), nullptr, nullptr);
      } else {
        send_reply_callback(status, nullptr, nullptr);
      }
    } else {
      RAY_CHECK(objects_valid) << return_objects.size() << "  " << num_returns;
      send_reply_callback(status, nullptr, nullptr);
    }
  };

  // Run actor creation task immediately on the main thread, without going
  // through a scheduling queue.
  if (task_spec.IsActorCreationTask()) {
    accept_callback();
    return;
  }

  auto reject_callback = [send_reply_callback]() {
    send_reply_callback(Status::Invalid("client cancelled stale rpc"), nullptr, nullptr);
  };

  auto it = scheduling_queue_.find(task_spec.CallerWorkerId());
  if (it == scheduling_queue_.end()) {
    auto result = scheduling_queue_.emplace(
        task_spec.CallerWorkerId(),
        SchedulingQueue(task_main_io_service_, *waiter_, worker_context_));
    it = result.first;
  }
  auto dependencies = task_spec.GetDependencies();
  // Pop the dummy actor dependency.
  if (task_spec.IsActorTask()) {
    // TODO(swang): Remove this with legacy raylet code.
    dependencies.pop_back();
  }
  it->second.Add(request.sequence_number(), request.client_processed_up_to(),
                 accept_callback, reject_callback, dependencies);
}

}  // namespace ray
