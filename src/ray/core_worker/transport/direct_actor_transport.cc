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

void CoreWorkerDirectActorTaskSubmitter::KillActor(const ActorID &actor_id,
                                                   bool force_kill,
                                                   bool no_reconstruction) {
  absl::MutexLock lock(&mu_);
  rpc::KillActorRequest request;
  request.set_intended_actor_id(actor_id.Binary());
  request.set_force_kill(force_kill);
  request.set_no_reconstruction(no_reconstruction);
  auto inserted = pending_force_kills_.emplace(actor_id, request);
  if (!inserted.second && force_kill) {
    // Overwrite the previous request to kill the actor if the new request is a
    // force kill.
    inserted.first->second.set_force_kill(true);
    if (no_reconstruction) {
      // Overwrite the previous request to disable reconstruction if the new request's
      // no_reconstruction flag is set to true.
      inserted.first->second.set_no_reconstruction(true);
    }
  }
  auto it = rpc_clients_.find(actor_id);
  if (it == rpc_clients_.end()) {
    // Actor is not yet created, or is being reconstructed, cache the request
    // and submit after actor is alive.
    // TODO(zhijunfu): it might be possible for a user to specify an invalid
    // actor handle (e.g. from unpickling), in that case it might be desirable
    // to have a timeout to mark it as invalid if it doesn't show up in the
    // specified time.
    RAY_LOG(DEBUG) << "Actor " << actor_id << " is not yet created.";
  } else {
    SendPendingTasks(actor_id);
  }
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submitting task " << task_spec.TaskId();
  RAY_CHECK(task_spec.IsActorTask());

  // We must fix the send order prior to resolving dependencies, which may complete
  // out of order. This ensures we preserve the client-side send order.
  int64_t send_pos = -1;
  {
    absl::MutexLock lock(&mu_);
    send_pos = next_send_position_to_assign_[task_spec.ActorId()]++;
  }

  resolver_.ResolveDependencies(task_spec, [this, send_pos, task_spec]() mutable {
    const auto &actor_id = task_spec.ActorId();

    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    request->mutable_caller_address()->CopyFrom(rpc_address_);
    // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
    // fails, then the task data will be gone when the TaskManager attempts to
    // access the task.
    request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
    request->set_caller_version(caller_creation_timestamp_ms_);

    absl::MutexLock lock(&mu_);

    auto inserted = pending_requests_[actor_id].emplace(send_pos, std::move(request));
    RAY_CHECK(inserted.second);

    auto it = rpc_clients_.find(actor_id);
    if (it == rpc_clients_.end()) {
      // Actor is not yet created, or is being reconstructed, cache the request
      // and submit after actor is alive.
      // TODO(zhijunfu): it might be possible for a user to specify an invalid
      // actor handle (e.g. from unpickling), in that case it might be desirable
      // to have a timeout to mark it as invalid if it doesn't show up in the
      // specified time.
      RAY_LOG(DEBUG) << "Actor " << actor_id << " is not yet created.";
    } else {
      SendPendingTasks(actor_id);
    }
  });

  // If the task submission subsequently fails, then the client will receive
  // the error in a callback.
  return Status::OK();
}

void CoreWorkerDirectActorTaskSubmitter::ConnectActor(const ActorID &actor_id,
                                                      const rpc::Address &address) {
  absl::MutexLock lock(&mu_);
  // Update the mapping so new RPCs go out with the right intended worker id.
  worker_ids_[actor_id] = address.worker_id();
  // Create a new connection to the actor.
  // TODO(edoakes): are these clients cleaned up properly?
  if (rpc_clients_.count(actor_id) == 0) {
    rpc_clients_[actor_id] =
        std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(address));
  }
  SendPendingTasks(actor_id);
}

void CoreWorkerDirectActorTaskSubmitter::DisconnectActor(const ActorID &actor_id,
                                                         bool dead) {
  absl::MutexLock lock(&mu_);
  if (!dead) {
    // We're reconstructing the actor, so erase the client for now. The new client
    // will be inserted once actor reconstruction completes. We don't erase the
    // client when the actor is DEAD, so that all further tasks will be failed.
    rpc_clients_.erase(actor_id);
    worker_ids_.erase(actor_id);
  } else {
    RAY_LOG(INFO) << "Failing pending tasks for actor " << actor_id;
    // If there are pending requests, treat the pending tasks as failed.
    auto pending_it = pending_requests_.find(actor_id);
    if (pending_it != pending_requests_.end()) {
      auto head = pending_it->second.begin();
      while (head != pending_it->second.end()) {
        auto request = std::move(head->second);
        head = pending_it->second.erase(head);
        auto task_id = TaskID::FromBinary(request->task_spec().task_id());
        auto status = Status::IOError("cancelling all pending tasks of dead actor");
        task_finisher_->PendingTaskFailed(task_id, rpc::ErrorType::ACTOR_DIED, &status);
      }
      pending_requests_.erase(pending_it);
    }
    // No need to clean up tasks that have been sent and are waiting for
    // replies. They will be treated as failed once the connection dies.
    // We retain the sequencing information so that we can properly fail
    // any tasks submitted after the actor death.

    pending_force_kills_.erase(actor_id);
  }
}

void CoreWorkerDirectActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto &client = rpc_clients_[actor_id];
  RAY_CHECK(client);
  // Check if there is a pending force kill. If there is, send it and disconnect the
  // client.
  auto it = pending_force_kills_.find(actor_id);
  if (it != pending_force_kills_.end()) {
    RAY_LOG(INFO) << "Sending KillActor request to actor " << actor_id;
    // It's okay if this fails because this means the worker is already dead.
    RAY_UNUSED(client->KillActor(it->second, nullptr));
    pending_force_kills_.erase(it);
  }

  // Submit all pending requests.
  auto &requests = pending_requests_[actor_id];
  auto head = requests.begin();
  while (head != requests.end() && head->first == next_send_position_[actor_id]) {
    auto request = std::move(head->second);
    head = requests.erase(head);

    auto num_returns = request->task_spec().num_returns();
    auto task_id = TaskID::FromBinary(request->task_spec().task_id());
    PushActorTask(*client, std::move(request), actor_id, task_id, num_returns);
  }
}

void CoreWorkerDirectActorTaskSubmitter::PushActorTask(
    rpc::CoreWorkerClientInterface &client, std::unique_ptr<rpc::PushTaskRequest> request,
    const ActorID &actor_id, const TaskID &task_id, int num_returns) {
  RAY_LOG(DEBUG) << "Pushing task " << task_id << " to actor " << actor_id;
  next_send_position_[actor_id]++;
  auto it = worker_ids_.find(actor_id);
  RAY_CHECK(it != worker_ids_.end()) << "Actor worker id not found " << actor_id.Hex();
  request->set_intended_worker_id(it->second);
  rpc::Address addr(client.Addr());
  RAY_CHECK_OK(client.PushActorTask(
      std::move(request),
      [this, addr, task_id](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          task_finisher_->PendingTaskFailed(task_id, rpc::ErrorType::ACTOR_DIED, &status);
        } else {
          task_finisher_->CompletePendingTask(task_id, reply, addr);
        }
      }));
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  absl::MutexLock lock(&mu_);

  auto iter = rpc_clients_.find(actor_id);
  return (iter != rpc_clients_.end());
}

void CoreWorkerDirectActorTaskSubmitter::SetCallerCreationTimestamp(int64_t timestamp) {
  caller_creation_timestamp_ms_ = timestamp;
}

void CoreWorkerDirectTaskReceiver::Init(
    rpc::ClientFactoryFn client_factory, rpc::Address rpc_address,
    std::shared_ptr<DependencyWaiterInterface> dependency_client) {
  waiter_.reset(new DependencyWaiterImpl(*dependency_client));
  rpc_address_ = rpc_address;
  client_factory_ = client_factory;
}

void CoreWorkerDirectTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  const TaskSpecification task_spec(request.task_spec());
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < task_spec.NumArgs(); ++i) {
    int count = task_spec.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(task_spec.ArgId(i, j));
    }
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
        ObjectID id = ObjectID::ForTaskReturn(
            task_spec.TaskId(), /*index=*/i + 1,
            /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
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
        // actor, thus if this worker dies later raylet will reconstruct the actor.
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

  auto caller_worker_id = WorkerID::FromBinary(request.caller_address().worker_id());
  auto caller_version = request.caller_version();
  auto it = scheduling_queue_.find(task_spec.CallerId());
  if (it != scheduling_queue_.end()) {
    if (it->second.first.caller_worker_id != caller_worker_id) {
      // We received a request with the same caller ID, but from a different worker,
      // this indicates the caller (actor) is reconstructed.
      if (it->second.first.caller_creation_timestamp_ms < caller_version) {
        // The new request has a newer caller version, then remove the old entry
        // from scheduling queue since it's invalid now.
        RAY_LOG(INFO) << "Remove existing scheduling queue for caller "
                      << task_spec.CallerId() << " after receiving a "
                      << "request from a different worker ID with a newer "
                      << "version, old worker ID: " << it->second.first.caller_worker_id
                      << ", new worker ID" << caller_worker_id;
        scheduling_queue_.erase(task_spec.CallerId());
        it = scheduling_queue_.end();
      } else {
        // The existing caller has the newer version, this indicates the request
        // is from an old caller, which might be possible when network has problems.
        // In this case fail this request.
        RAY_LOG(WARNING) << "Ignoring request from an old caller because "
                         << "it has a smaller timestamp, old worker ID: "
                         << caller_worker_id << ", current worker ID"
                         << it->second.first.caller_worker_id;
        // Fail request with an old caller version.
        reject_callback();
        return;
      }
    }
  }

  if (it == scheduling_queue_.end()) {
    SchedulingQueueTag tag;
    tag.caller_worker_id = caller_worker_id;
    tag.caller_creation_timestamp_ms = caller_version;
    auto result = scheduling_queue_.emplace(
        task_spec.CallerId(),
        std::make_pair(tag, std::unique_ptr<SchedulingQueue>(new SchedulingQueue(
                                task_main_io_service_, *waiter_, worker_context_))));
    it = result.first;
  }
  it->second.second->Add(request.sequence_number(), request.client_processed_up_to(),
                         accept_callback, reject_callback, dependencies);
}

void CoreWorkerDirectTaskReceiver::HandleDirectActorCallArgWaitComplete(
    const rpc::DirectActorCallArgWaitCompleteRequest &request,
    rpc::DirectActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
  waiter_->OnWaitComplete(request.tag());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace ray
