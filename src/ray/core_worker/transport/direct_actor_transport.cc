#include "ray/core_worker/transport/direct_actor_transport.h"

#include <thread>

#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

Status CoreWorkerDirectActorTaskSubmitter::KillActor(const ActorID &actor_id) {
  absl::MutexLock lock(&mu_);
  pending_force_kills_.insert(actor_id);
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
  return Status::OK();
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
  if (pending_requests_.count(actor_id) > 0) {
    SendPendingTasks(actor_id);
  }
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
  }
}

void CoreWorkerDirectActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto &client = rpc_clients_[actor_id];
  RAY_CHECK(client);
  // Check if there is a pending force kill. If there is, send it and disconnect the
  // client.
  if (pending_force_kills_.find(actor_id) != pending_force_kills_.end()) {
    rpc::KillActorRequest request;
    request.set_intended_actor_id(actor_id.Binary());
    RAY_CHECK_OK(client->KillActor(request, nullptr));
    pending_force_kills_.erase(actor_id);
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

void CoreWorkerDirectTaskReceiver::Init(rpc::ClientFactoryFn client_factory,
                                        rpc::Address rpc_address) {
  waiter_.reset(new DependencyWaiterImpl(*local_raylet_client_));
  rpc_address_ = rpc_address;
  client_factory_ = client_factory;
}

void CoreWorkerDirectTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  const TaskSpecification task_spec(request.task_spec());
  RAY_LOG(DEBUG) << "Received task " << task_spec.DebugString();
  if (task_spec.IsActorTask() && !worker_context_.CurrentTaskIsDirectCall()) {
    send_reply_callback(Status::Invalid("This actor doesn't accept direct calls."),
                        nullptr, nullptr);
    return;
  }

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
    // We have posted an exit task onto the main event loop,
    // so shouldn't bother executing any further work.
    if (exiting_) return;

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
        RAY_CHECK_OK(local_raylet_client_->TaskDone());
      }
    }
    if (status.IsSystemExit()) {
      // Don't allow the worker to be reused, even though the reply status is OK.
      // The worker will be shutting down shortly.
      reply->set_worker_exiting(true);
      // In Python, SystemExit can only be raised on the main thread. To
      // work around this when we are executing tasks on worker threads,
      // we re-post the exit event explicitly on the main thread.
      exiting_ = true;
      if (objects_valid) {
        // This happens when max_calls is hit. We still need to return the objects.
        send_reply_callback(Status::OK(), nullptr, nullptr);
      } else {
        send_reply_callback(status, nullptr, nullptr);
      }
      task_main_io_service_.post(
          [this, status]() { exit_handler_(status.IsIntentionalSystemExit()); });
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

  auto it = scheduling_queue_.find(task_spec.CallerId());
  if (it == scheduling_queue_.end()) {
    auto result = scheduling_queue_.emplace(
        task_spec.CallerId(), std::unique_ptr<SchedulingQueue>(new SchedulingQueue(
                                  task_main_io_service_, *waiter_, worker_context_)));
    it = result.first;
  }
  it->second->Add(request.sequence_number(), request.client_processed_up_to(),
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
