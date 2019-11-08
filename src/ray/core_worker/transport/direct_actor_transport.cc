#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    boost::asio::io_service &io_service,
    std::unique_ptr<CoreWorkerMemoryStoreProvider> store_provider)
    : io_service_(io_service),
      client_call_manager_(io_service),
      in_memory_store_(std::move(store_provider)) {}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(
    const TaskSpecification &task_spec) {
  RAY_LOG(DEBUG) << "Submitting task " << task_spec.TaskId();

  RAY_CHECK(task_spec.IsActorTask());
  const auto &actor_id = task_spec.ActorId();

  const auto task_id = task_spec.TaskId();
  const auto num_returns = task_spec.NumReturns();

  auto request = std::unique_ptr<rpc::DirectActorAssignTaskRequest>(
      new rpc::DirectActorAssignTaskRequest);
  request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());

  std::unique_lock<std::mutex> guard(mutex_);

  auto iter = actor_states_.find(actor_id);
  if (iter == actor_states_.end() ||
      iter->second.state_ == ActorTableData::RECONSTRUCTING) {
    // Actor is not yet created, or is being reconstructed, cache the request
    // and submit after actor is alive.
    // TODO(zhijunfu): it might be possible for a user to specify an invalid
    // actor handle (e.g. from unpickling), in that case it might be desirable
    // to have a timeout to mark it as invalid if it doesn't show up in the
    // specified time.
    pending_requests_[actor_id].emplace_back(std::move(request));
    RAY_LOG(DEBUG) << "Actor " << actor_id << " is not yet created.";
  } else if (iter->second.state_ == ActorTableData::ALIVE) {
    // Actor is alive, submit the request.
    if (rpc_clients_.count(actor_id) == 0) {
      // If rpc client is not available, then create it.
      ConnectAndSendPendingTasks(actor_id, iter->second.location_.first,
                                 iter->second.location_.second);
    }

    // Submit request.
    auto &client = rpc_clients_[actor_id];
    DirectActorAssignTask(*client, std::move(request), actor_id, task_id, num_returns);
  } else {
    // Actor is dead, treat the task as failure.
    RAY_CHECK(iter->second.state_ == ActorTableData::DEAD);
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
  }

  // If the task submission subsequently fails, then the client will receive
  // the error in a callback.
  return Status::OK();
}

void CoreWorkerDirectActorTaskSubmitter::HandleActorUpdate(
    const ActorID &actor_id, const ActorTableData &actor_data) {
  std::unique_lock<std::mutex> guard(mutex_);
  actor_states_.erase(actor_id);
  actor_states_.emplace(
      actor_id,
      ActorStateData(actor_data.state(), actor_data.ip_address(), actor_data.port()));

  if (actor_data.state() == ActorTableData::ALIVE) {
    // Check if this actor is the one that we're interested, if we already have
    // a connection to the actor, or have pending requests for it, we should
    // create a new connection.
    if (pending_requests_.count(actor_id) > 0 && rpc_clients_.count(actor_id) == 0) {
      ConnectAndSendPendingTasks(actor_id, actor_data.ip_address(), actor_data.port());
    }
  } else {
    // Remove rpc client if it's dead or being reconstructed.
    rpc_clients_.erase(actor_id);

    // For tasks that have been sent and are waiting for replies, treat them
    // as failed when the destination actor is dead or reconstructing.
    auto iter = waiting_reply_tasks_.find(actor_id);
    if (iter != waiting_reply_tasks_.end()) {
      for (const auto &entry : iter->second) {
        const auto &task_id = entry.first;
        const auto num_returns = entry.second;
        TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
      }
      waiting_reply_tasks_.erase(actor_id);
    }

    // If there are pending requests, treat the pending tasks as failed.
    auto pending_it = pending_requests_.find(actor_id);
    if (pending_it != pending_requests_.end()) {
      for (const auto &request : pending_it->second) {
        TreatTaskAsFailed(TaskID::FromBinary(request->task_spec().task_id()),
                          request->task_spec().num_returns(), rpc::ErrorType::ACTOR_DIED);
      }
      pending_requests_.erase(pending_it);
    }
  }
}

void CoreWorkerDirectActorTaskSubmitter::ConnectAndSendPendingTasks(
    const ActorID &actor_id, std::string ip_address, int port) {
  std::shared_ptr<rpc::WorkerTaskClient> grpc_client =
      std::make_shared<rpc::WorkerTaskClient>(ip_address, port, client_call_manager_);
  RAY_CHECK(rpc_clients_.emplace(actor_id, std::move(grpc_client)).second);

  // Submit all pending requests.
  auto &client = rpc_clients_[actor_id];
  auto &requests = pending_requests_[actor_id];
  while (!requests.empty()) {
    auto request = std::move(requests.front());
    auto num_returns = request->task_spec().num_returns();
    auto task_id = TaskID::FromBinary(request->task_spec().task_id());
    DirectActorAssignTask(*client, std::move(request), actor_id, task_id, num_returns);
    requests.pop_front();
  }
}

void CoreWorkerDirectActorTaskSubmitter::DirectActorAssignTask(
    rpc::WorkerTaskClient &client,
    std::unique_ptr<rpc::DirectActorAssignTaskRequest> request, const ActorID &actor_id,
    const TaskID &task_id, int num_returns) {
  RAY_LOG(DEBUG) << "Pushing task " << task_id << " to actor " << actor_id;
  waiting_reply_tasks_[actor_id].insert(std::make_pair(task_id, num_returns));

  auto status = client.DirectActorAssignTask(
      std::move(request),
      [this, actor_id, task_id, num_returns](
          Status status, const rpc::DirectActorAssignTaskReply &reply) {
        {
          std::unique_lock<std::mutex> guard(mutex_);
          waiting_reply_tasks_[actor_id].erase(task_id);
        }
        if (!status.ok()) {
          // Note that this might be the __ray_terminate__ task, so we don't log
          // loudly with ERROR here.
          RAY_LOG(INFO) << "Task failed with error: " << status;
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
          return;
        }
        for (int i = 0; i < reply.return_objects_size(); i++) {
          const auto &return_object = reply.return_objects(i);
          ObjectID object_id = ObjectID::FromBinary(return_object.object_id());

          if (return_object.in_plasma()) {
            // Mark it as in plasma with a dummy object.
            std::string meta =
                std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
            auto metadata =
                const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
            auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
            RAY_CHECK_OK(
                in_memory_store_->Put(RayObject(nullptr, meta_buffer), object_id));
          } else {
            std::shared_ptr<LocalMemoryBuffer> data_buffer;
            if (return_object.data().size() > 0) {
              data_buffer = std::make_shared<LocalMemoryBuffer>(
                  const_cast<uint8_t *>(
                      reinterpret_cast<const uint8_t *>(return_object.data().data())),
                  return_object.data().size());
            }
            std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
            if (return_object.metadata().size() > 0) {
              metadata_buffer = std::make_shared<LocalMemoryBuffer>(
                  const_cast<uint8_t *>(
                      reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
                  return_object.metadata().size());
            }
            RAY_CHECK_OK(in_memory_store_->Put(RayObject(data_buffer, metadata_buffer),
                                               object_id));
          }
        }
      });
  if (!status.ok()) {
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
  }
}

void CoreWorkerDirectActorTaskSubmitter::TreatTaskAsFailed(
    const TaskID &task_id, int num_returns, const rpc::ErrorType &error_type) {
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT_ACTOR));
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    RAY_CHECK_OK(in_memory_store_->Put(RayObject(nullptr, meta_buffer), object_id));
  }
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  std::unique_lock<std::mutex> guard(mutex_);

  auto iter = actor_states_.find(actor_id);
  return (iter != actor_states_.end() && iter->second.state_ == ActorTableData::ALIVE);
}

CoreWorkerDirectActorTaskReceiver::CoreWorkerDirectActorTaskReceiver(
    WorkerContext &worker_context, boost::asio::io_service &main_io_service,
    rpc::GrpcServer &server, const TaskHandler &task_handler,
    const std::function<void()> &exit_handler)
    : worker_context_(worker_context),
      task_handler_(task_handler),
      exit_handler_(exit_handler),
      task_main_io_service_(main_io_service) {}

void CoreWorkerDirectActorTaskReceiver::Init(RayletClient &raylet_client) {
  waiter_.reset(new DependencyWaiterImpl(raylet_client));
}

void CoreWorkerDirectActorTaskReceiver::SetMaxActorConcurrency(int max_concurrency) {
  if (max_concurrency != max_concurrency_) {
    RAY_LOG(INFO) << "Creating new thread pool of size " << max_concurrency;
    RAY_CHECK(pool_ == nullptr) << "Cannot change max concurrency at runtime.";
    pool_.reset(new BoundedExecutor(max_concurrency));
    max_concurrency_ = max_concurrency;
  }
}

void CoreWorkerDirectActorTaskReceiver::HandleDirectActorAssignTask(
    const rpc::DirectActorAssignTaskRequest &request,
    rpc::DirectActorAssignTaskReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(waiter_ != nullptr) << "Must call init() prior to use";
  const TaskSpecification task_spec(request.task_spec());
  RAY_LOG(DEBUG) << "Received task " << task_spec.TaskId();
  if (task_spec.IsActorTask() && !worker_context_.CurrentActorUseDirectCall()) {
    send_reply_callback(Status::Invalid("This actor doesn't accept direct calls."),
                        nullptr, nullptr);
    return;
  }
  SetMaxActorConcurrency(worker_context_.CurrentActorMaxConcurrency());

  // TODO(ekl) resolving object dependencies is expensive and requires an IPC to
  // the raylet, which is a central bottleneck. In the future, we should inline
  // dependencies that are small and already known to be local to the client.
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < task_spec.NumArgs(); ++i) {
    int count = task_spec.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(task_spec.ArgId(i, j));
    }
  }

  auto it = scheduling_queue_.find(task_spec.CallerId());
  if (it == scheduling_queue_.end()) {
    auto result = scheduling_queue_.emplace(
        task_spec.CallerId(), std::unique_ptr<SchedulingQueue>(new SchedulingQueue(
                                  task_main_io_service_, *waiter_, pool_)));
    it = result.first;
  }
  it->second->Add(
      request.sequence_number(), request.client_processed_up_to(),
      [this, reply, send_reply_callback, task_spec]() {
        auto num_returns = task_spec.NumReturns();
        RAY_CHECK(task_spec.IsActorCreationTask() || task_spec.IsActorTask());
        RAY_CHECK(num_returns > 0);
        // Decrease to account for the dummy object id.
        num_returns--;

        // TODO(edoakes): resource IDs are currently kept track of in the raylet,
        // need to come up with a solution for this.
        ResourceMappingType resource_ids;
        std::vector<std::shared_ptr<RayObject>> return_objects;
        auto status = task_handler_(task_spec, resource_ids, &return_objects);
        if (status.IsSystemExit()) {
          // In Python, SystemExit can only be raised on the main thread. To work
          // around this when we are executing tasks on worker threads, we re-post the
          // exit event explicitly on the main thread.
          task_main_io_service_.post([this]() { exit_handler_(); });
          return;
        }
        RAY_CHECK(return_objects.size() == num_returns)
            << return_objects.size() << "  " << num_returns;

        for (size_t i = 0; i < return_objects.size(); i++) {
          auto return_object = reply->add_return_objects();
          ObjectID id = ObjectID::ForTaskReturn(
              task_spec.TaskId(), /*index=*/i + 1,
              /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT_ACTOR));
          return_object->set_object_id(id.Binary());

          // The object is nullptr if it already existed in the object store.
          const auto &result = return_objects[i];
          if (result == nullptr || result->GetData()->IsPlasmaBuffer()) {
            return_object->set_in_plasma(true);
          } else {
            if (result->GetData() != nullptr) {
              return_object->set_data(result->GetData()->Data(),
                                      result->GetData()->Size());
            }
            if (result->GetMetadata() != nullptr) {
              return_object->set_metadata(result->GetMetadata()->Data(),
                                          result->GetMetadata()->Size());
            }
          }
        }

        send_reply_callback(status, nullptr, nullptr);
      },
      [send_reply_callback]() {
        send_reply_callback(Status::Invalid("client cancelled rpc"), nullptr, nullptr);
      },
      dependencies);
}

void CoreWorkerDirectActorTaskReceiver::HandleDirectActorCallArgWaitComplete(
    const rpc::DirectActorCallArgWaitCompleteRequest &request,
    rpc::DirectActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
  waiter_->OnWaitComplete(request.tag());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace ray
