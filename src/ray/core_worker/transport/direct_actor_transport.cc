
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

bool HasByReferenceArgs(const TaskSpecification &spec) {
  for (int i = 0; i < spec.NumArgs(); ++i) {
    if (spec.ArgIdCount(i) > 0) {
      return true;
    }
  }
  return false;
}

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    boost::asio::io_service &io_service, gcs::RedisGcsClient &gcs_client,
    CoreWorkerObjectInterface &object_interface)
    : io_service_(io_service),
      gcs_client_(gcs_client),
      client_call_manager_(io_service),
      store_provider_(
          object_interface.CreateStoreProvider(StoreProviderType::LOCAL_PLASMA)) {
  RAY_CHECK_OK(SubscribeActorTable());
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(
    const TaskSpecification &task_spec) {
  if (HasByReferenceArgs(task_spec)) {
    return Status::Invalid("direct actor call only supports by-value arguments");
  }

  RAY_CHECK(task_spec.IsActorTask());
  const auto &actor_id = task_spec.ActorId();

  const auto task_id = task_spec.TaskId();
  const auto num_returns = task_spec.NumReturns();

  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->set_task_id(task_spec.TaskId().Binary());
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());

  std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
  auto iter = actor_states_.find(actor_id);
  if (iter == actor_states_.end() || iter->second == ActorTableData::RECONSTRUCTING) {
    // Actor is not yet created, or is being reconstructing, cache the request
    // and submit after actor is alive.
    auto pending_request = std::unique_ptr<PendingTaskRequest>(
        new PendingTaskRequest(task_id, num_returns, std::move(request)));
    pending_requests_[actor_id].emplace_back(std::move(pending_request));
    return Status::OK();
  } else if (iter->second == ActorTableData::ALIVE) {
    // Actor is alive, submit the request.
    RAY_CHECK(rpc_clients_.count(actor_id) > 0);

    // Submit request.
    auto &client = rpc_clients_[actor_id];
    return PushTask(*client, *request, task_id, num_returns);
  } else {
    // Actor is dead, treat the task as failure.
    RAY_CHECK(iter->second == ActorTableData::DEAD);
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
    return Status::IOError("actor is dead or being reconstructed");
  }
}

Status CoreWorkerDirectActorTaskSubmitter::SubscribeActorTable() {
  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](const ActorID &actor_id,
                                            const ActorTableData &actor_data) {
    if (actor_data.state() == ActorTableData::ALIVE) {
      RAY_LOG(INFO) << "received notification on actor alive, actor_id: " << actor_id
                    << ", ip address: " << actor_data.ip_address()
                    << ", port: " << actor_data.port();

      // TODO(zhijunfu): later when the interface to subscribe for an individual actor
      // is available, we'll only receive notifications for interested actors.
      // Create a rpc client to the actor.
      std::unique_ptr<rpc::DirectActorClient> grpc_client(new rpc::DirectActorClient(
          actor_data.ip_address(), actor_data.port(), client_call_manager_));

      std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
      actor_states_[actor_id] = actor_data.state();
      // replace old rpc client if it exists.
      rpc_clients_[actor_id] = std::move(grpc_client);

      // Submit all pending requests.
      auto &client = rpc_clients_[actor_id];
      auto &requests = pending_requests_[actor_id];
      while (!requests.empty()) {
        const auto &request = *requests.front();
        auto status =
            PushTask(*client, *request.request_, request.task_id_, request.num_returns_);
        requests.pop_front();
      }
    } else {
      RAY_LOG(INFO) << "received notification on actor, state="
                    << static_cast<int>(actor_data.state()) << ", actor_id: " << actor_id;
      std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
      actor_states_[actor_id] = actor_data.state();
    }
  };

  return gcs_client_.Actors().AsyncSubscribe(actor_notification_callback, nullptr);
}

Status CoreWorkerDirectActorTaskSubmitter::PushTask(rpc::DirectActorClient &client,
                                                    const rpc::PushTaskRequest &request,
                                                    const TaskID &task_id,
                                                    int num_returns) {
  auto status = client.PushTask(
      request,
      [this, task_id, num_returns](Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
          return;
        }

        RAY_CHECK(reply.return_object_ids_size() == reply.return_objects_size());
        for (int i = 0; i < reply.return_object_ids_size(); i++) {
          ObjectID object_id = ObjectID::FromBinary(reply.return_object_ids(i));
          const auto &return_object = reply.return_objects(i);

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
          store_provider_->Put(RayObject(data_buffer, metadata_buffer), object_id);
        }
      });
  return status;
}

void CoreWorkerDirectActorTaskSubmitter::TreatTaskAsFailed(
    const TaskID &task_id, int num_returns, const rpc::ErrorType &error_type) {
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(task_id, i + 1);
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto data = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(data, meta.size());
    store_provider_->Put(RayObject(nullptr, meta_buffer), object_id);
  }
}

CoreWorkerDirectActorTaskReceiver::CoreWorkerDirectActorTaskReceiver(
    CoreWorkerObjectInterface &object_interface, boost::asio::io_service &io_service,
    rpc::GrpcServer &server, const TaskHandler &task_handler)
    : object_interface_(object_interface),
      task_service_(io_service, *this),
      task_handler_(task_handler) {
  server.RegisterService(task_service_);
}

void CoreWorkerDirectActorTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const TaskSpecification spec(request.task_spec());
  if (HasByReferenceArgs(spec)) {
    send_reply_callback(
        Status::Invalid("direct actor call only supports by value arguments"), nullptr,
        nullptr);
    return;
  }

  std::vector<std::shared_ptr<RayObject>> results;
  auto status = task_handler_(spec, &results);
  RAY_CHECK(results.size() == spec.NumReturns())
      << results.size() << "  " << spec.NumReturns();
  for (int i = 0; i < spec.NumReturns(); i++) {
    ObjectID id = ObjectID::ForTaskReturn(spec.TaskId(), i + 1);
    (*reply).add_return_object_ids(id.Binary());
  }

  for (int i = 0; i < results.size(); i++) {
    auto return_object = (*reply).add_return_objects();
    const auto &result = results[i];
    if (result->GetData() != nullptr) {
      return_object->set_data(result->GetData()->Data(), result->GetData()->Size());
    }
    if (result->GetMetadata() != nullptr) {
      return_object->set_metadata(result->GetMetadata()->Data(),
                                  result->GetMetadata()->Size());
    }
  }

  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace ray
