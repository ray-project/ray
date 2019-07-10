
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet/task.h"

using ray::rpc::ActorTableData;

namespace ray {

bool HasByReferenceArgs(const raylet::TaskSpecification &spec) {
  for (int i = 0; i < spec.NumArgs(); ++i) {
    if (spec.ArgIdCount(i) > 0) {
      return true;
    }
  }
  return false;
}

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    boost::asio::io_service &io_service,
    std::unique_ptr<gcs::GcsClient> &gcs_client,
    const std::string &store_socket)
    : io_service_(io_service),
      gcs_client_(gcs_client),
      client_call_manager_(io_service),
      counter_(0) {
  RAY_CHECK_OK(SubscribeActorTable());
  // TODO: this is a hack.
  auto status = store_client_.Connect(store_socket);
  RAY_ARROW_CHECK_OK(status);
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(const TaskSpec &task) {

  if (HasByReferenceArgs(task.GetTaskSpecification())) {
    return Status::Invalid("direct actor call only supports by value arguments");
  }

  RAY_CHECK(task.GetTaskSpecification().IsActorTask());
  const auto &actor_id = task.GetTaskSpecification().ActorId();
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->set_task_id(task.GetTaskSpecification().TaskId().Binary());
  request->set_task_spec(task.GetTaskSpecification().SpecToString());

  std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
  auto entry = rpc_clients_.find(actor_id);
  if (entry == rpc_clients_.end()) {
    // TODO: what if actor is not created yet?
    if (pending_requests_[actor_id].empty()) {
      gcs_client_->Actors().RequestNotifications(JobID::Nil(), actor_id,
          gcs_client_->GetLocalClientID());
    }

    // append the task to the pending list.
    pending_requests_[actor_id].emplace_back(std::move(request));

    return Status::OK();
  }

  auto &client = entry->second;
  auto status = PushTask(*client, *request);

  return status;
}

Status CoreWorkerDirectActorTaskSubmitter::SubscribeActorTable() {
  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](const ActorID &actor_id,
                                            const std::vector<ActorTableData> &data) {
    if (!data.empty()) {
      const auto &actor_data = data.back();
      
      if (actor_data.state() == ActorTableData::ALIVE) {
        std::unique_ptr<rpc::DirectActorClient> grpc_client(
            new rpc::DirectActorClient(actor_data.ip_address(),
            actor_data.port(), client_call_manager_));
        
        std::unique_lock<std::mutex> guard(rpc_clients_mutex_);

        rpc_clients_.emplace(actor_id, std::move(grpc_client));
        auto entry = rpc_clients_.find(actor_id);
        RAY_CHECK(entry != rpc_clients_.end());

        auto &client = entry->second;
        auto &requests = pending_requests_[actor_id];
        while (!requests.empty()) {
          const auto &request = *requests.front();
          auto status = PushTask(*client, request);
          requests.pop_front();
        }
        
      } else if (actor_data.state() == ActorTableData::DEAD) {

        // There are a couple of cases for actor/objects:
        // - dead
        // - reconstruction
        // - eviction
        // For get, there are a few possibilities:
        // - pending
        // - future (but on old objects)

      } else {
        //
        RAY_CHECK(actor_data.state() == ActorTableData::RECONSTRUCTING);
      }

      // TODO: handle other states.
    }
  };

  return gcs_client_->Actors().AsyncSubscribe(
      JobID::Nil(), ClientID::Nil(), actor_notification_callback, nullptr);
}

Status CoreWorkerDirectActorTaskSubmitter::PushTask(rpc::DirectActorClient &client,
    const rpc::PushTaskRequest &request) {
  auto status = client.PushTask(request, [this](
                            Status status, const rpc::PushTaskReply &reply) {
    RAY_LOG(INFO) << ++counter_ << status;

    // TODO(zhijunfu): if return id count doesn't match, write an exception into store.
    RAY_CHECK(reply.return_object_ids_size() == reply.return_objects_size());
    for (int i = 0; i < reply.return_object_ids_size(); i++) {
      ObjectID object_id = ObjectID::FromBinary(reply.return_object_ids(i));
      const std::string &return_object = reply.return_objects(i);
      auto data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
          return_object.data()));
      LocalMemoryBuffer buffer(data, return_object.size());
      Put(buffer, object_id);     
    }});
  return status;
}


Status CoreWorkerDirectActorTaskSubmitter::Put(const Buffer &buffer, const ObjectID &object_id) {
  auto plasma_id = object_id.ToPlasmaId();
  std::shared_ptr<arrow::Buffer> data;
  RAY_ARROW_RETURN_NOT_OK(
      store_client_.Create(plasma_id, buffer.Size(), nullptr, 0, &data));

  memcpy(data->mutable_data(), buffer.Data(), buffer.Size());

  RAY_ARROW_RETURN_NOT_OK(store_client_.Seal(plasma_id));
  RAY_ARROW_RETURN_NOT_OK(store_client_.Release(plasma_id));
  return Status::OK();
}

CoreWorkerDirectActorTaskReceiver::CoreWorkerDirectActorTaskReceiver(
    boost::asio::io_service &io_service,
    rpc::GrpcServer &server)
    : task_service_(io_service, *this) {
  
  server.RegisterService(task_service_);
}

void CoreWorkerDirectActorTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request,
    rpc::PushTaskReply *reply,
    rpc::RequestDoneCallback done_callback) {

  const std::string &task_message = request.task_spec();
  const raylet::TaskSpecification spec(task_message);
  //const raylet::Task task(*flatbuffers::GetRoot<protocol::Task>(
  //    reinterpret_cast<const uint8_t *>(task_message.data())));  
  //const auto &spec = task.GetTaskSpecification();

  if (HasByReferenceArgs(spec)) {
    done_callback(Status::Invalid("direct actor call only supports by value arguments"));
    return;
  }

  std::vector<std::shared_ptr<Buffer>> results;
  auto status = task_handler_(spec, &results);
  RAY_CHECK(results.size() == spec.NumReturns());
  for (int i = 0; i < spec.NumReturns(); i++) {
    ObjectID id = ObjectID::ForTaskReturn(spec.TaskId(), i + 1);
    (*reply).add_return_object_ids(id.Binary());
  }

  for (int i = 0; i < results.size(); i++) {
    std::string data(reinterpret_cast<const char*>(
        const_cast<const uint8_t*>(results[i]->Data())), results[i]->Size());
    (*reply).add_return_objects(data);
  }

  usleep(100 * 1000);
  done_callback(status);
}

Status CoreWorkerDirectActorTaskReceiver::SetTaskHandler(const TaskHandler &callback) {
  task_handler_ = callback; 
  return Status::OK();
}

}  // namespace ray
