
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet/task.h"

namespace ray {

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    boost::asio::io_service &io_service)
    : io_service_(io_service),
      client_call_manager_(io_service) {}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(const TaskSpec &task) {

  RAY_CHECK(task.GetTaskSpecification().IsActorTask());
  const auto &actor_id = task.GetTaskSpecification().ActorId();

  auto entry = rpc_clients_.find(actor_id);
  if (entry == rpc_clients_.end()) {
    // TODO: what if actor is not created yet?

    // Initialize a rpc client to the new worker.
    // TODO: fill in ip address and port.
    std::unique_ptr<rpc::DirectActorClient> grpc_client(
        new rpc::DirectActorClient("0.0.0.0", 0, client_call_manager_));
    rpc_clients_.emplace(actor_id, std::move(grpc_client));
    entry = rpc_clients_.find(actor_id);
    RAY_CHECK(entry != rpc_clients_.end());
  }

  auto &client = entry->second;
  rpc::PushTaskRequest request;
  request.set_task_id(task.GetTaskSpecification().TaskId().Binary());
  request.set_task_spec(task.GetTaskSpecification().SerializeAsString());

  auto status = client->PushTask(request, [this](
                    Status status, const rpc::PushTaskReply &reply) {
    // Worker has finished this task.

    // TODO: If it's an error, then write erros to the return objects
    // so that `ray.get` can throw an exception.
  });    
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
  const raylet::Task task(*flatbuffers::GetRoot<protocol::Task>(
      reinterpret_cast<const uint8_t *>(task_message.data())));  
  const auto &spec = task.GetTaskSpecification();

  auto status = task_handler_(spec);
  done_callback(status);
}

Status CoreWorkerDirectActorTaskReceiver::SetTaskHandler(const TaskHandler &callback) {
  task_handler_ = callback; 
  return Status::OK();
}

}  // namespace ray
