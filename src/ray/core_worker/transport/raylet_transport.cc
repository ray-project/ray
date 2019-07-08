
#include "ray/core_worker/transport/raylet_transport.h"
#include "ray/raylet/task.h"

namespace ray {

CoreWorkerRayletTaskSubmitter::CoreWorkerRayletTaskSubmitter(
    std::unique_ptr<RayletClient> &raylet_client)
    : raylet_client_(raylet_client) {}

Status CoreWorkerRayletTaskSubmitter::SubmitTask(const TaskSpec &task) {
  RAY_CHECK(raylet_client_ != nullptr);
  return raylet_client_->SubmitTask(task.GetDependencies(), task.GetTaskSpecification());
}

CoreWorkerRayletTaskReceiver::CoreWorkerRayletTaskReceiver(
    std::unique_ptr<RayletClient> &raylet_client, boost::asio::io_service &io_service,
    rpc::GrpcServer &server)
    : raylet_client_(raylet_client), task_service_(io_service, *this) {
  server.RegisterService(task_service_);
}

Status CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply) {
  const std::string &task_message = request.task_spec();
  const raylet::Task task(*flatbuffers::GetRoot<protocol::Task>(
      reinterpret_cast<const uint8_t *>(task_message.data())));
  const auto &spec = task.GetTaskSpecification();

  return task_handler_(spec);
}

Status CoreWorkerRayletTaskReceiver::SetTaskHandler(const TaskHandler &callback) {
  task_handler_ = callback;
  return Status::OK();
}

}  // namespace ray
