
#include "ray/core_worker/transport/raylet_transport.h"
#include "ray/raylet/task.h"

namespace ray {

CoreWorkerRayletTaskSubmitter::CoreWorkerRayletTaskSubmitter(
    std::unique_ptr<RayletClient> &raylet_client)
    : raylet_client_(raylet_client) {}

Status CoreWorkerRayletTaskSubmitter::SubmitTask(const TaskSpec &task) {
  return raylet_client_->SubmitTask(task.GetDependencies(), task.GetTaskSpecification());
}

Status CoreWorkerRayletTaskReceiver::GetTasks(std::vector<TaskSpec> *tasks) {
  std::unique_ptr<raylet::TaskSpecification> task_spec;
  auto status = raylet_client_->GetTask(&task_spec);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Get task from raylet failed with error: "
                   << ray::Status::IOError(status.message());
    return status;
  }

  std::vector<ObjectID> dependencies;
  RAY_CHECK((*tasks).empty());
  (*tasks).emplace_back(*task_spec, dependencies);

  return Status::OK();
}

CoreWorkerRayletTaskReceiver::CoreWorkerRayletTaskReceiver(
    std::unique_ptr<RayletClient> &raylet_client, boost::asio::io_service &io_service,
    rpc::GrpcServer &server)
    : raylet_client_(raylet_client), task_service_(io_service, *this) {
  server.RegisterService(task_service_);
}

void CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const raylet::Task task(request.task());
  const auto &spec = task.GetTaskSpecification();
  auto status = task_handler_(spec);
  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace ray
