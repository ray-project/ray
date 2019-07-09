
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
    std::unique_ptr<RayletClient> &raylet_client,
    boost::asio::io_service &io_service, rpc::GrpcServer &server,
    const TaskHandler &task_handler)
    : raylet_client_(raylet_client),
      task_service_(io_service, *this), task_handler_(task_handler) {
  server.RegisterService(task_service_);
}

void CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply,
    rpc::RequestDoneCallback done_callback) {
  const std::string &task_message = request.task_spec();
  const raylet::Task task(*flatbuffers::GetRoot<protocol::Task>(
      reinterpret_cast<const uint8_t *>(task_message.data())));
  const auto &spec = task.GetTaskSpecification();

  auto status = task_handler_(spec);
  // Notify raylet the current done is done. This is to ensure that the task
  // is marked as finished by raylet only after previous raylet client calls are
  // completed. The rpc `done_callback` is sent via a different connection
  // from raylet client connection, so it cannot guarantee the rpc reply arrives
  // at raylet after a previous `NotifyUnblocked` message.
  raylet_client_->TaskDone();
  done_callback(status);
}

}  // namespace ray
