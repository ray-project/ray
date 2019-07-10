
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
    boost::asio::io_service &io_service, rpc::GrpcServer &server,
    CoreWorkerObjectInterface &object_interface)
    : task_service_(io_service, *this),
      object_interface_(object_interface) {
  server.RegisterService(task_service_);
}

void CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply,
    rpc::RequestDoneCallback done_callback) {
  const std::string &task_message = request.task_spec();
  const raylet::Task task(*flatbuffers::GetRoot<protocol::Task>(
      reinterpret_cast<const uint8_t *>(task_message.data())));
  const auto &spec = task.GetTaskSpecification();

  std::vector<std::shared_ptr<Buffer>> results;
  auto status = task_handler_(spec, &results);

  auto num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    RAY_CHECK(num_returns > 0);
    // Decrease to account for the dummy object id.
    num_returns--;
  }

  RAY_CHECK(results.size() == num_returns);
  for (int i = 0; i < num_returns; i++) {
    ObjectID id = ObjectID::ForTaskReturn(spec.TaskId(), i + 1);
    object_interface_.Put(*results[i], id);
  }
  done_callback(status);
}

Status CoreWorkerRayletTaskReceiver::SetTaskHandler(const TaskHandler &callback) {
  task_handler_ = callback;
  return Status::OK();
}

}  // namespace ray
