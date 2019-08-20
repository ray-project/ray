
#include "ray/core_worker/transport/raylet_transport.h"
#include "ray/common/task/task.h"

namespace ray {

CoreWorkerRayletTaskSubmitter::CoreWorkerRayletTaskSubmitter(
    std::unique_ptr<RayletClient> &raylet_client)
    : raylet_client_(raylet_client) {}

Status CoreWorkerRayletTaskSubmitter::SubmitTask(const TaskSpecification &task) {
  RAY_CHECK(raylet_client_ != nullptr);
  return raylet_client_->SubmitTask(task);
}

CoreWorkerRayletTaskReceiver::CoreWorkerRayletTaskReceiver(
    std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerObjectInterface &object_interface, boost::asio::io_service &io_service,
    rpc::GrpcServer &server, const TaskHandler &task_handler)
    : raylet_client_(raylet_client),
      object_interface_(object_interface),
      task_service_(io_service, *this),
      task_handler_(task_handler) {
  server.RegisterService(task_service_);
}

void CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const Task task(request.task());
  const auto &task_spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Received task " << task_spec.TaskId();
  std::vector<std::shared_ptr<RayObject>> results;
  auto status = task_handler_(task_spec, &results);

  auto num_returns = task_spec.NumReturns();
  if (task_spec.IsActorCreationTask() || task_spec.IsActorTask()) {
    RAY_CHECK(num_returns > 0);
    // Decrease to account for the dummy object id.
    num_returns--;
  }

  RAY_LOG(DEBUG) << "Assigned task " << task_spec.TaskId()
                 << " finished execution. num_returns: " << num_returns;
  RAY_CHECK(results.size() == num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    ObjectID id = ObjectID::ForTaskReturn(
        task_spec.TaskId(), /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::RAYLET));
    Status status = object_interface_.Put(*results[i], id);
    if (!status.ok()) {
      // NOTE(hchen): `PlasmaObjectExists` error is already ignored inside
      // `ObjectInterface::Put`, we treat other error types as fatal here.
      RAY_LOG(FATAL) << "Task " << task_spec.TaskId() << " failed to put object " << id
                     << " in store: " << status.message();
    } else {
      RAY_LOG(DEBUG) << "Task " << task_spec.TaskId() << " put object " << id
                     << " in store.";
    }
  }

  // Notify raylet that current task is done via a `TaskDone` message. This is to
  // ensure that the task is marked as finished by raylet only after previous
  // raylet client calls are completed. For example, if the worker sends a
  // NotifyUnblocked message that it is no longer blocked in a `ray.get`
  // on the normal raylet socket, then completes an assigned task, we
  // need to guarantee that raylet gets the former message first before
  // marking the task as completed. This is why a `TaskDone` message
  // is required - without it, it's possible that raylet receives
  // rpc reply first before the NotifyUnblocked message arrives,
  // as they use different connections, the `TaskDone` message is sent
  // to raylet via the same connection so the order is guaranteed.
  RAY_UNUSED(raylet_client_->TaskDone());
  // Send rpc reply.
  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace ray
