
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

bool CoreWorkerRayletTaskSubmitter::ShouldWaitTask(const TaskID &task_id) const {
  // Return true so that `ray.get` will wait for the objects.
  // This is fine since raylet has lineage and can reconstruct the tasks
  // and the objects will eventually be available, or errors will be written
  // into store.
  return true;  
}

StoreProviderType
CoreWorkerRayletTaskSubmitter::GetStoreProviderTypeForReturnObject() const {
  return StoreProviderType::PLASMA;
}

CoreWorkerRayletTaskReceiver::CoreWorkerRayletTaskReceiver(
    std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer, boost::asio::io_service &io_service,
    const TaskHandler &task_handler)
    : raylet_client_(raylet_client),
      store_provider_layer_(store_provider_layer),
      task_service_(io_service, *this),
      task_handler_(task_handler) {
}

void CoreWorkerRayletTaskReceiver::HandleAssignTask(
    const rpc::AssignTaskRequest &request, rpc::AssignTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const Task task(request.task());
  const auto &task_spec = task.GetTaskSpecification();
  std::vector<std::shared_ptr<RayObject>> results;
  auto status = task_handler_(task_spec, &results);

  auto num_returns = task_spec.NumReturns();
  if (task_spec.IsActorCreationTask() || task_spec.IsActorTask()) {
    RAY_CHECK(num_returns > 0);
    // Decrease to account for the dummy object id.
    num_returns--;
  }

  RAY_CHECK(results.size() == num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    ObjectID id = ObjectID::ForTaskReturn(
        task_spec.TaskId(), /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::RAYLET));
    RAY_CHECK_OK(store_provider_layer_.Put(StoreProviderType::PLASMA, *results[i], id));
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

rpc::GrpcService &CoreWorkerRayletTaskReceiver::GetRpcService() {
  return task_service_;
}

}  // namespace ray
