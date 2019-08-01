
#include "ray/core_worker/transport/transport_layer.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"
#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

CoreWorkerTaskSubmitterLayer::CoreWorkerTaskSubmitterLayer(
    boost::asio::io_service &io_service,
    std::unique_ptr<RayletClient> &raylet_client,
    gcs::RedisGcsClient &gcs_client,
    CoreWorkerStoreProviderLayer &store_provider_layer) {

  // Add all task submitters.
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter(raylet_client)));
  task_submitters_.emplace(TaskTransportType::DIRECT_ACTOR,
                           std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                               new CoreWorkerDirectActorTaskSubmitter(
                                   io_service, gcs_client, store_provider_layer)));
}

Status CoreWorkerTaskSubmitterLayer::SubmitTask(TaskTransportType type, const TaskSpecification &task_spec) {
  return task_submitters_[type]->SubmitTask(task_spec);
}

CoreWorkerTaskReceiverLayer::CoreWorkerTaskReceiverLayer(
    std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer,
    CoreWorkerTaskReceiver::TaskHandler executor_func)
    : worker_server_("Worker", 0 /* let grpc choose port */),
      main_work_(main_service_) {
  RAY_CHECK(executor_func != nullptr);
  
  // Add all task receivers.
  task_receivers_.emplace(
      TaskTransportType::RAYLET,
      std::unique_ptr<CoreWorkerRayletTaskReceiver>(new CoreWorkerRayletTaskReceiver(
          raylet_client, store_provider_layer, main_service_, executor_func)));
  task_receivers_.emplace(
      TaskTransportType::DIRECT_ACTOR,
      std::unique_ptr<CoreWorkerDirectActorTaskReceiver>(
          new CoreWorkerDirectActorTaskReceiver(main_service_, executor_func)));
  
  for (const auto &entry : task_receivers_) {
    worker_server_.RegisterService(entry.second->GetRpcService());
  }

  // Start RPC server after all the task receivers are properly initialized.
  worker_server_.Run();
}

void CoreWorkerTaskReceiverLayer::Run() {
  main_service_.run();
}

int CoreWorkerTaskReceiverLayer::GetRpcServerPort() const {
  return worker_server_.GetPort();
}

}  // namespace ray
