#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
namespace rpc {

GcsRpcClient::GcsRpcClient(const std::string &address, const int port,
                           ClientCallManager &client_call_manager) {
  job_info_grpc_client_ = std::unique_ptr<GrpcClient<JobInfoGcsService>>(
      new GrpcClient<JobInfoGcsService>(address, port, client_call_manager));
  actor_info_grpc_client_ = std::unique_ptr<GrpcClient<ActorInfoGcsService>>(
      new GrpcClient<ActorInfoGcsService>(address, port, client_call_manager));
  node_info_grpc_client_ = std::unique_ptr<GrpcClient<NodeInfoGcsService>>(
      new GrpcClient<NodeInfoGcsService>(address, port, client_call_manager));
  object_info_grpc_client_ = std::unique_ptr<GrpcClient<ObjectInfoGcsService>>(
      new GrpcClient<ObjectInfoGcsService>(address, port, client_call_manager));
  task_info_grpc_client_ = std::unique_ptr<GrpcClient<TaskInfoGcsService>>(
      new GrpcClient<TaskInfoGcsService>(address, port, client_call_manager));
  stats_grpc_client_ = std::unique_ptr<GrpcClient<StatsGcsService>>(
      new GrpcClient<StatsGcsService>(address, port, client_call_manager));
  error_info_grpc_client_ = std::unique_ptr<GrpcClient<ErrorInfoGcsService>>(
      new GrpcClient<ErrorInfoGcsService>(address, port, client_call_manager));
  worker_info_grpc_client_ = std::unique_ptr<GrpcClient<WorkerInfoGcsService>>(
      new GrpcClient<WorkerInfoGcsService>(address, port, client_call_manager));
}

}  // namespace rpc
}  // namespace ray
