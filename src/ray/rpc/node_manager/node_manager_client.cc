#include "ray/rpc/node_manager/node_manager_client.h"

namespace ray {
namespace rpc {

std::shared_ptr<NodeManagerWorkerClient> NodeManagerWorkerClient::make(
    const std::string &address, const int port, ClientCallManager &client_call_manager) {
  auto instance = new NodeManagerWorkerClient(address, port, client_call_manager);
  return std::shared_ptr<NodeManagerWorkerClient>(instance);
}

NodeManagerClient::NodeManagerClient(const std::string &address, const int port,
                                     ClientCallManager &client_call_manager)
    : client_call_manager_(client_call_manager) {
  grpc_client_ = std::unique_ptr<GrpcClient<NodeManagerService>>(
      new GrpcClient<NodeManagerService>(address, port, client_call_manager));
}
NodeManagerWorkerClient::NodeManagerWorkerClient(const std::string &address,
                                                 const int port,
                                                 ClientCallManager &client_call_manager)
    : client_call_manager_(client_call_manager) {
  grpc_client_ = std::unique_ptr<GrpcClient<NodeManagerService>>(
      new GrpcClient<NodeManagerService>(address, port, client_call_manager));
}

}  // namespace rpc
}  // namespace ray
