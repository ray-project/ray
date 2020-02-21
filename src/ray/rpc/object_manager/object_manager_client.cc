#include "ray/rpc/object_manager/object_manager_client.h"

namespace ray {
namespace rpc {

ObjectManagerClient::ObjectManagerClient(const std::string &address, const int port,
                                         ClientCallManager &client_call_manager,
                                         int num_connections)
    : num_connections_(num_connections), client_call_manager_(client_call_manager) {
  push_rr_index_ = rand() % num_connections_;
  pull_rr_index_ = rand() % num_connections_;
  freeobjects_rr_index_ = rand() % num_connections_;
  grpc_clients_.reserve(num_connections_);
  for (int i = 0; i < num_connections_; i++) {
    grpc_clients_.emplace_back(new GrpcClient<ObjectManagerService>(
        address, port, client_call_manager, num_connections_));
  }
}

}  // namespace rpc
}  // namespace ray
