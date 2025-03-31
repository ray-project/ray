#include "ray/rpc/head_node_service.h"

namespace ray {
namespace rpc {

HeadNodeService::HeadNodeService(gcs::GcsServer &gcs_server,
                               const std::string &address,
                               int port)
    : gcs_server_(gcs_server) {
  // Create service handler
  service_handler_ = std::make_unique<HeadNodeServiceHandler>(gcs_server_);

  // Create gRPC server
  server_ = std::make_unique<GrpcServer>(address, port);
  server_->RegisterService(service_handler_.get());

  // Create client
  client_ = std::make_unique<HeadNodeClient>(address, port, server_->GetCallManager());
}

HeadNodeService::~HeadNodeService() {
  Stop();
}

void HeadNodeService::Start() {
  // Start gRPC server
  server_->Start();
}

void HeadNodeService::Stop() {
  // Stop gRPC server
  if (server_) {
    server_->Stop();
  }
}

}  // namespace rpc
}  // namespace ray 