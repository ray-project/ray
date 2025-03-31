#pragma once

#include <memory>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/head_node_client.h"
#include "ray/rpc/head_node_service_handler.h"

namespace ray {
namespace rpc {

class HeadNodeService {
 public:
  HeadNodeService(gcs::GcsServer &gcs_server, const std::string &address, int port);
  ~HeadNodeService();

  void Start();
  void Stop();

  HeadNodeClient &GetClient() { return *client_; }

 private:
  gcs::GcsServer &gcs_server_;
  std::unique_ptr<GrpcServer> server_;
  std::unique_ptr<HeadNodeServiceHandler> service_handler_;
  std::unique_ptr<HeadNodeClient> client_;
};

}  // namespace rpc
}  // namespace ray 