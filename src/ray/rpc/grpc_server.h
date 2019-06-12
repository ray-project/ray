#ifndef RAY_RPC_GRPC_SERVER_H
#define RAY_RPC_GRPC_SERVER_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/server_call.h"

namespace ray {

/// Abstract base class that represents a general gRPC server.
class GrpcServer {
 public:
  GrpcServer(const uint32_t port) : port_(port) {}

  ~GrpcServer() {
    server_->Shutdown();
    cq_->Shutdown();
  }

  void Run();

 protected:
  virtual void RegisterServices(::grpc::ServerBuilder &builder) = 0;

  virtual void InitServerCallFactories(
      std::vector<std::unique_ptr<UntypedServerCallFactory>> *server_call_factories) = 0;

  void StartPolling();

  const uint32_t port_;

  std::vector<std::unique_ptr<UntypedServerCallFactory>> server_call_factories_;

  std::unique_ptr<std::thread> polling_thread_;

  std::unique_ptr<::grpc::ServerCompletionQueue> cq_;
  std::unique_ptr<::grpc::Server> server_;
};

}  // namespace ray

#endif
