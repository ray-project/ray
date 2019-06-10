#ifndef RAY_RPC_GRPC_SERVER_H
#define RAY_RPC_GRPC_SERVER_H

#include "ray/rpc/grpc_request.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

class NodeManagerServiceHandler {
  virtual HandleForwardTask(const ForwardTaskRequest &request, ForwardTaskReply *reply,
                            RequestDoneCallback done_callback) = 0;
};

class NodeManagerServer : public GrpcServer {
  NodeManagerServer(const uint32_t port) : GrpcServer(port){};

  void RegisterServices(::grpc::ServerBuilder &builder) override {
    builder.RegisterService(&service_);
  }

  void EnqueueRequests() override {
    new Call<NodeManagerService, NodeManagerServiceHandler, ForwardTaskRequest,
             ForwardTaskReply>(
        &service_, &NodeManagerService::AsyncService::RequestForwardTask, handler,
        &NodeManagerServiceHandler::HandleForwardTask, cq_.get());
  }

 private:
  NodeManagerService::AsyncService service_;
  NodeManagerServiceHandler *handler;
}

}  // namespace ray

#endif
