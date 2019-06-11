#ifndef RAY_RPC_NODE_MANAGER_SERVER_H
#define RAY_RPC_NODE_MANAGER_SERVER_H

#include "ray/rpc/server_call.h"
#include "ray/rpc/grpc_server.h"

#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

class NodeManagerServiceHandler {
 public:
  virtual void HandleForwardTask(const ForwardTaskRequest &request,
                                 ForwardTaskReply *reply,
                                 RequestDoneCallback done_callback) = 0;
};

class NodeManagerServer : public GrpcServer {
 public:
  NodeManagerServer(const uint32_t port, NodeManagerServiceHandler *handler)
      : GrpcServer(port), handler_(handler){};

  void RegisterServices(::grpc::ServerBuilder &builder) override {
    builder.RegisterService(&service_);
  }

  void EnqueueRequests() override {
    new ServerCall<NodeManagerService::AsyncService, NodeManagerServiceHandler,
                    ForwardTaskRequest, ForwardTaskReply>(
        &service_, &NodeManagerService::AsyncService::RequestForwardTask, handler_,
        &NodeManagerServiceHandler::HandleForwardTask, cq_.get());
  }

 private:
  NodeManagerService::AsyncService service_;
  NodeManagerServiceHandler *handler_;
};

}  // namespace ray

#endif
