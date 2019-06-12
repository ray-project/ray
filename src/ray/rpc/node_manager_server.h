#ifndef RAY_RPC_NODE_MANAGER_SERVER_H
#define RAY_RPC_NODE_MANAGER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

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

  void InitServerCallFactories(std::vector<std::unique_ptr<UntypedServerCallFactory>>
                                   *server_call_factories) override {
    std::unique_ptr<UntypedServerCallFactory> forward_task_call_factory(
        new ServerCallFactory<NodeManagerService, NodeManagerServiceHandler,
                              ForwardTaskRequest, ForwardTaskReply>(
            &service_, &NodeManagerService::AsyncService::RequestForwardTask, handler_,
            &NodeManagerServiceHandler::HandleForwardTask, cq_));
    server_call_factories->push_back(std::move(forward_task_call_factory));
  }

 private:
  NodeManagerService::AsyncService service_;
  NodeManagerServiceHandler *handler_;
};

}  // namespace ray

#endif
