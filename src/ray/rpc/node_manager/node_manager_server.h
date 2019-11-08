#ifndef RAY_RPC_NODE_MANAGER_SERVER_H
#define RAY_RPC_NODE_MANAGER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `NodeManagerService`, see `src/ray/protobuf/node_manager.proto`.
class NodeManagerServiceHandler {
 public:
  /// Handlers. For all of the following handlers, the implementations can
  /// handle the request asynchronously. When handling is done, the
  /// `send_reply_callback` should be called. See
  /// src/ray/rpc/node_manager/node_manager_client.h and
  /// src/ray/protobuf/node_manager.proto for a description of the
  /// functionality of each handler.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.

  virtual void HandleSubmitTask(const SubmitTaskRequest &request, SubmitTaskReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleForwardTask(const ForwardTaskRequest &request,
                                 ForwardTaskReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;

  virtual void HandleNodeStatsRequest(const NodeStatsRequest &request,
                                      NodeStatsReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeManagerService`.
class NodeManagerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  NodeManagerGrpcService(boost::asio::io_service &io_service,
                         NodeManagerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for requests.
    std::unique_ptr<ServerCallFactory> submit_task_call_factory(
        new ServerCallFactoryImpl<NodeManagerService, NodeManagerServiceHandler,
                                  SubmitTaskRequest, SubmitTaskReply>(
            service_, &NodeManagerService::AsyncService::RequestSubmitTask,
            service_handler_, &NodeManagerServiceHandler::HandleSubmitTask, cq,
            main_service_));

    std::unique_ptr<ServerCallFactory> forward_task_call_factory(
        new ServerCallFactoryImpl<NodeManagerService, NodeManagerServiceHandler,
                                  ForwardTaskRequest, ForwardTaskReply>(
            service_, &NodeManagerService::AsyncService::RequestForwardTask,
            service_handler_, &NodeManagerServiceHandler::HandleForwardTask, cq,
            main_service_));

    std::unique_ptr<ServerCallFactory> node_stats_call_factory(
        new ServerCallFactoryImpl<NodeManagerService, NodeManagerServiceHandler,
                                  NodeStatsRequest, NodeStatsReply>(
            service_, &NodeManagerService::AsyncService::RequestGetNodeStats,
            service_handler_, &NodeManagerServiceHandler::HandleNodeStatsRequest, cq,
            main_service_));

    // Set accept concurrency.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(submit_task_call_factory), 100);
    server_call_factories_and_concurrencies->emplace_back(
        std::move(forward_task_call_factory), 100);
    server_call_factories_and_concurrencies->emplace_back(
        std::move(node_stats_call_factory), 1);
  }

 private:
  /// The grpc async service object.
  NodeManagerService::AsyncService service_;

  /// The service handler that actually handle the requests.
  NodeManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
