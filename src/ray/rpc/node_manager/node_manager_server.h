#ifndef RAY_RPC_NODE_MANAGER_SERVER_H
#define RAY_RPC_NODE_MANAGER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.
#define RAY_NODE_MANAGER_RPC_HANDLERS                              \
  RPC_SERVICE_HANDLER(NodeManagerService, RequestWorkerLease, 100) \
  RPC_SERVICE_HANDLER(NodeManagerService, ReturnWorker, 100)       \
  RPC_SERVICE_HANDLER(NodeManagerService, ForwardTask, 100)        \
  RPC_SERVICE_HANDLER(NodeManagerService, PinObjectIDs, 100)       \
  RPC_SERVICE_HANDLER(NodeManagerService, GetNodeStats, 1)         \
  RPC_SERVICE_HANDLER(NodeManagerService, GlobalGC, 1)

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

  virtual void HandleRequestWorkerLease(const RequestWorkerLeaseRequest &request,
                                        RequestWorkerLeaseReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReturnWorker(const ReturnWorkerRequest &request,
                                  ReturnWorkerReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleForwardTask(const ForwardTaskRequest &request,
                                 ForwardTaskReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePinObjectIDs(const PinObjectIDsRequest &request,
                                  PinObjectIDsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNodeStats(const GetNodeStatsRequest &request,
                                  GetNodeStatsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGlobalGC(const GlobalGCRequest &request, GlobalGCReply *reply,
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
    RAY_NODE_MANAGER_RPC_HANDLERS
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
