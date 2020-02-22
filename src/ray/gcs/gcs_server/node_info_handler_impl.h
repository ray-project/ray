#ifndef RAY_GCS_NODE_INFO_HANDLER_IMPL_H
#define RAY_GCS_NODE_INFO_HANDLER_IMPL_H

#include <grpcpp/support/client_interceptor.h>
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `NodeInfoHandler`.
class DefaultNodeInfoHandler : public rpc::NodeInfoHandler {
 public:
  explicit DefaultNodeInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleRegisterNode(const RegisterNodeRequest &request, RegisterNodeReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleUnregisterNode(const UnregisterNodeRequest &request,
                            UnregisterNodeReply *reply,
                            SendReplyCallback send_reply_callback) override;

  void HandleGetAllNodeInfo(const GetAllNodeInfoRequest &request,
                            GetAllNodeInfoReply *reply,
                            SendReplyCallback send_reply_callback) override;

  void HandleReportHeartbeat(const ReportHeartbeatRequest &request,
                             ReportHeartbeatReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleReportBatchHeartbeat(const ReportBatchHeartbeatRequest &request,
                                  ReportBatchHeartbeatReply *reply,
                                  SendReplyCallback send_reply_callback) override;

  void HandleGetResources(const GetResourcesRequest &request, GetResourcesReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleUpdateResources(const UpdateResourcesRequest &request,
                             UpdateResourcesReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleDeleteResources(const DeleteResourcesRequest &request,
                             DeleteResourcesReply *reply,
                             SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

class NodeInfoInterceptor : public grpc::experimental::Interceptor {
 public:
  NodeInfoInterceptor() {}
  NodeInfoInterceptor(grpc::experimental::ClientRpcInfo *info) { info_ = info; }

  virtual void Intercept(grpc::experimental::InterceptorBatchMethods *methods) {
    RAY_LOG(INFO) << "Hello world!!!!!!!!!!!!!!!";
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
      RAY_LOG(INFO) << "PRE_SEND_MESSAGE!!!!!!!!!!!!!!!";
      const rpc::RegisterNodeRequest* req_msg =
          static_cast<const rpc::RegisterNodeRequest*>(methods->GetSendMessage());
      ClientID node_id = ClientID::FromBinary(req_msg->node_info().node_id());
      RAY_LOG(INFO) << "###########Registering node info, node id = " << node_id;
    }

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::POST_RECV_MESSAGE)) {
      RAY_LOG(INFO) << "POST_RECV_MESSAGE!!!!!!!!!!!!!!!";
    }

    methods->Proceed();
  }

 private:
  grpc::experimental::ClientRpcInfo *info_;
};

class NodeInfoInterceptorFactory
 : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  virtual grpc::experimental::Interceptor* CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo* info) override {
    return new NodeInfoInterceptor(info);
  }
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_NODE_INFO_HANDLER_IMPL_H
