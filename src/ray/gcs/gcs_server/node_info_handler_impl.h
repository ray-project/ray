#ifndef RAY_GCS_NODE_INFO_HANDLER_IMPL_H
#define RAY_GCS_NODE_INFO_HANDLER_IMPL_H

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

class HijackingInterceptor : public experimental::Interceptor {
 public:
  HijackingInterceptor(experimental::ClientRpcInfo* info) {
    info_ = info;
    // Make sure it is the right method
    EXPECT_EQ(strcmp("/grpc.testing.EchoTestService/Echo", info->method()), 0);
    EXPECT_EQ(info->type(), experimental::ClientRpcInfo::Type::UNARY);
  }

  virtual void Intercept(experimental::InterceptorBatchMethods* methods) {
    bool hijack = false;
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      auto* map = methods->GetSendInitialMetadata();
      // Check that we can see the test metadata
      ASSERT_EQ(map->size(), static_cast<unsigned>(1));
      auto iterator = map->begin();
      EXPECT_EQ("testkey", iterator->first);
      EXPECT_EQ("testvalue", iterator->second);
      hijack = true;
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
      EchoRequest req;
      auto* buffer = methods->GetSerializedSendMessage();
      auto copied_buffer = *buffer;
      EXPECT_TRUE(
          SerializationTraits<EchoRequest>::Deserialize(&copied_buffer, &req)
              .ok());
      EXPECT_EQ(req.message(), "Hello");
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_SEND_CLOSE)) {
      // Got nothing to do here for now
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
      auto* map = methods->GetRecvInitialMetadata();
      // Got nothing better to do here for now
      EXPECT_EQ(map->size(), static_cast<unsigned>(0));
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::POST_RECV_MESSAGE)) {
      EchoResponse* resp =
          static_cast<EchoResponse*>(methods->GetRecvMessage());
      // Check that we got the hijacked message, and re-insert the expected
      // message
      EXPECT_EQ(resp->message(), "Hello1");
      resp->set_message("Hello");
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::POST_RECV_STATUS)) {
      auto* map = methods->GetRecvTrailingMetadata();
      bool found = false;
      // Check that we received the metadata as an echo
      for (const auto& pair : *map) {
        found = pair.first.starts_with("testkey") &&
            pair.second.starts_with("testvalue");
        if (found) break;
      }
      EXPECT_EQ(found, true);
      auto* status = methods->GetRecvStatus();
      EXPECT_EQ(status->ok(), true);
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_RECV_INITIAL_METADATA)) {
      auto* map = methods->GetRecvInitialMetadata();
      // Got nothing better to do here at the moment
      EXPECT_EQ(map->size(), static_cast<unsigned>(0));
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_RECV_MESSAGE)) {
      // Insert a different message than expected
      EchoResponse* resp =
          static_cast<EchoResponse*>(methods->GetRecvMessage());
      resp->set_message("Hello1");
    }
    if (methods->QueryInterceptionHookPoint(
        experimental::InterceptionHookPoints::PRE_RECV_STATUS)) {
      auto* map = methods->GetRecvTrailingMetadata();
      // insert the metadata that we want
      EXPECT_EQ(map->size(), static_cast<unsigned>(0));
      map->insert(std::make_pair("testkey", "testvalue"));
      auto* status = methods->GetRecvStatus();
      *status = Status(StatusCode::OK, "");
    }
    if (hijack) {
      methods->Hijack();
    } else {
      methods->Proceed();
    }
  }

 private:
  experimental::ClientRpcInfo* info_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_NODE_INFO_HANDLER_IMPL_H
