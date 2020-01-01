#ifndef RAY_RPC_GCS_RPC_SERVER_H
#define RAY_RPC_GCS_RPC_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(JobInfoGcsService, HANDLER, CONCURRENCY)

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ActorInfoGcsService, HANDLER, CONCURRENCY)

#define NODE_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(NodeInfoGcsService, HANDLER, CONCURRENCY)

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService, HANDLER, CONCURRENCY)

class JobInfoHandler {
 public:
  virtual ~JobInfoHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(boost::asio::io_service &io_service,
                              JobInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(AddJob, 1);
    RPC_SERVICE_HANDLER(MarkJobFinished, 1);
  }

 private:
  /// The grpc async service object.
  JobInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoHandler &service_handler_;
};

class ActorInfoHandler {
 public:
  virtual ~ActorInfoHandler() = default;

  virtual void HandleGetActorInfo(const GetActorInfoRequest &request,
                                  GetActorInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                                       RegisterActorInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                                     UpdateActorInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ActorInfoGcsService`.
class ActorInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ActorInfoGrpcService(boost::asio::io_service &io_service,
                                ActorInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(GetActorInfo, 1);
    RPC_SERVICE_HANDLER(RegisterActorInfo, 1);
    RPC_SERVICE_HANDLER(UpdateActorInfo, 1);
  }

 private:
  /// The grpc async service object.
  ActorInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ActorInfoHandler &service_handler_;
};

class NodeInfoHandler {
 public:
  virtual ~NodeInfoHandler() = default;

  virtual void HandleRegisterNode(const RegisterNodeRequest &request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNode(const UnregisterNodeRequest &request,
                                    UnregisterNodeReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodeInfo(const GetAllNodeInfoRequest &request,
                                    GetAllNodeInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeInfoGcsService`.
class NodeInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeInfoGrpcService(boost::asio::io_service &io_service,
                               NodeInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(NodeInfoGrpcService, RegisterNode, 1);
    RPC_SERVICE_HANDLER(NodeInfoGrpcService, UnregisterNode, 1);
    RPC_SERVICE_HANDLER(NodeInfoGrpcService, GetAllNodeInfo, 1);
  }

 private:
  /// The grpc async service object.
  NodeInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  NodeInfoHandler &service_handler_;
};

class ObjectInfoHandler {
 public:
  virtual ~ObjectInfoHandler() = default;

  virtual void HandleGetObjectLocations(const GetObjectLocationsRequest &request,
                                        GetObjectLocationsReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddObjectLocation(const AddObjectLocationRequest &request,
                                       AddObjectLocationReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemoveObjectLocation(const RemoveObjectLocationRequest &request,
                                          RemoveObjectLocationReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ObjectInfoHandler`.
class ObjectInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ObjectInfoGrpcService(boost::asio::io_service &io_service,
                                 ObjectInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, GetObjectLocations, 1);
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, AddObjectLocation, 1);
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, RemoveObjectLocation, 1);
  }

 private:
  /// The grpc async service object.
  ObjectInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectInfoHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_SERVER_H
