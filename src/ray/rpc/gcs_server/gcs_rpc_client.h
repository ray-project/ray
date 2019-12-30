#ifndef RAY_RPC_GCS_RPC_CLIENT_H
#define RAY_RPC_GCS_RPC_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "src/ray/protobuf/gcs_service.pb.h"
#include "src/ray/rpc/grpc_client.h"

namespace ray {
namespace rpc {

/// Client used for communicating with gcs server.
class GcsRpcClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of gcs server.
  /// \param[in] port Port of the gcs server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  GcsRpcClient(const std::string &address, const int port,
               ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    job_info_rpc_client_ = std::unique_ptr<GrpcClient<JobInfoGcsService>>(
        new GrpcClient<JobInfoGcsService>(address, port, client_call_manager));
    actor_info_rpc_client_ = std::unique_ptr<GrpcClient<ActorInfoGcsService>>(
        new GrpcClient<ActorInfoGcsService>(address, port, client_call_manager));
    node_info_rpc_client_ = std::unique_ptr<GrpcClient<NodeInfoGcsService>>(
        new GrpcClient<NodeInfoGcsService>(address, port, client_call_manager));
    object_info_rpc_client_ = std::unique_ptr<GrpcClient<ObjectInfoGcsService>>(
        new GrpcClient<ObjectInfoGcsService>(address, port, client_call_manager));
  };

  /// Add job info to gcs server.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AddJob(const AddJobRequest &request, const ClientCallback<AddJobReply> &callback) {
    RPC_CALL_METHOD(JobInfoGcsService, AddJob, request, callback);
  }

  /// Mark job as finished to gcs server.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void MarkJobFinished(const MarkJobFinishedRequest &request,
                       const ClientCallback<MarkJobFinishedReply> &callback) {
    RPC_CALL_METHOD(JobInfoGcsService, MarkJobFinished, request, callback);
  }

  /// Get actor data from GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void GetActorInfo(const GetActorInfoRequest &request,
                    const ClientCallback<GetActorInfoReply> &callback) {
    RPC_CALL_METHOD(ActorInfoGcsService, GetActorInfo, request, callback);
  }

  /// Register an actor to GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void RegisterActorInfo(const RegisterActorInfoRequest &request,
                         const ClientCallback<RegisterActorInfoReply> &callback) {
    RPC_CALL_METHOD(ActorInfoGcsService, RegisterActorInfo, request, callback);
  }

  ///  Update actor info in GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void UpdateActorInfo(const UpdateActorInfoRequest &request,
                       const ClientCallback<UpdateActorInfoReply> &callback) {
    RPC_CALL_METHOD(ActorInfoGcsService, UpdateActorInfo, request, callback);
  }

  /// Register a node to GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void RegisterNode(const RegisterNodeRequest &request,
                    const ClientCallback<RegisterNodeReply> &callback) {
    RPC_CALL_METHOD(NodeInfoGcsService, RegisterNode, request, callback);
  }

  /// Unregister a node from GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void UnregisterNode(const UnregisterNodeRequest &request,
                      const ClientCallback<UnregisterNodeReply> &callback) {
    RPC_CALL_METHOD(NodeInfoGcsService, UnregisterNode, request, callback);
  }

  /// Get information of all nodes from GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void GetAllNodeInfo(const GetAllNodeInfoRequest &request,
                      const ClientCallback<GetAllNodeInfoReply> &callback) {
    RPC_CALL_METHOD(NodeInfoGcsService, GetAllNodeInfo, request, callback);
  }

  /// Get object's locations from GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void GetObjectLocations(const GetObjectLocationsRequest &request,
                          const ClientCallback<GetObjectLocationsReply> &callback) {
    RPC_CALL_METHOD(ObjectInfoGcsService, GetObjectLocations, request, callback);
  }

  /// Add location of object to GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AddObjectLocation(const AddObjectLocationRequest &request,
                         const ClientCallback<AddObjectLocationReply> &callback) {
    RPC_CALL_METHOD(ObjectInfoGcsService, AddObjectLocation, request, callback);
  }

  /// Remove location of object to GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void RemoveObjectLocation(const RemoveObjectLocationRequest &request,
                            const ClientCallback<RemoveObjectLocationReply> &callback) {
    RPC_CALL_METHOD(ObjectInfoGcsService, RemoveObjectLocation, request, callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<JobInfoGcsService::Stub> job_info_rpc_client_;
  std::unique_ptr<ActorInfoGcsService::Stub> actor_info_rpc_client_;
  std::unique_ptr<NodeInfoGcsService::Stub> node_info_rpc_client_;
  std::unique_ptr<ObjectInfoGcsService::Stub> object_info_rpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_CLIENT_H
