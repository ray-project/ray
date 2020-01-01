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
    job_info_grpc_client_ = std::unique_ptr<GrpcClient<JobInfoGcsService>>(
        new GrpcClient<JobInfoGcsService>(address, port, client_call_manager));
    actor_info_grpc_client_ = std::unique_ptr<GrpcClient<ActorInfoGcsService>>(
        new GrpcClient<ActorInfoGcsService>(address, port, client_call_manager));
    node_info_grpc_client_ = std::unique_ptr<GrpcClient<NodeInfoGcsService>>(
        new GrpcClient<NodeInfoGcsService>(address, port, client_call_manager));
    object_info_grpc_client_ = std::unique_ptr<GrpcClient<ObjectInfoGcsService>>(
        new GrpcClient<ObjectInfoGcsService>(address, port, client_call_manager));
  };

  /// Add job info to gcs server.
  VOID_RPC_CLIENT_METHOD(JobInfoGcsService, AddJob, request, callback,
                         job_info_grpc_client_)

  /// Mark job as finished to gcs server.
  VOID_RPC_CLIENT_METHOD(JobInfoGcsService, MarkJobFinished, request, callback,
                         job_info_grpc_client_)

  /// Get actor data from GCS Service.
  VOID_RPC_CLIENT_METHOD(ActorInfoGcsService, GetActorInfo, request, callback,
                         actor_info_grpc_client_)

  /// Register an actor to GCS Service.
  VOID_RPC_CLIENT_METHOD(ActorInfoGcsService, RegisterActorInfo, request, callback,
                         actor_info_grpc_client_)

  ///  Update actor info in GCS Service.
  VOID_RPC_CLIENT_METHOD(ActorInfoGcsService, UpdateActorInfo, request, callback,
                         actor_info_grpc_client_)

  /// Register a node to GCS Service.
  VOID_RPC_CLIENT_METHOD(NodeInfoGcsService, RegisterNode, request, callback,
                         node_info_grpc_client_)

  /// Unregister a node from GCS Service.
  VOID_RPC_CLIENT_METHOD(NodeInfoGcsService, UnregisterNode, request, callback,
                         node_info_grpc_client_)

  /// Get information of all nodes from GCS Service.
  VOID_RPC_CLIENT_METHOD(NodeInfoGcsService, GetAllNodeInfo, request, callback,
                         node_info_grpc_client_)

  /// Get object's locations from GCS Service.
  VOID_RPC_CLIENT_METHOD(ObjectInfoGcsService, GetObjectLocations, request, callback,
                         object_info_grpc_client_)

  /// Add location of object to GCS Service.
  VOID_RPC_CLIENT_METHOD(ObjectInfoGcsService, AddObjectLocation, request, callback,
                         object_info_grpc_client_)

  /// Remove location of object to GCS Service.
  VOID_RPC_CLIENT_METHOD(ObjectInfoGcsService, RemoveObjectLocation, request, callback,
                         object_info_grpc_client_)

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<GrpcClient<JobInfoGcsService>> job_info_grpc_client_;
  std::unique_ptr<GrpcClient<ActorInfoGcsService>> actor_info_grpc_client_;
  std::unique_ptr<GrpcClient<NodeInfoGcsService>> node_info_grpc_client_;
  std::unique_ptr<GrpcClient<ObjectInfoGcsService>> object_info_grpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_CLIENT_H
