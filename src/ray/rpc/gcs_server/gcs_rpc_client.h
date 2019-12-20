#ifndef RAY_RPC_GCS_RPC_CLIENT_H
#define RAY_RPC_GCS_RPC_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "src/ray/protobuf/gcs_service.pb.h"
#include "src/ray/rpc/client_call.h"

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
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    job_info_stub_ = JobInfoGcsService::NewStub(channel);
    actor_info_stub_ = ActorInfoGcsService::NewStub(channel);
  };

  /// Add job info to gcs server.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AddJob(const AddJobRequest &request, const ClientCallback<AddJobReply> &callback) {
    client_call_manager_.CreateCall<JobInfoGcsService, AddJobRequest, AddJobReply>(
        *job_info_stub_, &JobInfoGcsService::Stub::PrepareAsyncAddJob, request, callback);
  }

  /// Mark job as finished to gcs server.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void MarkJobFinished(const MarkJobFinishedRequest &request,
                       const ClientCallback<MarkJobFinishedReply> &callback) {
    client_call_manager_
        .CreateCall<JobInfoGcsService, MarkJobFinishedRequest, MarkJobFinishedReply>(
            *job_info_stub_, &JobInfoGcsService::Stub::PrepareAsyncMarkJobFinished,
            request, callback);
  }

  /// Get actor data from GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void GetActorInfo(const GetActorInfoRequest &request,
                    const ClientCallback<GetActorInfoReply> &callback) {
    client_call_manager_
        .CreateCall<ActorInfoGcsService, GetActorInfoRequest, GetActorInfoReply>(
            *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncGetActorInfo,
            request, callback);
  }

  /// Register an actor to GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void RegisterActorInfo(const RegisterActorInfoRequest &request,
                         const ClientCallback<RegisterActorInfoReply> &callback) {
    client_call_manager_.CreateCall<ActorInfoGcsService, RegisterActorInfoRequest,
                                    RegisterActorInfoReply>(
        *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncRegisterActorInfo,
        request, callback);
  }

  ///  Update actor info in GCS Service.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void UpdateActorInfo(const UpdateActorInfoRequest &request,
                       const ClientCallback<UpdateActorInfoReply> &callback) {
    client_call_manager_
        .CreateCall<ActorInfoGcsService, UpdateActorInfoRequest, UpdateActorInfoReply>(
            *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncUpdateActorInfo,
            request, callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<JobInfoGcsService::Stub> job_info_stub_;
  std::unique_ptr<ActorInfoGcsService::Stub> actor_info_stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_CLIENT_H
