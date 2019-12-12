#pragma once

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

  /// Get actor specification from gcs server asynchronously.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AsyncGet(const ActorAsyncGetRequest &request,
                const ClientCallback<ActorAsyncGetReply> &callback) {
    client_call_manager_
        .CreateCall<ActorInfoGcsService, ActorAsyncGetRequest, ActorAsyncGetReply>(
            *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncAsyncGet, request,
            callback);
  }

  /// Register an actor to gcs server asynchronously.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AsyncRegister(const ActorAsyncRegisterRequest &request,
                     const ClientCallback<ActorAsyncRegisterReply> &callback) {
    client_call_manager_.CreateCall<ActorInfoGcsService, ActorAsyncRegisterRequest,
                                    ActorAsyncRegisterReply>(
        *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncAsyncRegister, request,
        callback);
  }

  ///  Update dynamic states of actor in gcs server asynchronously.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server.
  void AsyncUpdate(const ActorAsyncUpdateRequest &request,
                   const ClientCallback<ActorAsyncUpdateReply> &callback) {
    client_call_manager_
        .CreateCall<ActorInfoGcsService, ActorAsyncUpdateRequest, ActorAsyncUpdateReply>(
            *actor_info_stub_, &ActorInfoGcsService::Stub::PrepareAsyncAsyncUpdate,
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
