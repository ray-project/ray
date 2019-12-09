#pragma once

#include <thread>

#include <grpcpp/grpcpp.h>

#include "src/ray/protobuf/job_info_access.grpc.pb.h"
#include "src/ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote job info access server.
class JobInfoAccessClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of job info access server.
  /// \param[in] port Port of the job info access server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  JobInfoAccessClient(const std::string &address, const int port,
                      ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = JobInfoAccessService::NewStub(channel);
  };

  /// Add job info to remote job info access server
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  void AddJob(const GcsJobInfo &request, const ClientCallback<AddJobReply> &callback) {
    client_call_manager_.CreateCall<JobInfoAccessService, GcsJobInfo, AddJobReply>(
        *stub_, &JobInfoAccessService::Stub::PrepareAsyncAddJob, request, callback);
  }

  /// Mark job as finished to remote job info access server
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  void MarkJobFinished(const FinishedJob &request,
                       const ClientCallback<MarkJobFinishedReply> &callback) {
    client_call_manager_
        .CreateCall<JobInfoAccessService, FinishedJob, MarkJobFinishedReply>(
            *stub_, &JobInfoAccessService::Stub::PrepareAsyncMarkJobFinished, request,
            callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<JobInfoAccessService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray