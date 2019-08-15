#ifndef RAY_RPC_WORKER_CLIENT_H
#define RAY_RPC_WORKER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/worker.grpc.pb.h"
#include "src/ray/protobuf/worker.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote worker server.
class WorkerTaskClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the worker server.
  /// \param[in] port Port of the worker server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  WorkerTaskClient(const std::string &address, const int port,
                   ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = WorkerTaskService::NewStub(channel);
  };

  /// Assign a task to the work.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status AssignTask(const AssignTaskRequest &request,
                         const ClientCallback<AssignTaskReply> &callback) {
    auto call = client_call_manager_
                    .CreateCall<WorkerTaskService, AssignTaskRequest, AssignTaskReply>(
                        *stub_, &WorkerTaskService::Stub::PrepareAsyncAssignTask, request,
                        callback);
    return call->GetStatus();
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<WorkerTaskService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_WORKER_CLIENT_H
