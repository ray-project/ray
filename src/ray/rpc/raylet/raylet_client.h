#ifndef RAY_RPC_OBJECT_MANAGER_CLIENT_H
#define RAY_RPC_OBJECT_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"
#include "src/ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a local node manager server.
class RayletClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  RayletClient(const std::string &address, const int port,
                      ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = RayletService::NewStub(channel);
  };

  /// Submit task to local raylet
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  void SubmitTask(const SubmitTaskRequest &request, const ClientCallback<SubmitTaskReply> &callback) {
    client_call_manager_.CreateCall<RayletService, SubmitTaskRequest, SubmitTaskReply>(
        *stub_, &RayletService::Stub::PrepareAsyncSubmitTask, request, callback);
  }

  /// Notify local raylet that a task has finished
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  void TaskDone(const TaskDoneRequest &request, const ClientCallback<TaskDoneReply> &callback) {
    client_call_manager_.CreateCall<RayletService, TaskDoneRequest, TaskDoneReply>(
        *stub_, &RayletService::Stub::PrepareAsyncTaskDone, request, callback);
  }

  /// Get a new task from local raylet
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  void GetTask(const GetTaskRequest &request,
                   const ClientCallback<GetTaskReply> &callback) {
    client_call_manager_
        .CreateCall<RayletService, GetTaskRequest, GetTaskReply>(
            *stub_, &RayletService::Stub::PrepareAsyncGetTask, request,
            callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<RayletService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_OBJECT_MANAGER_CLIENT_H
