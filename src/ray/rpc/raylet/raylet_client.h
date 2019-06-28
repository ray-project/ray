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
  /// Constructor for raylet client.
  /// TODO(jzh): At present, client call manager and reply handler service are generated
  /// in raylet client.
  /// Change them as input parameters once we changed the worker into a server.
  ///
  /// \param[in] raylet_socket Unix domain socket of the raylet server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  RayletClient(const std::string &raylet_socket, const ClientID &client_id,
               bool is_worker, const DriverID &driver_id, const Language &language,
               ClientCallManager &client_call_manager)
      : client_id_(client_id),
        is_worker_(is_worker),
        driver_id_(driver_id),
        language_(language),
        main_service_(),
        work_(main_service_),
        client_call_manager_(main_service_) {
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateChannel("unix:" + raylet_socket, grpc::InsecureChannelCredentials());
    stub_ = RayletService::NewStub(channel);
    rpc_thread_ = std::thread([this]() { main_service_.run(); })
  };

  ~RayletClient() {
    main_service_.stop();
    rpc_thread_.join();
  }

 public:
  /// Register this worker in raylet.
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  void RegisterClient(const RegisterClientRequest &request,
                      const ClientCallback<RegisterClientReply> &callback) {
    client_call_manager_
        .CreateCall<RayletService, RegisterClientRequest, RegisterClientReply>(
            *stub_, &RayletService::Stub::PrepareAsyncRegisterClient, request, callback);
  }

  /// Submit task to local raylet
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  void SubmitTask(const SubmitTaskRequest &request,
                  const ClientCallback<SubmitTaskReply> &callback) {
    client_call_manager_.CreateCall<RayletService, SubmitTaskRequest, SubmitTaskReply>(
        *stub_, &RayletService::Stub::PrepareAsyncSubmitTask, request, callback);
  }

  /// Notify local raylet that a task has finished
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  void TaskDone(const TaskDoneRequest &request,
                const ClientCallback<TaskDoneReply> &callback) {
    client_call_manager_.CreateCall<RayletService, TaskDoneRequest, TaskDoneReply>(
        *stub_, &RayletService::Stub::PrepareAsyncTaskDone, request, callback);
  }

  /// Get a new task from local raylet
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  void EventLog(const EventLogRequest &request,
                const ClientCallback<GetTaskReply> &callback) {
    client_call_manager_.CreateCall<RayletService, EventLogRequest, EventLogReply>(
        *stub_, &RayletService::Stub::PrepareAsyncEventLog, request, callback);
  }

  /// Get a new task from local raylet
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  void GetTask(const GetTaskRequest &request,
               const ClientCallback<GetTaskReply> &callback) {
    client_call_manager_.CreateCall<RayletService, GetTaskRequest, GetTaskReply>(
        *stub_, &RayletService::Stub::PrepareAsyncGetTask, request, callback);
  }

  Language GetLanguage() const { return language_; }

  ClientID GetClientID() const { return client_id_; }

  DriverID GetDriverID() const { return driver_id_; }

  bool IsWorker() const { return is_worker_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  const ClientID client_id_;
  const bool is_worker_;
  const DriverID driver_id_;
  const Language language_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  /// The gRPC-generated stub.
  std::unique_ptr<RayletService::Stub> stub_;

  /// Service for handling reply.
  boost::asio::io_service main_service_;

  /// Asio work for main service.
  boost::asio::io_service::worker work_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager client_call_manager_;

  /// The thread used to handle reply.
  std::thread rpc_thread_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_OBJECT_MANAGER_CLIENT_H
