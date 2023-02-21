// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.
/// Disable gRPC server metrics since it incurs too high cardinality.
#define RAY_CORE_WORKER_RPC_HANDLERS                                                     \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, PushTask, -1)           \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, DirectActorCallArgWaitComplete, -1)                             \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, RayletNotifyGCSRestart, -1)                                     \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, GetObjectStatus, -1)    \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, WaitForActorOutOfScope, -1)                                     \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, PubsubLongPolling, -1)  \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, PubsubCommandBatch, -1) \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, UpdateObjectLocationBatch, -1)                                  \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, GetObjectLocationsOwner, -1)                                    \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, KillActor, -1)          \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, CancelTask, -1)         \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, RemoteCancelTask, -1)   \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, GetCoreWorkerStats, -1) \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, LocalGC, -1)            \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, DeleteObjects, -1)      \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, SpillObjects, -1)       \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, RestoreSpilledObjects, -1)                                      \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(                                           \
      CoreWorkerService, DeleteSpilledObjects, -1)                                       \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, PlasmaObjectReady, -1)  \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, Exit, -1)               \
  RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(CoreWorkerService, AssignObjectOwner, -1)

#define RAY_CORE_WORKER_DECLARE_RPC_HANDLERS                              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PushTask)                       \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DirectActorCallArgWaitComplete) \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RayletNotifyGCSRestart)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectStatus)                \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(WaitForActorOutOfScope)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PubsubLongPolling)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PubsubCommandBatch)             \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(UpdateObjectLocationBatch)      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectLocationsOwner)        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(KillActor)                      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(CancelTask)                     \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RemoteCancelTask)               \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetCoreWorkerStats)             \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(LocalGC)                        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteObjects)                  \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(SpillObjects)                   \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RestoreSpilledObjects)          \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteSpilledObjects)           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PlasmaObjectReady)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(Exit)                           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AssignObjectOwner)

/// Interface of the `CoreWorkerServiceHandler`, see `src/ray/protobuf/core_worker.proto`.
class CoreWorkerServiceHandler {
 public:
  virtual ~CoreWorkerServiceHandler() {}
  /// Handlers. For all of the following handlers, the implementations can
  /// handle the request asynchronously. When handling is done, the
  /// `send_reply_callback` should be called. See
  /// src/ray/rpc/node_manager/node_manager_client.h and
  /// src/ray/protobuf/node_manager.proto for a description of the
  /// functionality of each handler.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  RAY_CORE_WORKER_DECLARE_RPC_HANDLERS
};

/// The `GrpcServer` for `CoreWorkerService`.
class CoreWorkerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  CoreWorkerGrpcService(instrumented_io_context &main_service,
                        CoreWorkerServiceHandler &service_handler)
      : GrpcService(main_service), service_handler_(service_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_CORE_WORKER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  CoreWorkerService::AsyncService service_;

  /// The service handler that actually handles the requests.
  CoreWorkerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
