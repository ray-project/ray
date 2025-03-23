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

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {
/// TODO(vitsai): Remove this when auth is implemented for node manager
#define RAY_CORE_WORKER_RPC_SERVICE_HANDLER(METHOD)        \
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED( \
      CoreWorkerService, METHOD, -1, AuthType::NO_AUTH)

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.
/// Disable gRPC server metrics since it incurs too high cardinality.
#define RAY_CORE_WORKER_RPC_HANDLERS                                  \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PushTask)                       \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(DirectActorCallArgWaitComplete) \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RayletNotifyGCSRestart)         \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetObjectStatus)                \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(WaitForActorRefDeleted)         \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PubsubLongPolling)              \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PubsubCommandBatch)             \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(UpdateObjectLocationBatch)      \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetObjectLocationsOwner)        \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(ReportGeneratorItemReturns)     \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(KillActor)                      \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(CancelTask)                     \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RemoteCancelTask)               \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RegisterMutableObjectReader)    \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetCoreWorkerStats)             \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(LocalGC)                        \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(DeleteObjects)                  \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(SpillObjects)                   \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RestoreSpilledObjects)          \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(DeleteSpilledObjects)           \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PlasmaObjectReady)              \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(Exit)                           \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(AssignObjectOwner)              \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(NumPendingTasks)

#define RAY_CORE_WORKER_DECLARE_RPC_HANDLERS                              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PushTask)                       \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DirectActorCallArgWaitComplete) \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RayletNotifyGCSRestart)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectStatus)                \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(WaitForActorRefDeleted)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PubsubLongPolling)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PubsubCommandBatch)             \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(UpdateObjectLocationBatch)      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectLocationsOwner)        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(ReportGeneratorItemReturns)     \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(KillActor)                      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(CancelTask)                     \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RemoteCancelTask)               \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RegisterMutableObjectReader)    \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetCoreWorkerStats)             \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(LocalGC)                        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteObjects)                  \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(SpillObjects)                   \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RestoreSpilledObjects)          \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteSpilledObjects)           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PlasmaObjectReady)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(Exit)                           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AssignObjectOwner)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(NumPendingTasks)

/// Interface of the `CoreWorkerServiceHandler`, see `src/ray/protobuf/core_worker.proto`.
class CoreWorkerServiceHandler : public DelayedServiceHandler {
 public:
  /// Blocks until the service is ready to serve RPCs.
  virtual void WaitUntilInitialized() = 0;

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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override {
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
