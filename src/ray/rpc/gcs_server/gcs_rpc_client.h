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

#include <gtest/gtest_prod.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/container/btree_map.h"
#include "ray/common/grpc_util.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

/// Convenience macro to invoke VOID_GCS_RPC_CLIENT_METHOD_FULL with defaults.
///
/// Creates a Sync and an Async method just like in VOID_GCS_RPC_CLIENT_METHOD_FULL,
/// with NAMESPACE = ray::rpc, and handle_payload_status = true.
#define VOID_GCS_RPC_CLIENT_METHOD(                         \
    SERVICE, METHOD, grpc_client, method_timeout_ms, SPECS) \
  VOID_GCS_RPC_CLIENT_METHOD_FULL(                          \
      ray::rpc, SERVICE, METHOD, grpc_client, method_timeout_ms, true, SPECS)

/// Define a void GCS RPC client method.
///
/// Example:
///   VOID_GCS_RPC_CLIENT_METHOD_FULL(
///     ray::rpc,
///     ActorInfoGcsService,
///     CreateActor,
///     actor_info_grpc_client_,
///     /*handle_payload_status=*/true,
///     /*method_timeout_ms*/ -1,) # Default value
///   generates
///
///     # Asynchronous RPC. Callback will be invoked once the RPC is replied.
///     rpc_client_.CreateActor(request, callback, timeout_ms = -1);
///
///     # Synchronous RPC. The function will return once the RPC is replied.
///     rpc_client_.SyncCreateActor(request, *reply, timeout_ms = -1);
///
/// Retry protocol:
///   Currently, Ray assumes the GCS server is HA.
///   That says, when there's any RPC failure, the method will automatically retry
///   under the hood.
///
/// \param NAMESPACE namespace of the service.
/// \param SERVICE name of the service.
/// \param METHOD name of the RPC method.
/// \param grpc_client The grpc client to invoke RPC.
/// \param method_timeout_ms The RPC timeout in ms. If the RPC times out,
/// it will return status::TimedOut. Timeout can be configured in 3 levels;
/// whole service, handler, and each call.
/// The priority of timeout is each call > handler > whole service
/// (the lower priority timeout is overwritten by the higher priority timeout).
/// \param handle_payload_status true if the Reply has a status we want to return.
/// \param SPECS The cpp method spec. For example, override.
///
/// Currently, SyncMETHOD will copy the reply additionally.
/// TODO(sang): Fix it.
#define VOID_GCS_RPC_CLIENT_METHOD_FULL(NAMESPACE,                         \
                                        SERVICE,                           \
                                        METHOD,                            \
                                        grpc_client,                       \
                                        method_timeout_ms,                 \
                                        handle_payload_status,             \
                                        SPECS)                             \
  void METHOD(const NAMESPACE::METHOD##Request &request,                   \
              const ClientCallback<NAMESPACE::METHOD##Reply> &callback,    \
              const int64_t timeout_ms = method_timeout_ms) SPECS {        \
    invoke_async_method<NAMESPACE::SERVICE,                                \
                        NAMESPACE::METHOD##Request,                        \
                        NAMESPACE::METHOD##Reply,                          \
                        handle_payload_status>(                            \
        &NAMESPACE::SERVICE::Stub::PrepareAsync##METHOD,                   \
        grpc_client,                                                       \
        #NAMESPACE "::" #SERVICE ".grpc_client." #METHOD,                  \
        request,                                                           \
        callback,                                                          \
        timeout_ms);                                                       \
  }                                                                        \
  ray::Status Sync##METHOD(const NAMESPACE::METHOD##Request &request,      \
                           NAMESPACE::METHOD##Reply *reply_in,             \
                           const int64_t timeout_ms = method_timeout_ms) { \
    std::promise<Status> promise;                                          \
    METHOD(                                                                \
        request,                                                           \
        [&promise, reply_in](const Status &status,                         \
                             const NAMESPACE::METHOD##Reply &reply) {      \
          reply_in->CopyFrom(reply);                                       \
          promise.set_value(status);                                       \
        },                                                                 \
        timeout_ms);                                                       \
    return promise.get_future().get();                                     \
  }

/// Client used for communicating with gcs server.
class GcsRpcClient {
 public:
  static std::shared_ptr<grpc::Channel> CreateGcsChannel(const std::string &address,
                                                         int port) {
    grpc::ChannelArguments arguments = CreateDefaultChannelArguments();
    arguments.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_max_reconnect_backoff_ms());
    arguments.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_min_reconnect_backoff_ms());
    arguments.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_initial_reconnect_backoff_ms());
    return BuildChannel(address, port, arguments);
  }

 public:
  /// Constructor. GcsRpcClient is not thread safe.
  ///
  // \param[in] address Address of gcs server.
  /// \param[in] port Port of the gcs server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  /// \param[in] gcs_service_failure_detected The function is used to redo subscription
  /// and reconnect to GCS RPC server when gcs service failure is detected.
  /// \param[in] reconnection_callback The callback function when the channel get
  /// reconnected due to some error.
  GcsRpcClient(const std::string &address,
               const int port,
               ClientCallManager &client_call_manager)
      : gcs_address_(address), gcs_port_(port) {
    channel_ = CreateGcsChannel(address, port);
    // If not the reconnection will continue to work.
    auto deadline =
        std::chrono::system_clock::now() +
        std::chrono::seconds(::RayConfig::instance().gcs_rpc_server_connect_timeout_s());
    if (!channel_->WaitForConnected(deadline)) {
      RAY_LOG(WARNING) << "Failed to connect to GCS at address " << address << ":" << port
                       << " within "
                       << ::RayConfig::instance().gcs_rpc_server_connect_timeout_s()
                       << " seconds.";
    }

    job_info_grpc_client_ =
        std::make_shared<GrpcClient<JobInfoGcsService>>(channel_, client_call_manager);
    actor_info_grpc_client_ =
        std::make_shared<GrpcClient<ActorInfoGcsService>>(channel_, client_call_manager);
    node_info_grpc_client_ =
        std::make_shared<GrpcClient<NodeInfoGcsService>>(channel_, client_call_manager);
    node_resource_info_grpc_client_ =
        std::make_shared<GrpcClient<NodeResourceInfoGcsService>>(channel_,
                                                                 client_call_manager);
    worker_info_grpc_client_ =
        std::make_shared<GrpcClient<WorkerInfoGcsService>>(channel_, client_call_manager);
    placement_group_info_grpc_client_ =
        std::make_shared<GrpcClient<PlacementGroupInfoGcsService>>(channel_,
                                                                   client_call_manager);
    internal_kv_grpc_client_ =
        std::make_shared<GrpcClient<InternalKVGcsService>>(channel_, client_call_manager);
    internal_pubsub_grpc_client_ = std::make_shared<GrpcClient<InternalPubSubGcsService>>(
        channel_, client_call_manager);
    task_info_grpc_client_ =
        std::make_shared<GrpcClient<TaskInfoGcsService>>(channel_, client_call_manager);
    autoscaler_state_grpc_client_ =
        std::make_shared<GrpcClient<autoscaler::AutoscalerStateService>>(
            channel_, client_call_manager);

    runtime_env_grpc_client_ =
        std::make_shared<GrpcClient<RuntimeEnvGcsService>>(channel_, client_call_manager);

    retryable_grpc_client_ = RetryableGrpcClient::Create(
        channel_,
        client_call_manager.GetMainService(),
        /*max_pending_requests_bytes=*/
        ::RayConfig::instance().gcs_grpc_max_request_queued_max_bytes(),
        /*check_channel_status_interval_milliseconds=*/
        ::RayConfig::instance()
            .grpc_client_check_connection_status_interval_milliseconds(),
        /*server_unavailable_timeout_seconds=*/
        ::RayConfig::instance().gcs_rpc_server_reconnect_timeout_s(),
        /*server_unavailable_timeout_callback=*/
        []() {
          RAY_LOG(ERROR) << "Failed to connect to GCS within "
                         << ::RayConfig::instance().gcs_rpc_server_reconnect_timeout_s()
                         << " seconds. "
                         << "GCS may have been killed. It's either GCS is terminated by "
                            "`ray stop` or "
                         << "is killed unexpectedly. If it is killed unexpectedly, "
                         << "see the log file gcs_server.out. "
                         << "https://docs.ray.io/en/master/ray-observability/user-guides/"
                            "configure-logging.html#logging-directory-structure. "
                         << "The program will terminate.";
          std::_Exit(EXIT_FAILURE);
        },
        /*server_name=*/"GCS");
  }

  template <typename Service,
            typename Request,
            typename Reply,
            bool handle_payload_status>
  void invoke_async_method(
      PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
      std::shared_ptr<GrpcClient<Service>> grpc_client,
      const std::string &call_name,
      const Request &request,
      const ClientCallback<Reply> &callback,
      const int64_t timeout_ms) {
    retryable_grpc_client_->template CallMethod<Service, Request, Reply>(
        prepare_async_function,
        std::move(grpc_client),
        call_name,
        request,
        [callback](const Status &status, Reply &&reply) {
          if (status.ok()) {
            if constexpr (handle_payload_status) {
              Status st = (reply.status().code() == static_cast<int>(StatusCode::OK))
                              ? Status()
                              : Status(StatusCode(reply.status().code()),
                                       reply.status().message());
              callback(st, std::move(reply));
            } else {
              callback(status, std::move(reply));
            }
          } else {
            callback(status, std::move(reply));
          }
        },
        timeout_ms);
  }

  /// Add job info to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService,
                             AddJob,
                             job_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Mark job as finished to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService,
                             MarkJobFinished,
                             job_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get information of all jobs from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService,
                             GetAllJobInfo,
                             job_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Report job error to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService,
                             ReportJobError,
                             job_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get next job id from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService,
                             GetNextJobID,
                             job_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Register actor via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             RegisterActor,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             ReportActorOutOfScope,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             RestartActor,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Create actor via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             CreateActor,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get actor data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             GetActorInfo,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get actor data from GCS Service by name.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             GetNamedActorInfo,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get all named actor names from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             ListNamedActors,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get all actor data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             GetAllActorInfo,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Kill actor via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService,
                             KillActorViaGcs,
                             actor_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  /// Register a client to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             GetClusterId,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Register a node to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             RegisterNode,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Drain a node from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             DrainNode,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Unregister a node from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             UnregisterNode,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get information of all nodes from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             GetAllNodeInfo,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Check GCS is alive.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             CheckAlive,
                             node_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get available resources of all nodes from the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeResourceInfoGcsService,
                             GetAllAvailableResources,
                             node_resource_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get total resources of all nodes from the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeResourceInfoGcsService,
                             GetAllTotalResources,
                             node_resource_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  VOID_GCS_RPC_CLIENT_METHOD(NodeResourceInfoGcsService,
                             GetDrainingNodes,
                             node_resource_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get resource usage of all nodes from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeResourceInfoGcsService,
                             GetAllResourceUsage,
                             node_resource_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Add task events info to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService,
                             AddTaskEventData,
                             task_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Add task events info to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService,
                             GetTaskEvents,
                             task_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Report a worker failure to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             ReportWorkerFailure,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get worker information from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             GetWorkerInfo,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get information of all workers from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             GetAllWorkerInfo,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Add worker information to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             AddWorkerInfo,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Add worker debugger port
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             UpdateWorkerDebuggerPort,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Update the worker number of paused threads delta
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService,
                             UpdateWorkerNumPausedThreads,
                             worker_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Create placement group via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             CreatePlacementGroup,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Remove placement group via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             RemovePlacementGroup,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  /// Get placement group via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             GetPlacementGroup,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get placement group data from GCS Service by name.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             GetNamedPlacementGroup,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get information of all placement group from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             GetAllPlacementGroup,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Wait for placement group until ready via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
                             WaitPlacementGroupUntilReady,
                             placement_group_info_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Operations for kv (Get, Put, Del, Exists)
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVGet,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVMultiGet,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVPut,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVDel,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVExists,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             InternalKVKeys,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Get internal config of the node from the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                             GetInternalConfig,
                             internal_kv_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  /// Operations for pubsub
  VOID_GCS_RPC_CLIENT_METHOD(InternalPubSubGcsService,
                             GcsPublish,
                             internal_pubsub_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalPubSubGcsService,
                             GcsSubscriberPoll,
                             internal_pubsub_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  VOID_GCS_RPC_CLIENT_METHOD(InternalPubSubGcsService,
                             GcsSubscriberCommandBatch,
                             internal_pubsub_grpc_client_,
                             /*method_timeout_ms*/ -1, )
  /// Operations for autoscaler
  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  GetClusterResourceState,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  ReportAutoscalingState,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  ReportClusterConfig,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  RequestClusterResourceConstraint,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  GetClusterStatus,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  VOID_GCS_RPC_CLIENT_METHOD_FULL(ray::rpc::autoscaler,
                                  AutoscalerStateService,
                                  DrainNode,
                                  autoscaler_state_grpc_client_,
                                  /*method_timeout_ms*/ -1,
                                  /*handle_payload_status=*/false, )

  /// Runtime Env GCS Service
  VOID_GCS_RPC_CLIENT_METHOD(RuntimeEnvGcsService,
                             PinRuntimeEnvURI,
                             runtime_env_grpc_client_,
                             /*method_timeout_ms*/ -1, )

  std::pair<std::string, int64_t> GetAddress() const {
    return std::make_pair(gcs_address_, gcs_port_);
  }

  std::shared_ptr<grpc::Channel> GetChannel() const { return channel_; }

 private:
  const std::string gcs_address_;
  const int64_t gcs_port_;
  std::shared_ptr<grpc::Channel> channel_;
  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;

  /// The gRPC-generated stub.
  std::shared_ptr<GrpcClient<JobInfoGcsService>> job_info_grpc_client_;
  std::shared_ptr<GrpcClient<ActorInfoGcsService>> actor_info_grpc_client_;
  std::shared_ptr<GrpcClient<NodeInfoGcsService>> node_info_grpc_client_;
  std::shared_ptr<GrpcClient<NodeResourceInfoGcsService>> node_resource_info_grpc_client_;
  std::shared_ptr<GrpcClient<WorkerInfoGcsService>> worker_info_grpc_client_;
  std::shared_ptr<GrpcClient<PlacementGroupInfoGcsService>>
      placement_group_info_grpc_client_;
  std::shared_ptr<GrpcClient<InternalKVGcsService>> internal_kv_grpc_client_;
  std::shared_ptr<GrpcClient<InternalPubSubGcsService>> internal_pubsub_grpc_client_;
  std::shared_ptr<GrpcClient<TaskInfoGcsService>> task_info_grpc_client_;
  std::shared_ptr<GrpcClient<RuntimeEnvGcsService>> runtime_env_grpc_client_;
  std::shared_ptr<GrpcClient<autoscaler::AutoscalerStateService>>
      autoscaler_state_grpc_client_;

  friend class GcsClientReconnectionTest;
  FRIEND_TEST(GcsClientReconnectionTest, ReconnectionBackoff);
};

}  // namespace rpc
}  // namespace ray
