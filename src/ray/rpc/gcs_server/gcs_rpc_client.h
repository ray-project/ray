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
#include <thread>

#include "absl/container/btree_map.h"
#include "ray/common/grpc_util.h"
#include "ray/common/network_util.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class GcsRpcClient;

/// \class Executor
/// Executor saves operation and support retries.
class Executor {
 public:
  Executor(std::function<void(const ray::Status &)> abort_callback)
      : abort_callback_(std::move(abort_callback)) {}

  /// This function is used to execute the given operation.
  ///
  /// \param operation The operation to be executed.
  void Execute(std::function<void()> operation) {
    operation_ = std::move(operation);
    operation_();
  }

  /// This function is used to retry the given operation.
  void Retry() { operation_(); }

  void Abort(const ray::Status &status) { abort_callback_(status); }

 private:
  std::function<void(ray::Status)> abort_callback_;
  std::function<void()> operation_;
};

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
        *grpc_client,                                                      \
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
      : gcs_address_(address),
        gcs_port_(port),
        io_context_(&client_call_manager.GetMainService()),
        timer_(std::make_unique<boost::asio::deadline_timer>(*io_context_)) {
    channel_ = CreateGcsChannel(address, port);
    // If not the reconnection will continue to work.
    auto deadline =
        std::chrono::system_clock::now() +
        std::chrono::seconds(::RayConfig::instance().gcs_rpc_server_connect_timeout_s());
    if (!channel_->WaitForConnected(deadline)) {
      RAY_LOG(ERROR) << "Failed to connect to GCS at address " << address << ":" << port
                     << " within "
                     << ::RayConfig::instance().gcs_rpc_server_connect_timeout_s()
                     << " seconds.";
      gcs_is_down_ = true;
    } else {
      gcs_is_down_ = false;
    }

    job_info_grpc_client_ =
        std::make_unique<GrpcClient<JobInfoGcsService>>(channel_, client_call_manager);
    actor_info_grpc_client_ =
        std::make_unique<GrpcClient<ActorInfoGcsService>>(channel_, client_call_manager);
    node_info_grpc_client_ =
        std::make_unique<GrpcClient<NodeInfoGcsService>>(channel_, client_call_manager);
    node_resource_info_grpc_client_ =
        std::make_unique<GrpcClient<NodeResourceInfoGcsService>>(channel_,
                                                                 client_call_manager);
    worker_info_grpc_client_ =
        std::make_unique<GrpcClient<WorkerInfoGcsService>>(channel_, client_call_manager);
    placement_group_info_grpc_client_ =
        std::make_unique<GrpcClient<PlacementGroupInfoGcsService>>(channel_,
                                                                   client_call_manager);
    internal_kv_grpc_client_ =
        std::make_unique<GrpcClient<InternalKVGcsService>>(channel_, client_call_manager);
    internal_pubsub_grpc_client_ = std::make_unique<GrpcClient<InternalPubSubGcsService>>(
        channel_, client_call_manager);
    task_info_grpc_client_ =
        std::make_unique<GrpcClient<TaskInfoGcsService>>(channel_, client_call_manager);
    autoscaler_state_grpc_client_ =
        std::make_unique<GrpcClient<autoscaler::AutoscalerStateService>>(
            channel_, client_call_manager);

    runtime_env_grpc_client_ =
        std::make_unique<GrpcClient<RuntimeEnvGcsService>>(channel_, client_call_manager);

    SetupCheckTimer();
  }

  template <typename Service,
            typename Request,
            typename Reply,
            bool handle_payload_status>
  void invoke_async_method(
      PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
      GrpcClient<Service> &grpc_client,
      const std::string &call_name,
      const Request &request,
      const ClientCallback<Reply> &callback,
      const int64_t timeout_ms) {
    auto executor = new Executor(
        [callback](const ray::Status &status) { callback(status, Reply()); });
    auto operation_callback = [this, request, callback, executor, timeout_ms](
                                  const ray::Status &status, const Reply &reply) {
      if (status.ok()) {
        if constexpr (handle_payload_status) {
          Status st =
              (reply.status().code() == (int)StatusCode::OK)
                  ? Status()
                  : Status(StatusCode(reply.status().code()), reply.status().message());
          callback(st, reply);
        } else {
          callback(status, reply);
        }
        delete executor;
      } else if (!IsGrpcRetryableStatus(status)) {
        callback(status, reply);
        delete executor;
      } else {
        /* In case of GCS failure, we queue the request and these requests will be */
        /* executed once GCS is back. */
        gcs_is_down_ = true;
        auto request_bytes = request.ByteSizeLong();
        if (pending_requests_bytes_ + request_bytes >
            ::RayConfig::instance().gcs_grpc_max_request_queued_max_bytes()) {
          RAY_LOG(WARNING) << "Pending queue for failed GCS request has reached the "
                           << "limit. Blocking the current thread until GCS is back";
          while (gcs_is_down_ && !shutdown_) {
            CheckChannelStatus(false);
            std::this_thread::sleep_for(std::chrono::milliseconds(
                ::RayConfig::instance()
                    .gcs_client_check_connection_status_interval_milliseconds()));
          }
          if (shutdown_) {
            callback(Status::Disconnected("GCS client has been disconnected."), reply);
            delete executor;
          } else {
            executor->Retry();
          }
        } else {
          pending_requests_bytes_ += request_bytes;
          auto timeout = timeout_ms == -1 ? absl::InfiniteFuture()
                                          : absl::Now() + absl::Milliseconds(timeout_ms);
          pending_requests_.emplace(timeout, std::make_pair(executor, request_bytes));
        }
      }
    };
    auto operation = [prepare_async_function,
                      &grpc_client,
                      call_name,
                      request,
                      operation_callback,
                      timeout_ms]() {
      grpc_client.template CallMethod<Request, Reply>(
          prepare_async_function, request, operation_callback, call_name, timeout_ms);
    };
    executor->Execute(std::move(operation));
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

  /// Get internal config of the node from the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                             GetInternalConfig,
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

  void Shutdown() {
    if (!shutdown_.exchange(true)) {
      // First call to shut down this GCS RPC client.
      absl::MutexLock lock(&timer_mu_);
      timer_->cancel();
    } else {
      RAY_LOG(DEBUG) << "GCS RPC client has already shutdown.";
    }
  }

  std::pair<std::string, int64_t> GetAddress() const {
    return std::make_pair(gcs_address_, gcs_port_);
  }

  std::shared_ptr<grpc::Channel> GetChannel() const { return channel_; }

 private:
  void SetupCheckTimer() {
    auto duration = boost::posix_time::milliseconds(
        ::RayConfig::instance()
            .gcs_client_check_connection_status_interval_milliseconds());
    absl::MutexLock lock(&timer_mu_);
    timer_->expires_from_now(duration);
    timer_->async_wait([this](boost::system::error_code error) {
      if (error == boost::system::errc::success) {
        CheckChannelStatus();
      }
    });
  }

  void CheckChannelStatus(bool reset_timer = true) {
    if (shutdown_) {
      return;
    }

    auto status = channel_->GetState(false);
    // https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html
    // https://grpc.github.io/grpc/core/connectivity__state_8h_source.html
    if (status != GRPC_CHANNEL_READY) {
      RAY_LOG(DEBUG) << "GCS channel status: " << status;
    }

    // We need to cleanup all the pending requests which are timeout.
    auto now = absl::Now();
    while (!pending_requests_.empty()) {
      auto iter = pending_requests_.begin();
      if (iter->first > now) {
        break;
      }
      auto [executor, request_bytes] = iter->second;
      executor->Abort(
          ray::Status::TimedOut("Timed out while waiting for GCS to become available."));
      pending_requests_bytes_ -= request_bytes;
      delete executor;
      pending_requests_.erase(iter);
    }

    switch (status) {
    case GRPC_CHANNEL_TRANSIENT_FAILURE:
    case GRPC_CHANNEL_CONNECTING:
      if (!gcs_is_down_) {
        gcs_is_down_ = true;
      } else {
        if (absl::ToInt64Seconds(absl::Now() - gcs_last_alive_time_) >=
            ::RayConfig::instance().gcs_rpc_server_reconnect_timeout_s()) {
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
        }
      }
      break;
    case GRPC_CHANNEL_SHUTDOWN:
      RAY_CHECK(shutdown_) << "Channel shoud never go to this status.";
      break;
    case GRPC_CHANNEL_READY:
    case GRPC_CHANNEL_IDLE:
      gcs_last_alive_time_ = absl::Now();
      gcs_is_down_ = false;
      // Retry the one queued.
      while (!pending_requests_.empty()) {
        pending_requests_.begin()->second.first->Retry();
        pending_requests_.erase(pending_requests_.begin());
      }
      pending_requests_bytes_ = 0;
      break;
    default:
      RAY_LOG(FATAL) << "Not covered status: " << status;
    }
    SetupCheckTimer();
  }

  const std::string gcs_address_;
  const int64_t gcs_port_;

  instrumented_io_context *const io_context_;

  // Timer can be called from either the GCS RPC event loop, or the application's
  // main thread. It needs to be protected by a mutex.
  absl::Mutex timer_mu_;
  const std::unique_ptr<boost::asio::deadline_timer> timer_;

  /// The gRPC-generated stub.
  std::unique_ptr<GrpcClient<JobInfoGcsService>> job_info_grpc_client_;
  std::unique_ptr<GrpcClient<ActorInfoGcsService>> actor_info_grpc_client_;
  std::unique_ptr<GrpcClient<NodeInfoGcsService>> node_info_grpc_client_;
  std::unique_ptr<GrpcClient<NodeResourceInfoGcsService>> node_resource_info_grpc_client_;
  std::unique_ptr<GrpcClient<WorkerInfoGcsService>> worker_info_grpc_client_;
  std::unique_ptr<GrpcClient<PlacementGroupInfoGcsService>>
      placement_group_info_grpc_client_;
  std::unique_ptr<GrpcClient<InternalKVGcsService>> internal_kv_grpc_client_;
  std::unique_ptr<GrpcClient<InternalPubSubGcsService>> internal_pubsub_grpc_client_;
  std::unique_ptr<GrpcClient<TaskInfoGcsService>> task_info_grpc_client_;
  std::unique_ptr<GrpcClient<RuntimeEnvGcsService>> runtime_env_grpc_client_;
  std::unique_ptr<GrpcClient<autoscaler::AutoscalerStateService>>
      autoscaler_state_grpc_client_;

  std::shared_ptr<grpc::Channel> channel_;
  bool gcs_is_down_ = false;
  absl::Time gcs_last_alive_time_ = absl::Now();

  std::atomic<bool> shutdown_ = false;
  absl::btree_multimap<absl::Time, std::pair<Executor *, size_t>> pending_requests_;
  size_t pending_requests_bytes_ = 0;

  friend class GcsClientReconnectionTest;
  FRIEND_TEST(GcsClientReconnectionTest, ReconnectionBackoff);
};

}  // namespace rpc
}  // namespace ray
