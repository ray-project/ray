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

#include "ray/common/network_util.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class GcsRpcClient;

/// \class Executor
/// Executor saves operation and support retries.
class Executor {
 public:
  explicit Executor(GcsRpcClient *gcs_rpc_client) : gcs_rpc_client_(gcs_rpc_client) {}

  /// This function is used to execute the given operation.
  ///
  /// \param operation The operation to be executed.
  void Execute(const std::function<void(GcsRpcClient *gcs_rpc_client)> &operation) {
    operation_ = operation;
    operation(gcs_rpc_client_);
  }

  /// This function is used to retry the given operation.
  void Retry() { operation_(gcs_rpc_client_); }

 private:
  GcsRpcClient *gcs_rpc_client_;
  std::function<void(GcsRpcClient *gcs_rpc_client)> operation_;
};

// Define a void GCS RPC client method.
#define VOID_GCS_RPC_CLIENT_METHOD(SERVICE, METHOD, grpc_client, SPECS)                \
  void METHOD(const METHOD##Request &request,                                          \
              const ClientCallback<METHOD##Reply> &callback) SPECS {                   \
    auto executor = new Executor(this);                                                \
    auto operation_callback = [this, request, callback, executor](                     \
                                  const ray::Status &status,                           \
                                  const METHOD##Reply &reply) {                        \
      if (!status.IsIOError()) {                                                       \
        auto status =                                                                  \
            reply.status().code() == (int)StatusCode::OK                               \
                ? Status()                                                             \
                : Status(StatusCode(reply.status().code()), reply.status().message()); \
        callback(status, reply);                                                       \
        delete executor;                                                               \
      } else {                                                                         \
        gcs_service_failure_detected_(GcsServiceFailureType::RPC_DISCONNECT);          \
        executor->Retry();                                                             \
      }                                                                                \
    };                                                                                 \
    auto operation = [request, operation_callback](GcsRpcClient *gcs_rpc_client) {     \
      RAY_UNUSED(INVOKE_RPC_CALL(SERVICE, METHOD, request, operation_callback,         \
                                 gcs_rpc_client->grpc_client));                        \
    };                                                                                 \
    executor->Execute(operation);                                                      \
  }

/// Client used for communicating with gcs server.
class GcsRpcClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of gcs server.
  /// \param[in] port Port of the gcs server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  /// \param[in] gcs_service_failure_detected The function is used to redo subscription
  /// and reconnect to GCS RPC server when gcs service failure is detected.
  GcsRpcClient(
      const std::string &address, const int port, ClientCallManager &client_call_manager,
      std::function<void(GcsServiceFailureType)> gcs_service_failure_detected = nullptr)
      : gcs_service_failure_detected_(std::move(gcs_service_failure_detected)) {
    Reset(address, port, client_call_manager);
  };

  void Reset(const std::string &address, const int port,
             ClientCallManager &client_call_manager) {
    job_info_grpc_client_ = std::unique_ptr<GrpcClient<JobInfoGcsService>>(
        new GrpcClient<JobInfoGcsService>(address, port, client_call_manager));
    actor_info_grpc_client_ = std::unique_ptr<GrpcClient<ActorInfoGcsService>>(
        new GrpcClient<ActorInfoGcsService>(address, port, client_call_manager));
    node_info_grpc_client_ = std::unique_ptr<GrpcClient<NodeInfoGcsService>>(
        new GrpcClient<NodeInfoGcsService>(address, port, client_call_manager));
    object_info_grpc_client_ = std::unique_ptr<GrpcClient<ObjectInfoGcsService>>(
        new GrpcClient<ObjectInfoGcsService>(address, port, client_call_manager));
    task_info_grpc_client_ = std::unique_ptr<GrpcClient<TaskInfoGcsService>>(
        new GrpcClient<TaskInfoGcsService>(address, port, client_call_manager));
    stats_grpc_client_ = std::unique_ptr<GrpcClient<StatsGcsService>>(
        new GrpcClient<StatsGcsService>(address, port, client_call_manager));
    worker_info_grpc_client_ = std::unique_ptr<GrpcClient<WorkerInfoGcsService>>(
        new GrpcClient<WorkerInfoGcsService>(address, port, client_call_manager));
    placement_group_info_grpc_client_ =
        std::unique_ptr<GrpcClient<PlacementGroupInfoGcsService>>(
            new GrpcClient<PlacementGroupInfoGcsService>(address, port,
                                                         client_call_manager));
  }

  /// Add job info to gcs server.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService, AddJob, job_info_grpc_client_, )

  /// Mark job as finished to gcs server.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService, MarkJobFinished, job_info_grpc_client_, )

  /// Get information of all jobs from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(JobInfoGcsService, GetAllJobInfo, job_info_grpc_client_, )

  /// Register actor via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, RegisterActor,
                             actor_info_grpc_client_, )

  /// Create actor via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, CreateActor, actor_info_grpc_client_, )

  /// Get actor data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, GetActorInfo, actor_info_grpc_client_, )

  /// Get actor data from GCS Service by name.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, GetNamedActorInfo,
                             actor_info_grpc_client_, )

  /// Get all actor data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, GetAllActorInfo,
                             actor_info_grpc_client_, )

  /// Register an actor to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, RegisterActorInfo,
                             actor_info_grpc_client_, )

  ///  Update actor info in GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, UpdateActorInfo,
                             actor_info_grpc_client_, )

  ///  Add actor checkpoint data to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, AddActorCheckpoint,
                             actor_info_grpc_client_, )

  ///  Get actor checkpoint data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, GetActorCheckpoint,
                             actor_info_grpc_client_, )

  ///  Get actor checkpoint id data from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ActorInfoGcsService, GetActorCheckpointID,
                             actor_info_grpc_client_, )

  /// Register a node to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, RegisterNode, node_info_grpc_client_, )

  /// Unregister a node from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, UnregisterNode, node_info_grpc_client_, )

  /// Get information of all nodes from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, GetAllNodeInfo, node_info_grpc_client_, )

  /// Report heartbeat of a node to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, ReportHeartbeat,
                             node_info_grpc_client_, )

  /// Get node's resources from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, GetResources, node_info_grpc_client_, )

  /// Update resources of a node in GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, UpdateResources,
                             node_info_grpc_client_, )

  /// Delete resources of a node in GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, DeleteResources,
                             node_info_grpc_client_, )

  /// Set internal config of the cluster in the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, SetInternalConfig,
                             node_info_grpc_client_, )

  /// Get internal config of the node from the GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService, GetInternalConfig,
                             node_info_grpc_client_, )

  /// Get object's locations from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ObjectInfoGcsService, GetObjectLocations,
                             object_info_grpc_client_, )

  /// Get all object's locations from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ObjectInfoGcsService, GetAllObjectLocations,
                             object_info_grpc_client_, )

  /// Add location of object to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ObjectInfoGcsService, AddObjectLocation,
                             object_info_grpc_client_, )

  /// Remove location of object to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(ObjectInfoGcsService, RemoveObjectLocation,
                             object_info_grpc_client_, )

  /// Add a task to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, AddTask, task_info_grpc_client_, )

  /// Get task information from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, GetTask, task_info_grpc_client_, )

  /// Delete tasks from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, DeleteTasks, task_info_grpc_client_, )

  /// Add a task lease to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, AddTaskLease, task_info_grpc_client_, )

  /// Get task lease information from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, GetTaskLease, task_info_grpc_client_, )

  /// Attempt task reconstruction to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(TaskInfoGcsService, AttemptTaskReconstruction,
                             task_info_grpc_client_, )

  /// Add profile data to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(StatsGcsService, AddProfileData, stats_grpc_client_, )

  /// Get information of all profiles from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(StatsGcsService, GetAllProfileInfo, stats_grpc_client_, )

  /// Report a worker failure to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService, ReportWorkerFailure,
                             worker_info_grpc_client_, )

  /// Get worker information from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService, GetWorkerInfo,
                             worker_info_grpc_client_, )

  /// Get information of all workers from GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService, GetAllWorkerInfo,
                             worker_info_grpc_client_, )

  /// Add worker information to GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(WorkerInfoGcsService, AddWorkerInfo,
                             worker_info_grpc_client_, )

  /// Create placement group via GCS Service.
  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService, CreatePlacementGroup,
                             placement_group_info_grpc_client_, )

 private:
  std::function<void(GcsServiceFailureType)> gcs_service_failure_detected_;

  /// The gRPC-generated stub.
  std::unique_ptr<GrpcClient<JobInfoGcsService>> job_info_grpc_client_;
  std::unique_ptr<GrpcClient<ActorInfoGcsService>> actor_info_grpc_client_;
  std::unique_ptr<GrpcClient<NodeInfoGcsService>> node_info_grpc_client_;
  std::unique_ptr<GrpcClient<ObjectInfoGcsService>> object_info_grpc_client_;
  std::unique_ptr<GrpcClient<TaskInfoGcsService>> task_info_grpc_client_;
  std::unique_ptr<GrpcClient<StatsGcsService>> stats_grpc_client_;
  std::unique_ptr<GrpcClient<WorkerInfoGcsService>> worker_info_grpc_client_;
  std::unique_ptr<GrpcClient<PlacementGroupInfoGcsService>>
      placement_group_info_grpc_client_;
};

}  // namespace rpc
}  // namespace ray
