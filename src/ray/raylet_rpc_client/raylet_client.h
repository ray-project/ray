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

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

// Maps from resource name to its allocation.
using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;

namespace ray {
namespace rpc {

/// Raylet client is responsible for communication with raylet. It implements
/// [RayletClientInterface] and works on worker registration, lease management, etc.
class RayletClient : public RayletClientInterface {
 public:
  /// Connect to the raylet.
  ///
  /// \param address The IP address of the worker.
  /// \param client_call_manager The client call manager to use for the grpc connection.
  /// \param raylet_unavailable_timeout_callback callback to be called when the raylet is
  /// unavailable for a certain period of time.
  explicit RayletClient(const rpc::Address &address,
                        rpc::ClientCallManager &client_call_manager,
                        std::function<void()> raylet_unavailable_timeout_callback);

  std::shared_ptr<grpc::Channel> GetChannel() const override;

  void RequestWorkerLease(
      const rpc::LeaseSpec &lease_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size,
      const bool is_selected_based_on_locality) override;

  void ReturnWorkerLease(int worker_port,
                         const LeaseID &lease_id,
                         bool disconnect_worker,
                         const std::string &disconnect_worker_error_detail,
                         bool worker_exiting) override;

  void PrestartWorkers(
      const ray::rpc::PrestartWorkersRequest &request,
      const ray::rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) override;

  void GetWorkerFailureCause(
      const LeaseID &lease_id,
      const ray::rpc::ClientCallback<ray::rpc::GetWorkerFailureCauseReply> &callback)
      override;

  void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const ray::rpc::ClientCallback<ray::rpc::RegisterMutableObjectReply> &callback)
      override;

  void PushMutableObject(const ObjectID &writer_object_id,
                         uint64_t data_size,
                         uint64_t metadata_size,
                         void *data,
                         void *metadata,
                         const ray::rpc::ClientCallback<ray::rpc::PushMutableObjectReply>
                             &callback) override;

  void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override;

  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) override;

  void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override;

  void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
      override;

  void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
      override;

  void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
      override;

  void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) override;

  void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override;

  void ShutdownRaylet(
      const NodeID &node_id,
      bool graceful,
      const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) override;

  void DrainRaylet(const rpc::autoscaler::DrainNodeReason &reason,
                   const std::string &reason_message,
                   int64_t deadline_timestamp_ms,
                   const rpc::ClientCallback<rpc::DrainRayletReply> &callback) override;

  void CancelLeasesWithResourceShapes(
      const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
      const rpc::ClientCallback<rpc::CancelLeasesWithResourceShapesReply> &callback)
      override;

  void IsLocalWorkerDead(
      const WorkerID &worker_id,
      const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) override;

  void GetSystemConfig(
      const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) override;

  void GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback) override;

  void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override;

  void NotifyGCSRestart(
      const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) override;

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

  int64_t GetPinsInFlight() const override { return pins_in_flight_.load(); }

  void GetNodeStats(const rpc::GetNodeStatsRequest &request,
                    const rpc::ClientCallback<rpc::GetNodeStatsReply> &callback) override;

  void KillLocalActor(
      const rpc::KillLocalActorRequest &request,
      const rpc::ClientCallback<rpc::KillLocalActorReply> &callback) override;

  /// Get the worker pids from raylet.
  /// \param callback The callback to set the worker pids.
  /// \param timeout_ms The timeout in milliseconds.
  void GetWorkerPIDs(const gcs::OptionalItemCallback<std::vector<int32_t>> &callback,
                     int64_t timeout_ms);

 protected:
  /// gRPC client to the NodeManagerService.
  std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client_;

  /// Retryable gRPC client to monitor channel health and trigger timeout callbacks.
  std::shared_ptr<rpc::RetryableGrpcClient> retryable_grpc_client_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  /// The number of object ID pin RPCs currently in flight.
  std::atomic<int64_t> pins_in_flight_ = 0;
};

}  // namespace rpc
}  // namespace ray
