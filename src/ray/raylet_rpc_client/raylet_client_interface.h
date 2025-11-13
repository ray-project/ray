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
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

// Maps from resource name to its allocation.
using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;

namespace grpc {
class Channel;
}

namespace ray {

// Forward declarations.
class Status;
class WorkerID;
class ObjectID;
class LeaseID;
class NodeID;
class BundleSpecification;

class RayletClientInterface {
 public:
  /// Request to a raylet to pin a plasma object. The callback will be sent via gRPC.
  virtual void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) = 0;

  /// Requests a worker from the raylet. The callback will be sent via gRPC.
  /// \param lease_spec Lease that is requested by the owner.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param callback: The callback to call when the request finishes.
  /// \param backlog_size The queue length for the given shape on the CoreWorker.
  /// \param lease_id Unique lease ID for this worker lease request.
  virtual void RequestWorkerLease(
      const rpc::LeaseSpec &lease_spec,
      bool grant_or_reject,
      const rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size = -1,
      const bool is_selected_based_on_locality = false) = 0;

  /// Returns a worker to the raylet.
  /// \param worker_port The local port of the worker on the raylet node.
  /// \param lease_id The unique lease id of the worker on the raylet node.
  /// \param disconnect_worker Whether the raylet should disconnect the worker.
  /// \param worker_exiting Whether the worker is exiting and cannot be reused.
  virtual void ReturnWorkerLease(int worker_port,
                                 const LeaseID &lease_id,
                                 bool disconnect_worker,
                                 const std::string &disconnect_worker_error_detail,
                                 bool worker_exiting) = 0;

  /// Request the raylet to prestart workers. In `request` we can set the worker's owner,
  /// runtime env info and number of workers.
  ///
  virtual void PrestartWorkers(
      const rpc::PrestartWorkersRequest &request,
      const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) = 0;

  /// Notify raylets to release unused workers.
  /// \param workers_in_use Workers currently in use.
  /// \param callback Callback that will be called after raylet completes the release of
  /// unused workers. \return ray::Status
  virtual void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) = 0;

  virtual void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) = 0;

  /// Report the backlog size of a given worker and a given scheduling class to the
  /// raylet.
  /// \param worker_id The ID of the worker that reports the backlog size.
  /// \param backlog_reports The backlog report for each scheduling class
  virtual void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) = 0;

  virtual void GetWorkerFailureCause(
      const LeaseID &lease_id,
      const ray::rpc::ClientCallback<ray::rpc::GetWorkerFailureCauseReply> &callback) = 0;

  /// Request a raylet to prepare resources of given bundles for atomic placement group
  /// creation. This is used for the first phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param bundle_specs Bundles to be scheduled at this raylet.
  /// \return ray::Status
  virtual void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply>
          &callback) = 0;

  /// Request a raylet to commit resources of given bundles for atomic placement group
  /// creation. This is used for the second phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param bundle_specs Bundles to be scheduled at this raylet.
  /// \return ray::Status
  virtual void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) = 0;

  virtual void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback) = 0;

  virtual void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) = 0;

  virtual void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) = 0;
  /// Registers a mutable object on this node so that it can be read. Writes are performed
  /// on a remote node. This local node creates a mapping from `object_id` ->
  /// `reader_ref`.
  ///
  /// \param writer_object_id The object ID of the mutable object on the remote node that
  /// is written to.
  /// \param num_readers The number of readers that will read the object on this local
  /// node.
  /// \param reader_object_id The object ID of the mutable object that is read on this
  /// local node.
  /// \param callback This callback is executed to send a reply to the remote
  /// node once the mutable object is registered.
  virtual void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const rpc::ClientCallback<rpc::RegisterMutableObjectReply> &callback) = 0;

  /// Handles a mutable object write that was performed on a remote node and is being
  /// transferred to this node so that it can be read.
  ///
  /// \param writer_object_id The object ID of the mutable object on the remote node that
  /// is written to. This is *not* the object ID of the corresponding mutable object on
  /// this local node.
  /// \param data_size The size of the data to write to the mutable object on this local
  /// node.
  /// \param metadata_size The size of the metadata to write to the mutable object on this
  /// local node.
  /// \param data The data to write to the mutable object on this local node.
  /// \param metadata The metadata to write to the mutable object on this local node.
  /// \param callback This callback is executed to send a reply to the remote node once
  /// the mutable object is transferred.
  virtual void PushMutableObject(
      const ObjectID &writer_object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      void *metadata,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) = 0;

  /// Get the system config from Raylet.
  /// \param callback Callback that will be called after raylet replied the system config.
  virtual void GetSystemConfig(
      const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) = 0;

  virtual void GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback) = 0;

  virtual void NotifyGCSRestart(
      const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) = 0;

  virtual void ShutdownRaylet(
      const NodeID &node_id,
      bool graceful,
      const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) = 0;

  virtual void DrainRaylet(
      const rpc::autoscaler::DrainNodeReason &reason,
      const std::string &reason_message,
      int64_t deadline_timestamp_ms,
      const rpc::ClientCallback<rpc::DrainRayletReply> &callback) = 0;

  virtual void CancelLeasesWithResourceShapes(
      const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
      const rpc::ClientCallback<rpc::CancelLeasesWithResourceShapesReply> &callback) = 0;

  virtual void IsLocalWorkerDead(
      const WorkerID &worker_id,
      const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) = 0;

  virtual std::shared_ptr<grpc::Channel> GetChannel() const = 0;

  virtual void GetNodeStats(
      const rpc::GetNodeStatsRequest &request,
      const rpc::ClientCallback<rpc::GetNodeStatsReply> &callback) = 0;

  virtual void KillLocalActor(
      const rpc::KillLocalActorRequest &request,
      const rpc::ClientCallback<rpc::KillLocalActorReply> &callback) = 0;

  virtual int64_t GetPinsInFlight() const = 0;

  virtual ~RayletClientInterface() = default;
};

}  // namespace ray
