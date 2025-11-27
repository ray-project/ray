// Copyright 2025 The Ray Authors.
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

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/time/clock.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/status.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray {
namespace rpc {

class FakeRayletClient : public RayletClientInterface {
 public:
  void PinObjectIDs(const Address &caller_address,
                    const std::vector<ObjectID> &object_ids,
                    const ObjectID &generator_id,
                    const ClientCallback<PinObjectIDsReply> &callback) override {}

  void RequestWorkerLease(const LeaseSpec &lease_spec,
                          bool grant_or_reject,
                          const ClientCallback<RequestWorkerLeaseReply> &callback,
                          const int64_t backlog_size = -1,
                          const bool is_selected_based_on_locality = false) override {
    num_workers_requested += 1;
    callbacks.push_back(callback);
  }

  void ReturnWorkerLease(int worker_port,
                         const LeaseID &lease_id,
                         bool disconnect_worker,
                         const std::string &disconnect_worker_error_detail,
                         bool worker_exiting) override {
    if (disconnect_worker) {
      num_workers_disconnected++;
    } else {
      num_workers_returned++;
    }
  }

  void PrestartWorkers(const PrestartWorkersRequest &request,
                       const ClientCallback<PrestartWorkersReply> &callback) override {}

  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const ClientCallback<ReleaseUnusedActorWorkersReply> &callback) override {
    num_release_unused_workers += 1;
    release_callbacks.push_back(callback);
  }

  void CancelWorkerLease(
      const LeaseID &lease_id,
      const ClientCallback<CancelWorkerLeaseReply> &callback) override {
    num_leases_canceled += 1;
    cancel_callbacks.push_back(callback);
  }

  bool GrantWorkerLease() {
    return GrantWorkerLease("", 0, WorkerID::FromRandom(), node_id_, NodeID::Nil());
  }

  bool GrantWorkerLease(const std::string &address,
                        int port,
                        const WorkerID &worker_id,
                        const NodeID &node_id,
                        const NodeID &retry_at_node_id,
                        Status status = Status::OK(),
                        bool rejected = false) {
    RequestWorkerLeaseReply reply;
    if (!retry_at_node_id.IsNil()) {
      reply.mutable_retry_at_raylet_address()->set_ip_address(address);
      reply.mutable_retry_at_raylet_address()->set_port(port);
      reply.mutable_retry_at_raylet_address()->set_node_id(retry_at_node_id.Binary());
    } else {
      reply.mutable_worker_address()->set_ip_address(address);
      reply.mutable_worker_address()->set_port(port);
      reply.mutable_worker_address()->set_node_id(node_id.Binary());
      reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
    }
    if (rejected) {
      reply.set_rejected(true);
      auto resources_data = reply.mutable_resources_data();
      resources_data->set_node_id(node_id.Binary());
      resources_data->set_resources_normal_task_changed(true);
      auto &normal_task_map = *(resources_data->mutable_resources_normal_task());
      normal_task_map[kMemory_ResourceLabel] =
          static_cast<double>(std::numeric_limits<int>::max());
      resources_data->set_resources_normal_task_timestamp(absl::GetCurrentTimeNanos());
    }

    if (callbacks.size() == 0) {
      return false;
    } else {
      auto callback = callbacks.front();
      callback(status, std::move(reply));
      callbacks.pop_front();
      return true;
    }
  }

  bool ReplyCancelWorkerLease(bool success = true) {
    CancelWorkerLeaseReply reply;
    reply.set_success(success);
    if (cancel_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = cancel_callbacks.front();
      callback(Status::OK(), std::move(reply));
      cancel_callbacks.pop_front();
      return true;
    }
  }

  bool ReplyReleaseUnusedActorWorkers() {
    ReleaseUnusedActorWorkersReply reply;
    if (release_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = release_callbacks.front();
      callback(Status::OK(), std::move(reply));
      release_callbacks.pop_front();
      return true;
    }
  }

  bool ReplyDrainRaylet() {
    if (drain_raylet_callbacks.size() == 0) {
      return false;
    } else {
      DrainRayletReply reply;
      reply.set_is_accepted(true);
      auto callback = drain_raylet_callbacks.front();
      callback(Status::OK(), std::move(reply));
      drain_raylet_callbacks.pop_front();
      return true;
    }
  }

  void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ClientCallback<PrepareBundleResourcesReply> &callback) override {
    num_lease_requested += 1;
    lease_callbacks.push_back(callback);
  }

  void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ClientCallback<CommitBundleResourcesReply> &callback) override {
    num_commit_requested += 1;
    commit_callbacks.push_back(callback);
  }

  void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ClientCallback<CancelResourceReserveReply> &callback) override {
    num_return_requested += 1;
    return_callbacks.push_back(callback);
  }

  void ReleaseUnusedBundles(
      const std::vector<Bundle> &bundles_in_use,
      const ClientCallback<ReleaseUnusedBundlesReply> &callback) override {
    ++num_release_unused_bundles_requested;
  }

  bool GrantPrepareBundleResources(bool success = true,
                                   const Status &status = Status::OK()) {
    PrepareBundleResourcesReply reply;
    reply.set_success(success);
    if (lease_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = lease_callbacks.front();
      callback(status, std::move(reply));
      lease_callbacks.pop_front();
      return true;
    }
  }

  bool GrantCommitBundleResources(const Status &status = Status::OK()) {
    CommitBundleResourcesReply reply;
    if (commit_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = commit_callbacks.front();
      callback(status, std::move(reply));
      commit_callbacks.pop_front();
      return true;
    }
  }

  bool GrantCancelResourceReserve(bool success = true) {
    Status status = Status::OK();
    CancelResourceReserveReply reply;
    if (return_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = return_callbacks.front();
      callback(status, std::move(reply));
      return_callbacks.pop_front();
      return true;
    }
  }

  void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<WorkerBacklogReport> &backlog_reports) override {}

  void GetResourceLoad(const ClientCallback<GetResourceLoadReply> &callback) override {}

  void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const ClientCallback<RegisterMutableObjectReply> &callback) override {}

  void PushMutableObject(
      const ObjectID &writer_object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      void *metadata,
      const ClientCallback<PushMutableObjectReply> &callback) override {}

  void GetWorkerFailureCause(
      const LeaseID &lease_id,
      const ClientCallback<GetWorkerFailureCauseReply> &callback) override {
    GetWorkerFailureCauseReply reply;
    callback(Status::OK(), std::move(reply));
    num_get_task_failure_causes += 1;
  }

  void GetSystemConfig(const ClientCallback<GetSystemConfigReply> &callback) override {}

  void NotifyGCSRestart(const ClientCallback<NotifyGCSRestartReply> &callback) override {}

  void ShutdownRaylet(const NodeID &node_id,
                      bool graceful,
                      const ClientCallback<ShutdownRayletReply> &callback) override {}

  void DrainRaylet(const autoscaler::DrainNodeReason &reason,
                   const std::string &reason_message,
                   int64_t deadline_timestamp_ms,
                   const ClientCallback<DrainRayletReply> &callback) override {
    DrainRayletReply reply;
    reply.set_is_accepted(true);
    drain_raylet_callbacks.push_back(callback);
  }

  void CancelLeasesWithResourceShapes(
      const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
      const ClientCallback<CancelLeasesWithResourceShapesReply> &callback) override {}

  void IsLocalWorkerDead(
      const WorkerID &worker_id,
      const ClientCallback<IsLocalWorkerDeadReply> &callback) override {}

  std::shared_ptr<grpc::Channel> GetChannel() const override { return nullptr; }

  void GetNodeStats(const GetNodeStatsRequest &request,
                    const ClientCallback<GetNodeStatsReply> &callback) override {}

  void KillLocalActor(const KillLocalActorRequest &request,
                      const ClientCallback<KillLocalActorReply> &callback) override {
    killed_actors.push_back(ActorID::FromBinary(request.intended_actor_id()));
  }

  void GlobalGC(const ClientCallback<GlobalGCReply> &callback) override {}

  int64_t GetPinsInFlight() const override { return 0; }

  int num_workers_requested = 0;
  int num_workers_returned = 0;
  int num_workers_disconnected = 0;
  int num_leases_canceled = 0;
  int num_release_unused_workers = 0;
  int num_get_task_failure_causes = 0;
  NodeID node_id_ = NodeID::FromRandom();
  std::vector<ActorID> killed_actors;
  std::list<ClientCallback<DrainRayletReply>> drain_raylet_callbacks = {};
  std::list<ClientCallback<RequestWorkerLeaseReply>> callbacks = {};
  std::list<ClientCallback<CancelWorkerLeaseReply>> cancel_callbacks = {};
  std::list<ClientCallback<ReleaseUnusedActorWorkersReply>> release_callbacks = {};
  int num_lease_requested = 0;
  int num_return_requested = 0;
  int num_commit_requested = 0;

  int num_release_unused_bundles_requested = 0;
  std::list<ClientCallback<PrepareBundleResourcesReply>> lease_callbacks = {};
  std::list<ClientCallback<CommitBundleResourcesReply>> commit_callbacks = {};
  std::list<ClientCallback<CancelResourceReserveReply>> return_callbacks = {};
};

}  // namespace rpc
}  // namespace ray
