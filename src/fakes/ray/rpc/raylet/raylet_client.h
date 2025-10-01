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

#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/rpc/raylet/raylet_client_interface.h"

namespace ray {

class FakeRayletClient : public RayletClientInterface {
 public:
  void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override {}

  void RequestWorkerLease(
      const rpc::LeaseSpec &lease_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
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

  void PrestartWorkers(
      const rpc::PrestartWorkersRequest &request,
      const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) override {}

  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) override {
    num_release_unused_workers += 1;
    release_callbacks.push_back(callback);
  }

  void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
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
    rpc::RequestWorkerLeaseReply reply;
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
    rpc::CancelWorkerLeaseReply reply;
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
    rpc::ReleaseUnusedActorWorkersReply reply;
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
      rpc::DrainRayletReply reply;
      reply.set_is_accepted(true);
      auto callback = drain_raylet_callbacks.front();
      callback(Status::OK(), std::move(reply));
      drain_raylet_callbacks.pop_front();
      return true;
    }
  }

  void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
      override {
    num_lease_requested += 1;
    lease_callbacks.push_back(callback);
  }

  void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
      override {
    num_commit_requested += 1;
    commit_callbacks.push_back(callback);
  }

  void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
      override {
    num_return_requested += 1;
    return_callbacks.push_back(callback);
  }

  void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) override {
    ++num_release_unused_bundles_requested;
  }

  bool GrantPrepareBundleResources(bool success = true,
                                   const Status &status = Status::OK()) {
    rpc::PrepareBundleResourcesReply reply;
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
    rpc::CommitBundleResourcesReply reply;
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
    rpc::CancelResourceReserveReply reply;
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
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override {}

  void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override {}

  void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const rpc::ClientCallback<rpc::RegisterMutableObjectReply> &callback) override {}

  void PushMutableObject(
      const ObjectID &writer_object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      void *metadata,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) override {}

  void GetWorkerFailureCause(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::GetWorkerFailureCauseReply> &callback) override {
    ray::rpc::GetWorkerFailureCauseReply reply;
    callback(Status::OK(), std::move(reply));
    num_get_task_failure_causes += 1;
  }

  void GetSystemConfig(
      const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) override {}

  void NotifyGCSRestart(
      const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) override {}

  void ShutdownRaylet(
      const NodeID &node_id,
      bool graceful,
      const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) override {}

  void DrainRaylet(const rpc::autoscaler::DrainNodeReason &reason,
                   const std::string &reason_message,
                   int64_t deadline_timestamp_ms,
                   const rpc::ClientCallback<rpc::DrainRayletReply> &callback) override {
    rpc::DrainRayletReply reply;
    reply.set_is_accepted(true);
    drain_raylet_callbacks.push_back(callback);
  }

  void CancelLeasesWithResourceShapes(
      const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
      const rpc::ClientCallback<rpc::CancelLeasesWithResourceShapesReply> &callback)
      override {}

  void IsLocalWorkerDead(
      const WorkerID &worker_id,
      const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) override {}

  std::shared_ptr<grpc::Channel> GetChannel() const override { return nullptr; }

  void GetNodeStats(
      const rpc::GetNodeStatsRequest &request,
      const rpc::ClientCallback<rpc::GetNodeStatsReply> &callback) override {}

  void GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback) override {}

  int64_t GetPinsInFlight() const override { return 0; }

  int num_workers_requested = 0;
  int num_workers_returned = 0;
  int num_workers_disconnected = 0;
  int num_leases_canceled = 0;
  int num_release_unused_workers = 0;
  int num_get_task_failure_causes = 0;
  NodeID node_id_ = NodeID::FromRandom();
  std::list<rpc::ClientCallback<rpc::DrainRayletReply>> drain_raylet_callbacks = {};
  std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
  std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
  std::list<rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply>> release_callbacks =
      {};
  int num_lease_requested = 0;
  int num_return_requested = 0;
  int num_commit_requested = 0;

  int num_release_unused_bundles_requested = 0;
  std::list<rpc::ClientCallback<rpc::PrepareBundleResourcesReply>> lease_callbacks = {};
  std::list<rpc::ClientCallback<rpc::CommitBundleResourcesReply>> commit_callbacks = {};
  std::list<rpc::ClientCallback<rpc::CancelResourceReserveReply>> return_callbacks = {};
};

}  // namespace ray
