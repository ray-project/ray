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

#include "ray/raylet_client/raylet_client.h"

namespace ray {

class FakeRayletClient : public RayletClientInterface {
 public:
  void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override {}

  void RequestWorkerLease(
      const rpc::TaskSpec &task_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size = -1,
      const bool is_selected_based_on_locality = false) override {}

  ray::Status ReturnWorker(int worker_port,
                           const WorkerID &worker_id,
                           bool disconnect_worker,
                           const std::string &disconnect_worker_error_detail,
                           bool worker_exiting) override {
    return Status::OK();
  }

  void PrestartWorkers(
      const rpc::PrestartWorkersRequest &request,
      const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) override {}

  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) override {
  }

  void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {}

  void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
      override {}

  void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
      override {}

  void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
      override {}

  void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) override {}

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

  void GetTaskFailureCause(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::GetTaskFailureCauseReply> &callback) override {}

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
                   const rpc::ClientCallback<rpc::DrainRayletReply> &callback) override {}

  void CancelTasksWithResourceShapes(
      const std::vector<google::protobuf::Map<std::string, double>> &resource_shapes,
      const rpc::ClientCallback<rpc::CancelTasksWithResourceShapesReply> &callback)
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
};

}  // namespace ray
