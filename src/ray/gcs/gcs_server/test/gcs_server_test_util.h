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
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {

struct GcsServerMocker {
  class MockWorkerClient : public rpc::CoreWorkerClientInterface {
   public:
    void PushNormalTask(
        std::unique_ptr<rpc::PushTaskRequest> request,
        const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
      callbacks.push_back(callback);
    }

    bool ReplyPushTask(Status status = Status::OK(), bool exit = false) {
      if (callbacks.size() == 0) {
        return false;
      }
      auto callback = callbacks.front();
      auto reply = rpc::PushTaskReply();
      if (exit) {
        reply.set_worker_exiting(true);
      }
      callback(status, reply);
      callbacks.pop_front();
      return true;
    }

    std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  };

  class MockRayletClient : public RayletClientInterface {
   public:
    /// WorkerLeaseInterface
    ray::Status ReturnWorker(int worker_port,
                             const WorkerID &worker_id,
                             bool disconnect_worker,
                             bool worker_exiting) override {
      if (disconnect_worker) {
        num_workers_disconnected++;
      } else {
        num_workers_returned++;
      }
      return Status::OK();
    }

    void GetTaskFailureCause(
        const TaskID &task_id,
        const ray::rpc::ClientCallback<ray::rpc::GetTaskFailureCauseReply> &callback)
        override {
      ray::rpc::GetTaskFailureCauseReply reply;
      callback(Status::OK(), reply);
      num_get_task_failure_causes += 1;
    }

    std::shared_ptr<grpc::Channel> GetChannel() const override { return nullptr; }

    void ReportWorkerBacklog(
        const WorkerID &worker_id,
        const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override {}

    /// WorkerLeaseInterface
    void RequestWorkerLease(
        const rpc::TaskSpec &spec,
        bool grant_or_reject,
        const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
        const int64_t backlog_size,
        const bool is_selected_based_on_locality) override {
      num_workers_requested += 1;
      callbacks.push_back(callback);
    }

    /// WorkerLeaseInterface
    void ReleaseUnusedWorkers(
        const std::vector<WorkerID> &workers_in_use,
        const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) override {
      num_release_unused_workers += 1;
      release_callbacks.push_back(callback);
    }

    /// WorkerLeaseInterface
    void CancelWorkerLease(
        const TaskID &task_id,
        const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
      num_leases_canceled += 1;
      cancel_callbacks.push_back(callback);
    }

    bool GrantWorkerLease() {
      return GrantWorkerLease("", 0, WorkerID::FromRandom(), node_id, NodeID::Nil());
    }

    void GetResourceLoad(
        const ray::rpc::ClientCallback<ray::rpc::GetResourceLoadReply> &) override {}

    // Trigger reply to RequestWorkerLease.
    bool GrantWorkerLease(const std::string &address,
                          int port,
                          const WorkerID &worker_id,
                          const NodeID &raylet_id,
                          const NodeID &retry_at_raylet_id,
                          Status status = Status::OK(),
                          bool rejected = false) {
      rpc::RequestWorkerLeaseReply reply;
      if (!retry_at_raylet_id.IsNil()) {
        reply.mutable_retry_at_raylet_address()->set_ip_address(address);
        reply.mutable_retry_at_raylet_address()->set_port(port);
        reply.mutable_retry_at_raylet_address()->set_raylet_id(
            retry_at_raylet_id.Binary());
      } else {
        reply.mutable_worker_address()->set_ip_address(address);
        reply.mutable_worker_address()->set_port(port);
        reply.mutable_worker_address()->set_raylet_id(raylet_id.Binary());
        reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
      }
      if (rejected) {
        reply.set_rejected(true);
        auto resources_data = reply.mutable_resources_data();
        resources_data->set_node_id(raylet_id.Binary());
        resources_data->set_resources_normal_task_changed(true);
        auto &normal_task_map = *(resources_data->mutable_resources_normal_task());
        normal_task_map[kMemory_ResourceLabel] = double(std::numeric_limits<int>::max());
        resources_data->set_resources_normal_task_timestamp(absl::GetCurrentTimeNanos());
      }

      if (callbacks.size() == 0) {
        return false;
      } else {
        auto callback = callbacks.front();
        callback(status, reply);
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
        callback(Status::OK(), reply);
        cancel_callbacks.pop_front();
        return true;
      }
    }

    bool ReplyReleaseUnusedWorkers() {
      rpc::ReleaseUnusedWorkersReply reply;
      if (release_callbacks.size() == 0) {
        return false;
      } else {
        auto callback = release_callbacks.front();
        callback(Status::OK(), reply);
        release_callbacks.pop_front();
        return true;
      }
    }

    /// ResourceReserveInterface
    void PrepareBundleResources(
        const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
        const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
        override {
      num_lease_requested += 1;
      lease_callbacks.push_back(callback);
    }

    /// ResourceReserveInterface
    void CommitBundleResources(
        const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
        const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
        override {
      num_commit_requested += 1;
      commit_callbacks.push_back(callback);
    }

    /// ResourceReserveInterface
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

    // Trigger reply to PrepareBundleResources.
    bool GrantPrepareBundleResources(bool success = true,
                                     const Status &status = Status::OK()) {
      rpc::PrepareBundleResourcesReply reply;
      reply.set_success(success);
      if (lease_callbacks.size() == 0) {
        return false;
      } else {
        auto callback = lease_callbacks.front();
        callback(status, reply);
        lease_callbacks.pop_front();
        return true;
      }
    }

    // Trigger reply to CommitBundleResources.
    bool GrantCommitBundleResources(bool success = true,
                                    const Status &status = Status::OK()) {
      rpc::CommitBundleResourcesReply reply;
      if (commit_callbacks.size() == 0) {
        return false;
      } else {
        auto callback = commit_callbacks.front();
        callback(status, reply);
        commit_callbacks.pop_front();
        return true;
      }
    }

    // Trigger reply to CancelResourceReserve.
    bool GrantCancelResourceReserve(bool success = true) {
      Status status = Status::OK();
      rpc::CancelResourceReserveReply reply;
      if (return_callbacks.size() == 0) {
        return false;
      } else {
        auto callback = return_callbacks.front();
        callback(status, reply);
        return_callbacks.pop_front();
        return true;
      }
    }

    /// PinObjectsInterface
    void PinObjectIDs(
        const rpc::Address &caller_address,
        const std::vector<ObjectID> &object_ids,
        const ObjectID &generator_id,
        const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override {}

    /// DependencyWaiterInterface
    ray::Status WaitForDirectActorCallArgs(
        const std::vector<rpc::ObjectReference> &references, int64_t tag) override {
      return ray::Status::OK();
    }

    void GetSystemConfig(const ray::rpc::ClientCallback<ray::rpc::GetSystemConfigReply>
                             &callback) override {}

    /// ResourceUsageInterface
    void RequestResourceReport(
        const rpc::ClientCallback<rpc::RequestResourceReportReply> &callback) override {
      RAY_CHECK(false) << "Unused";
    };

    /// ResourceUsageInterface
    void UpdateResourceUsage(
        std::string &address,
        const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &callback) override {
      RAY_CHECK(false) << "Unused";
    };

    /// ShutdownRaylet
    void ShutdownRaylet(
        const NodeID &node_id,
        bool graceful,
        const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) override{};

    void NotifyGCSRestart(
        const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) override{};

    ~MockRayletClient() {}

    int num_workers_requested = 0;
    int num_workers_returned = 0;
    int num_workers_disconnected = 0;
    int num_leases_canceled = 0;
    int num_release_unused_workers = 0;
    int num_get_task_failure_causes = 0;
    NodeID node_id = NodeID::FromRandom();
    std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
    std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
    std::list<rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply>> release_callbacks = {};
    int num_lease_requested = 0;
    int num_return_requested = 0;
    int num_commit_requested = 0;

    int num_release_unused_bundles_requested = 0;
    std::list<rpc::ClientCallback<rpc::PrepareBundleResourcesReply>> lease_callbacks = {};
    std::list<rpc::ClientCallback<rpc::CommitBundleResourcesReply>> commit_callbacks = {};
    std::list<rpc::ClientCallback<rpc::CancelResourceReserveReply>> return_callbacks = {};
  };

  class MockedGcsActorScheduler : public gcs::GcsActorScheduler {
   public:
    using gcs::GcsActorScheduler::GcsActorScheduler;

    void TryLeaseWorkerFromNodeAgain(std::shared_ptr<gcs::GcsActor> actor,
                                     std::shared_ptr<rpc::GcsNodeInfo> node) {
      DoRetryLeasingWorkerFromNode(std::move(actor), std::move(node));
    }

   protected:
    void RetryLeasingWorkerFromNode(std::shared_ptr<gcs::GcsActor> actor,
                                    std::shared_ptr<rpc::GcsNodeInfo> node) override {
      ++num_retry_leasing_count_;
      if (num_retry_leasing_count_ <= 1) {
        DoRetryLeasingWorkerFromNode(actor, node);
      }
    }

    void RetryCreatingActorOnWorker(std::shared_ptr<gcs::GcsActor> actor,
                                    std::shared_ptr<GcsLeasedWorker> worker) override {
      ++num_retry_creating_count_;
      DoRetryCreatingActorOnWorker(actor, worker);
    }

   public:
    int num_retry_leasing_count_ = 0;
    int num_retry_creating_count_ = 0;
  };

  class MockedGcsPlacementGroupScheduler : public gcs::GcsPlacementGroupScheduler {
   public:
    using gcs::GcsPlacementGroupScheduler::GcsPlacementGroupScheduler;

    size_t GetWaitingRemovedBundlesSize() { return waiting_removed_bundles_.size(); }

   protected:
    friend class GcsPlacementGroupSchedulerTest;
    FRIEND_TEST(GcsPlacementGroupSchedulerTest, TestCheckingWildcardResource);
  };
  class MockedGcsActorTable : public gcs::GcsActorTable {
   public:
    MockedGcsActorTable(std::shared_ptr<gcs::StoreClient> store_client)
        : GcsActorTable(store_client) {}

    Status Put(const ActorID &key,
               const rpc::ActorTableData &value,
               const gcs::StatusCallback &callback) override {
      auto status = Status::OK();
      callback(status);
      return status;
    }

   private:
    instrumented_io_context main_io_service_;
    std::shared_ptr<gcs::StoreClient> store_client_ =
        std::make_shared<gcs::InMemoryStoreClient>(main_io_service_);
  };

  class MockedNodeInfoAccessor : public gcs::NodeInfoAccessor {
   public:
    Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                        const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status DrainSelf() override { return Status::NotImplemented(""); }

    const NodeID &GetSelfId() const override {
      static NodeID node_id;
      return node_id;
    }

    const rpc::GcsNodeInfo &GetSelfInfo() const override {
      static rpc::GcsNodeInfo node_info;
      return node_info;
    }

    Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                         const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncDrainNode(const NodeID &node_id,
                          const gcs::StatusCallback &callback) override {
      if (callback) {
        callback(Status::OK());
      }
      return Status::OK();
    }

    Status AsyncGetAll(
        const gcs::MultiItemCallback<rpc::GcsNodeInfo> &callback) override {
      if (callback) {
        callback(Status::OK(), {});
      }
      return Status::OK();
    }

    Status AsyncSubscribeToNodeChange(
        const gcs::SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    const rpc::GcsNodeInfo *Get(const NodeID &node_id,
                                bool filter_dead_nodes = true) const override {
      return nullptr;
    }

    const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &GetAll() const override {
      static absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> node_info_list;
      return node_info_list;
    }

    bool IsRemoved(const NodeID &node_id) const override { return false; }

    Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                                const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    void AsyncResubscribe() override {}
  };
};

}  // namespace ray
