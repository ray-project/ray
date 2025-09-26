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

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/lease/lease.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_actor_manager.h"
#include "ray/gcs/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/gcs_placement_group_mgr.h"
#include "ray/gcs/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray {

struct GcsServerMocker {
  class MockWorkerClient : public rpc::CoreWorkerClientInterface {
   public:
    void PushNormalTask(
        std::unique_ptr<rpc::PushTaskRequest> request,
        const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
      absl::MutexLock lock(&mutex_);
      callbacks_.push_back(callback);
    }

    bool ReplyPushTask(Status status = Status::OK(), bool exit = false) {
      rpc::ClientCallback<rpc::PushTaskReply> callback = nullptr;
      {
        absl::MutexLock lock(&mutex_);
        if (callbacks_.size() == 0) {
          return false;
        }
        callback = callbacks_.front();
        callbacks_.pop_front();
      }
      // call the callback without the lock to avoid deadlock.
      auto reply = rpc::PushTaskReply();
      if (exit) {
        reply.set_worker_exiting(true);
      }
      callback(status, std::move(reply));
      return true;
    }

    size_t GetNumCallbacks() {
      absl::MutexLock lock(&mutex_);
      return callbacks_.size();
    }

    std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks_ ABSL_GUARDED_BY(mutex_);
    absl::Mutex mutex_;
  };

  class MockRayletClient : public rpc::FakeRayletClient {
   public:
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

    void GetWorkerFailureCause(
        const LeaseID &lease_id,
        const ray::rpc::ClientCallback<ray::rpc::GetWorkerFailureCauseReply> &callback)
        override {
      ray::rpc::GetWorkerFailureCauseReply reply;
      callback(Status::OK(), std::move(reply));
      num_get_task_failure_causes += 1;
    }

    void RequestWorkerLease(
        const rpc::LeaseSpec &spec,
        bool grant_or_reject,
        const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
        const int64_t backlog_size,
        const bool is_selected_based_on_locality) override {
      num_workers_requested += 1;
      callbacks.push_back(callback);
    }

    void PrestartWorkers(
        const rpc::PrestartWorkersRequest &request,
        const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) override {
      RAY_LOG(FATAL) << "Not implemented";
    }

    void ReleaseUnusedActorWorkers(
        const std::vector<WorkerID> &workers_in_use,
        const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback)
        override {
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

    void DrainRaylet(
        const rpc::autoscaler::DrainNodeReason &reason,
        const std::string &reason_message,
        int64_t deadline_timestamp_ms,
        const rpc::ClientCallback<rpc::DrainRayletReply> &callback) override {
      rpc::DrainRayletReply reply;
      reply.set_is_accepted(true);
      drain_raylet_callbacks.push_back(callback);
    };

    ~MockRayletClient() {}

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
    std::list<rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply>>
        release_callbacks = {};
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

    using gcs::GcsPlacementGroupScheduler::ScheduleUnplacedBundles;
    // Extra conveinence overload for the mock tests to keep using the old interface.
    void ScheduleUnplacedBundles(
        const std::shared_ptr<gcs::GcsPlacementGroup> &placement_group,
        gcs::PGSchedulingFailureCallback failure_callback,
        gcs::PGSchedulingSuccessfulCallback success_callback) {
      ScheduleUnplacedBundles(
          gcs::SchedulePgRequest{placement_group, failure_callback, success_callback});
    };

   protected:
    friend class GcsPlacementGroupSchedulerTest;
    FRIEND_TEST(GcsPlacementGroupSchedulerTest, TestCheckingWildcardResource);
  };
  class MockedGcsActorTable : public gcs::GcsActorTable {
   public:
    // The store_client and io_context args are NOT used.
    explicit MockedGcsActorTable(std::shared_ptr<gcs::StoreClient> store_client)
        : GcsActorTable(store_client) {}

    Status Put(const ActorID &key,
               const rpc::ActorTableData &value,
               Postable<void(Status)> callback) override {
      auto status = Status::OK();
      std::move(callback).Post("FakeGcsActorTable.Put", status);
      return status;
    }

   private:
    std::shared_ptr<gcs::StoreClient> store_client_ =
        std::make_shared<gcs::InMemoryStoreClient>();
  };
};

}  // namespace ray
