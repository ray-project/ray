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

#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include "ray/util/asio_util.h"

namespace ray {

struct GcsServerMocker {
  class MockWorkerClient : public rpc::CoreWorkerClientInterface {
   public:
    ray::Status PushNormalTask(
        std::unique_ptr<rpc::PushTaskRequest> request,
        const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
      callbacks.push_back(callback);
      return Status::OK();
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

  class MockRayletClient : public WorkerLeaseInterface {
   public:
    ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                             bool disconnect_worker) override {
      if (disconnect_worker) {
        num_workers_disconnected++;
      } else {
        num_workers_returned++;
      }
      return Status::OK();
    }

    ray::Status RequestWorkerLease(
        const ray::TaskSpecification &resource_spec,
        const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) override {
      num_workers_requested += 1;
      callbacks.push_back(callback);
      return Status::OK();
    }

    ray::Status ReleaseUnusedWorkers(
        const std::vector<WorkerID> &workers_in_use,
        const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) override {
      num_release_unused_workers += 1;
      release_callbacks.push_back(callback);
      return Status::OK();
    }

    ray::Status CancelWorkerLease(
        const TaskID &task_id,
        const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
      num_leases_canceled += 1;
      cancel_callbacks.push_back(callback);
      return Status::OK();
    }

    bool GrantWorkerLease() {
      return GrantWorkerLease("", 0, WorkerID::FromRandom(), node_id, ClientID::Nil());
    }

    // Trigger reply to RequestWorkerLease.
    bool GrantWorkerLease(const std::string &address, int port, const WorkerID &worker_id,
                          const ClientID &raylet_id, const ClientID &retry_at_raylet_id,
                          Status status = Status::OK()) {
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

    ~MockRayletClient() {}

    int num_workers_requested = 0;
    int num_workers_returned = 0;
    int num_workers_disconnected = 0;
    int num_leases_canceled = 0;
    int num_release_unused_workers = 0;
    ClientID node_id = ClientID::FromRandom();
    std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
    std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
    std::list<rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply>> release_callbacks = {};
  };

  class MockRayletResourceClient : public ResourceReserveInterface {
   public:
    ray::Status RequestResourceReserve(
        const BundleSpecification &bundle_spec,
        const ray::rpc::ClientCallback<ray::rpc::RequestResourceReserveReply> &callback)
        override {
      num_lease_requested += 1;
      lease_callbacks.push_back(callback);
      return Status::OK();
    }

    ray::Status CancelResourceReserve(
        BundleSpecification &bundle_spec,
        const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
        override {
      num_return_requested += 1;
      return_callbacks.push_back(callback);
      return Status::OK();
    }

    // Trigger reply to RequestWorkerLease.
    bool GrantResourceReserve(bool success = true) {
      Status status = Status::OK();
      rpc::RequestResourceReserveReply reply;
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

    ~MockRayletResourceClient() {}

    int num_lease_requested = 0;
    int num_return_requested = 0;
    ClientID node_id = ClientID::FromRandom();
    std::list<rpc::ClientCallback<rpc::RequestResourceReserveReply>> lease_callbacks = {};
    std::list<rpc::ClientCallback<rpc::CancelResourceReserveReply>> return_callbacks = {};
  };
  class MockedGcsActorScheduler : public gcs::GcsActorScheduler {
   public:
    using gcs::GcsActorScheduler::GcsActorScheduler;

    void ResetLeaseClientFactory(gcs::LeaseClientFactoryFn lease_client_factory) {
      lease_client_factory_ = std::move(lease_client_factory);
    }

    void ResetClientFactory(rpc::ClientFactoryFn client_factory) {
      client_factory_ = std::move(client_factory);
    }

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

    void ResetLeaseClientFactory(
        gcs::ReserveResourceClientFactoryFn lease_client_factory) {
      lease_client_factory_ = std::move(lease_client_factory);
    }
  };
  class MockedGcsActorTable : public gcs::GcsActorTable {
   public:
    MockedGcsActorTable(std::shared_ptr<gcs::StoreClient> store_client)
        : GcsActorTable(store_client) {}

    Status Put(const ActorID &key, const rpc::ActorTableData &value,
               const gcs::StatusCallback &callback) override {
      auto status = Status::OK();
      callback(status);
      return status;
    }

   private:
    boost::asio::io_service main_io_service_;
    std::shared_ptr<gcs::StoreClient> store_client_ =
        std::make_shared<gcs::InMemoryStoreClient>(main_io_service_);
  };

  class MockedNodeInfoAccessor : public gcs::NodeInfoAccessor {
   public:
    Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info) override {
      return Status::NotImplemented("");
    }

    Status UnregisterSelf() override { return Status::NotImplemented(""); }

    const ClientID &GetSelfId() const override {
      static ClientID node_id;
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

    Status AsyncUnregister(const ClientID &node_id,
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
        const gcs::SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    boost::optional<rpc::GcsNodeInfo> Get(const ClientID &node_id) const override {
      return boost::none;
    }

    const std::unordered_map<ClientID, rpc::GcsNodeInfo> &GetAll() const override {
      static std::unordered_map<ClientID, rpc::GcsNodeInfo> node_info_list;
      return node_info_list;
    }

    bool IsRemoved(const ClientID &node_id) const override { return false; }

    Status AsyncGetResources(
        const ClientID &node_id,
        const gcs::OptionalItemCallback<ResourceMap> &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncUpdateResources(const ClientID &node_id, const ResourceMap &resources,
                                const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncDeleteResources(const ClientID &node_id,
                                const std::vector<std::string> &resource_names,
                                const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncSubscribeToResources(
        const gcs::ItemCallback<rpc::NodeResourceChange> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                                const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncSubscribeHeartbeat(
        const gcs::SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    Status AsyncReportBatchHeartbeat(
        const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
        const gcs::StatusCallback &callback) override {
      if (callback) {
        callback(Status::OK());
      }
      return Status::OK();
    }

    Status AsyncSubscribeBatchHeartbeat(
        const gcs::ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    void AsyncResubscribe(bool is_pubsub_server_restarted) override {}
  };

  class MockedErrorInfoAccessor : public gcs::ErrorInfoAccessor {
   public:
    Status AsyncReportJobError(const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
                               const gcs::StatusCallback &callback) override {
      if (callback) {
        callback(Status::OK());
      }
      return Status::OK();
    }
  };

  class MockGcsPubSub : public gcs::GcsPubSub {
   public:
    MockGcsPubSub(std::shared_ptr<gcs::RedisClient> redis_client)
        : GcsPubSub(redis_client) {}

    Status Publish(const std::string &channel, const std::string &id,
                   const std::string &data, const gcs::StatusCallback &done) override {
      return Status::OK();
    }
  };
};

}  // namespace ray
