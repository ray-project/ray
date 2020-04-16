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

#ifndef RAY_GCS_TEST_UTIL_H
#define RAY_GCS_TEST_UTIL_H

#include <memory>
#include <utility>

#include "src/ray/common/task/task.h"
#include "src/ray/common/task/task_util.h"
#include "src/ray/common/test_util.h"
#include "src/ray/gcs/gcs_server/gcs_actor_manager.h"
#include "src/ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "src/ray/gcs/gcs_server/gcs_node_manager.h"
#include "src/ray/util/asio_util.h"

namespace ray {

struct Mocker {
  static TaskSpecification GenActorCreationTask(const JobID &job_id,
                                                int max_reconstructions = 100) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    ray::FunctionDescriptor empty_descriptor =
        ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    builder.SetCommonTaskSpec(task_id, Language::PYTHON, empty_descriptor, job_id,
                              TaskID::Nil(), 0, TaskID::Nil(), empty_address, 1, {}, {});
    builder.SetActorCreationTaskSpec(actor_id, max_reconstructions);
    return builder.Build();
  }

  static rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id,
                                                       int max_reconstructions = 100) {
    rpc::CreateActorRequest request;
    auto actor_creation_task_spec = GenActorCreationTask(job_id, max_reconstructions);
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(uint16_t port = 0) {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(ClientID::FromRandom().Binary());
    node->set_node_manager_port(port);
    node->set_node_manager_address("127.0.0.1");
    return node;
  }

  static std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_driver_ip_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }

  static std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    return actor_table_data;
  }

  static std::shared_ptr<rpc::TaskTableData> GenTaskTableData(
      const std::string &job_id, const std::string &task_id) {
    auto task_table_data = std::make_shared<rpc::TaskTableData>();
    rpc::Task task;
    rpc::TaskSpec task_spec;
    task_spec.set_job_id(job_id);
    task_spec.set_task_id(task_id);
    task.mutable_task_spec()->CopyFrom(task_spec);
    task_table_data->mutable_task()->CopyFrom(task);
    return task_table_data;
  }

  static std::shared_ptr<rpc::TaskLeaseData> GenTaskLeaseData(
      const std::string &task_id, const std::string &node_id) {
    auto task_lease_data = std::make_shared<rpc::TaskLeaseData>();
    task_lease_data->set_task_id(task_id);
    task_lease_data->set_node_manager_id(node_id);
    return task_lease_data;
  }

  static std::shared_ptr<rpc::ProfileTableData> GenProfileTableData(
      const ClientID &node_id) {
    auto profile_table_data = std::make_shared<rpc::ProfileTableData>();
    profile_table_data->set_component_id(node_id.Binary());
    return profile_table_data;
  }

  static std::shared_ptr<rpc::ErrorTableData> GenErrorTableData(const JobID &job_id) {
    auto error_table_data = std::make_shared<rpc::ErrorTableData>();
    error_table_data->set_job_id(job_id.Binary());
    return error_table_data;
  }

  static std::shared_ptr<rpc::WorkerFailureData> GenWorkerFailureData() {
    auto worker_failure_data = std::make_shared<rpc::WorkerFailureData>();
    worker_failure_data->set_timestamp(std::time(nullptr));
    return worker_failure_data;
  }

  class MockWorkerClient : public rpc::CoreWorkerClientInterface {
   public:
    ray::Status PushNormalTask(
        std::unique_ptr<rpc::PushTaskRequest> request,
        const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
      callbacks.push_back(callback);
      if (enable_auto_reply) {
        ReplyPushTask();
      }
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

    bool enable_auto_reply = false;
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
      if (!auto_grant_node_id.IsNil()) {
        GrantWorkerLease("", 0, WorkerID::FromRandom(), auto_grant_node_id,
                         ClientID::Nil());
      }
      return Status::OK();
    }

    ray::Status CancelWorkerLease(
        const TaskID &task_id,
        const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
      num_leases_canceled += 1;
      cancel_callbacks.push_back(callback);
      return Status::OK();
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

    ~MockRayletClient() {}

    int num_workers_requested = 0;
    int num_workers_returned = 0;
    int num_workers_disconnected = 0;
    int num_leases_canceled = 0;
    ClientID auto_grant_node_id;
    std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
    std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
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

   protected:
    void RetryLeasingWorkerFromNode(std::shared_ptr<gcs::GcsActor> actor,
                                    std::shared_ptr<rpc::GcsNodeInfo> node) override {
      ++num_retry_leasing_count_;
      DoRetryLeasingWorkerFromNode(actor, node);
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

  class MockedActorInfoAccessor : public gcs::ActorInfoAccessor {
   public:
    Status GetAll(std::vector<rpc::ActorTableData> *actor_table_data_list) override {
      return Status::NotImplemented("");
    }

    Status AsyncGet(
        const ActorID &actor_id,
        const gcs::OptionalItemCallback<rpc::ActorTableData> &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncCreateActor(const TaskSpecification &task_spec,
                            const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                         const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncUpdate(const ActorID &actor_id,
                       const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const gcs::StatusCallback &callback) override {
      if (callback) {
        callback(Status::OK());
      }
      return Status::OK();
    }

    Status AsyncSubscribeAll(
        const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    Status AsyncSubscribe(
        const ActorID &actor_id,
        const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
        const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    Status AsyncUnsubscribe(const ActorID &actor_id,
                            const gcs::StatusCallback &done) override {
      return Status::NotImplemented("");
    }

    Status AsyncAddCheckpoint(const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
                              const gcs::StatusCallback &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncGetCheckpoint(
        const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
        const gcs::OptionalItemCallback<rpc::ActorCheckpointData> &callback) override {
      return Status::NotImplemented("");
    }

    Status AsyncGetCheckpointID(
        const ActorID &actor_id,
        const gcs::OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) override {
      return Status::NotImplemented("");
    }
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
        const gcs::SubscribeCallback<ClientID, gcs::ResourceChangeNotification>
            &subscribe,
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
};

}  // namespace ray

#endif  // RAY_GCS_TEST_UTIL_H
