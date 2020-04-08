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

#include <ray/common/task/task.h>
#include <ray/common/task/task_util.h>
#include <ray/common/test_util.h>
#include <ray/gcs/gcs_server/gcs_actor_manager.h>
#include <ray/gcs/gcs_server/gcs_actor_scheduler.h>
#include <ray/gcs/gcs_server/gcs_node_manager.h>
#include <ray/util/asio_util.h>

#include <memory>
#include <utility>

namespace ray {

struct Mocker {
  static void Reset() {
    push_normal_task_delegate = nullptr;
    worker_lease_delegate = nullptr;
    async_update_delegate = nullptr;
  }
  static std::function<Status(const rpc::Address &, std::unique_ptr<rpc::PushTaskRequest>,
                              rpc::PushTaskReply *)>
      push_normal_task_delegate;
  static std::function<Status(const rpc::Address &, const ray::TaskSpecification &,
                              ray::rpc::RequestWorkerLeaseReply *)>
      worker_lease_delegate;
  static std::function<Status(const ActorID &,
                              const std::shared_ptr<rpc::ActorTableData> &,
                              const gcs::StatusCallback &)>
      async_update_delegate;

  static TaskSpecification GenActorCreationTask(const JobID &job_id) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    ray::FunctionDescriptor empty_descriptor =
        ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    builder.SetCommonTaskSpec(task_id, Language::PYTHON, empty_descriptor, job_id,
                              TaskID::Nil(), 0, TaskID::Nil(), empty_address, 1, {}, {});
    builder.SetActorCreationTaskSpec(actor_id, 100);
    return builder.Build();
  }

  static std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_creation_task_spec = GenActorCreationTask(job_id);
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = actor_creation_task_spec.ActorCreationId();
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_PENDING);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    actor_table_data->mutable_task_spec()->CopyFrom(
        actor_creation_task_spec.GetMessage());
    return actor_table_data;
  }

  static rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id) {
    rpc::CreateActorRequest request;
    auto actor_creation_task_spec = GenActorCreationTask(job_id);
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

  static bool WaitForReady(const std::future<bool> &future, uint64_t timeout_ms) {
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    return status == std::future_status::ready;
  }

  class MockedWorkerLeaseImpl : public WorkerLeaseInterface {
   public:
    explicit MockedWorkerLeaseImpl(rpc::Address address) : address_(std::move(address)) {}

    ray::Status RequestWorkerLease(
        const ray::TaskSpecification &resource_spec,
        const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback)
        override {
      if (worker_lease_delegate) {
        ray::rpc::RequestWorkerLeaseReply reply;
        auto status = worker_lease_delegate(address_, resource_spec, &reply);
        callback(status, reply);
        return Status::OK();
      }
      if (callback) {
        ray::rpc::RequestWorkerLeaseReply reply;
        reply.mutable_worker_address()->set_raylet_id(address_.raylet_id());
        reply.mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
        callback(ray::Status::OK(), reply);
      }
      return ray::Status::OK();
    }

    ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                             bool disconnect_worker) override {
      return ray::Status::OK();
    }

   private:
    rpc::Address address_;
  };

  class MockedCoreWorkerClientImpl : public rpc::CoreWorkerClientInterface {
   public:
    explicit MockedCoreWorkerClientImpl(rpc::Address address)
        : address_(std::move(address)) {}

    ray::Status PushNormalTask(
        std::unique_ptr<rpc::PushTaskRequest> request,
        const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
      if (push_normal_task_delegate) {
        rpc::PushTaskReply reply;
        auto status = push_normal_task_delegate(address_, std::move(request), &reply);
        callback(status, reply);
        return ray::Status::OK();
      }
      if (callback) {
        callback(ray::Status::OK(), rpc::PushTaskReply());
      }
      return ray::Status::OK();
    }

   private:
    rpc::Address address_;
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
      if (async_update_delegate) {
        RAY_CHECK_OK(async_update_delegate(actor_id, data_ptr, callback));
        return Status::OK();
      }
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

std::function<Status(const rpc::Address &, std::unique_ptr<rpc::PushTaskRequest>,
                     rpc::PushTaskReply *)>
    Mocker::push_normal_task_delegate = nullptr;
std::function<Status(const rpc::Address &, const ray::TaskSpecification &,
                     ray::rpc::RequestWorkerLeaseReply *)>
    Mocker::worker_lease_delegate = nullptr;
std::function<Status(const ActorID &, const std::shared_ptr<rpc::ActorTableData> &,
                     const gcs::StatusCallback &)>
    Mocker::async_update_delegate = nullptr;

}  // namespace ray

#endif  // RAY_GCS_TEST_UTIL_H
