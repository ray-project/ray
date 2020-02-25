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

namespace ray {

struct Mocker {
  static void Reset() {
    mock_create_actor = nullptr;
    mock_lease_worker = nullptr;
  }
  static std::function<void(const TaskSpecification &actor_creation_task,
                            const std::function<void()> &on_done)>
      mock_create_actor;
  static std::function<void(const TaskSpecification &actor_creation_task,
                            const std::function<void()> &on_done)>
      default_actor_creator;
  static std::function<bool(std::shared_ptr<gcs::GcsActor> actor,
                            std::shared_ptr<rpc::GcsNodeInfo> node,
                            rpc::RequestWorkerLeaseReply *)>
      mock_lease_worker;

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

  class MockedGcsActorScheduler : public gcs::GcsActorScheduler {
    using gcs::GcsActorScheduler::GcsActorScheduler;
    class MockedGcsLeasedWorker : public gcs::GcsActorScheduler::GcsLeasedWorker {
     public:
      using gcs::GcsActorScheduler::GcsLeasedWorker::GcsLeasedWorker;
      void CreateActor(const TaskSpecification &actor_creation_task,
                       const std::function<void()> &on_done) override {
        if (Mocker::mock_create_actor) {
          assigned_actor_id_ = actor_creation_task.ActorCreationId();
          Mocker::mock_create_actor(actor_creation_task, on_done);
        } else {
          gcs::GcsActorScheduler::GcsLeasedWorker::CreateActor(actor_creation_task,
                                                               on_done);
        }
      }
    };

   protected:
    std::shared_ptr<GcsLeasedWorker> CreateLeasedWorker(
        rpc::Address address, std::vector<rpc::ResourceMapEntry> resources) override {
      return std::make_shared<MockedGcsLeasedWorker>(
          std::move(address), std::move(resources), io_context_, client_call_manager_);
    }

    void LeaseWorkerFromNode(std::shared_ptr<gcs::GcsActor> actor,
                             std::shared_ptr<rpc::GcsNodeInfo> node) override {
      if (Mocker::mock_lease_worker) {
        io_context_.post([this, actor, node] {
          if (gcs_node_manager_.GetNode(ClientID::FromBinary(node->node_id()))) {
            rpc::RequestWorkerLeaseReply reply;
            if (Mocker::mock_lease_worker(actor, node, &reply)) {
              HandleWorkerLeasedReply(actor, reply);
            } else {
              execute_after(io_context_,
                            [this, actor, node] {
                              if (gcs_node_manager_.GetNode(
                                      ClientID::FromBinary(node->node_id()))) {
                                LeaseWorkerFromNode(actor, node);
                              }
                            },
                            200);
            }
          }
        });
      } else {
        gcs::GcsActorScheduler::LeaseWorkerFromNode(actor, node);
      }
    }

   public:
    std::unordered_map<ClientID,
                       std::unordered_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
    GetNodeToWorkersInPhaseOfCreating() const {
      return node_to_workers_when_creating_;
    }
  };
};
std::function<void(const TaskSpecification &actor_creation_task,
                   const std::function<void()> &on_done)>
    Mocker::mock_create_actor = nullptr;
std::function<void(const TaskSpecification &actor_creation_task,
                   const std::function<void()> &on_done)>
    Mocker::default_actor_creator =
        [](const TaskSpecification &actor_creation_task,
           const std::function<void()> &on_done) { on_done(); };
std::function<bool(std::shared_ptr<gcs::GcsActor> actor,
                   std::shared_ptr<rpc::GcsNodeInfo> node,
                   rpc::RequestWorkerLeaseReply *)>
    Mocker::mock_lease_worker = nullptr;

}  // namespace ray

#endif  // RAY_GCS_TEST_UTIL_H
