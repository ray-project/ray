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

#include "ray/raylet/node_manager.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "gmock/gmock.h"
#include "mock/ray/raylet/local_task_manager.h"
#include "mock/ray/raylet/worker_pool.h"
#include "ray/raylet/test/util.h"

namespace ray::raylet {
using ::testing::_;
using ::testing::Return;

TaskSpecification BuildTaskSpec(
    const std::unordered_map<std::string, double> &resources) {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  rpc::JobConfig config;
  FunctionDescriptor function_descriptor =
      FunctionDescriptorBuilder::BuildPython("x", "", "", "");
  builder.SetCommonTaskSpec(TaskID::FromRandom(JobID::Nil()),
                            "dummy_task",
                            Language::PYTHON,
                            function_descriptor,
                            JobID::Nil(),
                            config,
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            empty_address,
                            1,
                            false,
                            false,
                            -1,
                            resources,
                            resources,
                            "",
                            0,
                            TaskID::Nil(),
                            "");
  return std::move(builder).ConsumeAndBuild();
}

TEST(NodeManagerTest, TestHandleReportWorkerBacklog) {
  {
    // Worker backlog report from a disconnected worker should be ignored.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(_)).Times(0);
    EXPECT_CALL(local_task_manager, SetWorkerBacklog(_, _, _)).Times(0);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    rpc::ReportWorkerBacklogReply reply;
    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }

  {
    // Worker backlog report from a connected driver should be recorded.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> driver = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto task_spec_1 = BuildTaskSpec({{"CPU", 1}});
    backlog_report_1->mutable_resource_spec()->CopyFrom(task_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto task_spec_2 = BuildTaskSpec({{"GPU", 2}});
    backlog_report_2->mutable_resource_spec()->CopyFrom(task_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(driver));
    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }

  {
    // Worker backlog report from a connected worker should be recorded.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> worker = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto task_spec_1 = BuildTaskSpec({{"CPU", 1}});
    backlog_report_1->mutable_resource_spec()->CopyFrom(task_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto task_spec_2 = BuildTaskSpec({{"GPU", 2}});
    backlog_report_2->mutable_resource_spec()->CopyFrom(task_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(worker));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(0)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }
}

TEST(NodeManagerTest, TestHandleUnexpectedWorkerFailure) {
  {
    WorkerID owner_id = WorkerID::FromRandom();
    absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers;
    absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> killed_workers;

    rpc::Address owner_address;
    owner_address.set_worker_id(owner_id.Binary());

    WorkerID worker_id_1 = WorkerID::FromRandom();
    WorkerID worker_id_2 = WorkerID::FromRandom();
    auto worker_1 = std::make_shared<MockWorker>(worker_id_1, 10);
    auto worker_2 = std::make_shared<MockWorker>(worker_id_2, 10);
    worker_1->SetOwnerAddress(owner_address);
    worker_2->SetOwnerAddress(owner_address);

    TaskSpecBuilder builder_1;
    worker_1->SetAssignedTask(RayTask(std::move(builder_1).ConsumeAndBuild()));

    ActorID actor_id =
        ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
    TaskSpecBuilder builder_2;
    builder_2.SetActorCreationTaskSpec(actor_id,
                                       /*serialized_actor_handle=*/"",
                                       rpc::SchedulingStrategy(),
                                       /*max_restarts=*/0,
                                       /*max_task_retries=*/0,
                                       /*dynamic_worker_options=*/{},
                                       /*max_concurrency=*/1,
                                       /*is_detached=*/true,
                                       /*name=*/"",
                                       /*ray_namespace=*/"",
                                       /*is_asyncio=*/false,
                                       /*concurrency_groups=*/{},
                                       /*extension_data=*/"",
                                       /*execute_out_of_order=*/false,
                                       /*root_detached_actor_id=*/actor_id);
    worker_2->SetAssignedTask(RayTask(std::move(builder_2).ConsumeAndBuild()));

    leased_workers.insert({worker_id_1, worker_1});
    leased_workers.insert({worker_id_2, worker_2});

    NodeManager::HandleUnexpectedWorkerFailure(
        leased_workers,
        [&killed_workers](const std::shared_ptr<WorkerInterface> &worker) {
          killed_workers.insert({worker->WorkerId(), worker});
        },
        owner_id);

    ASSERT_EQ(killed_workers.count(worker_id_1), 1);
    ASSERT_EQ(killed_workers.count(worker_id_2), 0);
  }
}

}  // namespace ray::raylet
