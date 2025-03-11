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

}  // namespace ray::raylet
