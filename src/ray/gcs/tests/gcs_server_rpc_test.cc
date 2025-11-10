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

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/observability/fake_metric.h"

namespace ray {

class GcsServerTest : public ::testing::Test {
 public:
  GcsServerTest()
      : fake_metrics_{
            /*actor_by_state_gauge=*/actor_by_state_gauge_,
            /*gcs_actor_by_state_gauge=*/gcs_actor_by_state_gauge_,
            /*running_job_gauge=*/running_job_gauge_,
            /*finished_job_counter=*/finished_job_counter_,
            /*job_duration_in_seconds_gauge=*/job_duration_in_seconds_gauge_,
            /*placement_group_gauge=*/placement_group_gauge_,
            /*placement_group_creation_latency_in_ms_histogram=*/
            placement_group_creation_latency_in_ms_histogram_,
            /*placement_group_scheduling_latency_in_ms_histogram=*/
            placement_group_scheduling_latency_in_ms_histogram_,
            /*placement_group_count_gauge=*/placement_group_count_gauge_,
            /*task_events_reported_gauge=*/task_events_reported_gauge_,
            /*task_events_dropped_gauge=*/task_events_dropped_gauge_,
            /*task_events_stored_gauge=*/task_events_stored_gauge_,
            /*event_recorder_dropped_events_counter=*/fake_dropped_events_counter_,
            /*storage_operation_latency_in_ms_histogram=*/
            storage_operation_latency_in_ms_histogram_,
            /*storage_operation_count_counter=*/storage_operation_count_counter_,
            fake_scheduler_placement_time_ms_histogram_,
        } {
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }

  virtual ~GcsServerTest() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.node_ip_address = "127.0.0.1";
    config.enable_sharding_conn = false;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();

    gcs_server_ = std::make_unique<gcs::GcsServer>(config, fake_metrics_, io_service_);
    gcs_server_->Start();

    thread_io_service_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(
        io_service_, /*record_stats=*/false, /*local_address=*/""));
    client_.reset(
        new rpc::GcsRpcClient("0.0.0.0", gcs_server_->GetPort(), *client_call_manager_));
  }

  void TearDown() override {
    io_service_.stop();
    rpc::DrainServerCallExecutor();
    gcs_server_->Stop();
    thread_io_service_->join();
    gcs_server_.reset();
    rpc::ResetServerCallExecutor();
  }

  bool AddJob(rpc::AddJobRequest request) {
    std::promise<bool> promise;
    client_->AddJob(std::move(request),
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(rpc::MarkJobFinishedRequest request) {
    std::promise<bool> promise;
    client_->MarkJobFinished(
        std::move(request),
        [&promise](const Status &status, const rpc::MarkJobFinishedReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::optional<rpc::ActorTableData> GetActorInfo(const std::string &actor_id) {
    rpc::GetActorInfoRequest request;
    request.set_actor_id(actor_id);
    std::optional<rpc::ActorTableData> actor_table_data_opt;
    std::promise<bool> promise;
    client_->GetActorInfo(std::move(request),
                          [&actor_table_data_opt, &promise](
                              const Status &status, const rpc::GetActorInfoReply &reply) {
                            RAY_CHECK_OK(status);
                            if (reply.has_actor_table_data()) {
                              actor_table_data_opt = reply.actor_table_data();
                            } else {
                              actor_table_data_opt = std::nullopt;
                            }
                            promise.set_value(true);
                          });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_table_data_opt;
  }

  bool RegisterNode(rpc::RegisterNodeRequest request) {
    std::promise<bool> promise;
    client_->RegisterNode(
        std::move(request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnregisterNode(rpc::UnregisterNodeRequest request) {
    std::promise<bool> promise;
    client_->UnregisterNode(
        std::move(request),
        [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfo() {
    std::vector<rpc::GcsNodeInfo> node_info_list;
    rpc::GetAllNodeInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllNodeInfo(
        std::move(request),
        [&node_info_list, &promise](const Status &status,
                                    const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            node_info_list.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return node_info_list;
  }

  bool ReportWorkerFailure(rpc::ReportWorkerFailureRequest request) {
    std::promise<bool> promise;
    client_->ReportWorkerFailure(
        std::move(request),
        [&promise](const Status &status, const rpc::ReportWorkerFailureReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(status.ok());
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::optional<rpc::WorkerTableData> GetWorkerInfo(const std::string &worker_id) {
    rpc::GetWorkerInfoRequest request;
    request.set_worker_id(worker_id);
    std::optional<rpc::WorkerTableData> worker_table_data_opt;
    std::promise<bool> promise;
    client_->GetWorkerInfo(
        std::move(request),
        [&worker_table_data_opt, &promise](const Status &status,
                                           const rpc::GetWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          if (reply.has_worker_table_data()) {
            worker_table_data_opt = reply.worker_table_data();
          } else {
            worker_table_data_opt = std::nullopt;
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return worker_table_data_opt;
  }

  std::vector<rpc::WorkerTableData> GetAllWorkerInfo() {
    std::vector<rpc::WorkerTableData> worker_table_data;
    rpc::GetAllWorkerInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllWorkerInfo(
        std::move(request),
        [&worker_table_data, &promise](const Status &status,
                                       const rpc::GetAllWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.worker_table_data_size(); ++index) {
            worker_table_data.push_back(reply.worker_table_data(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return worker_table_data;
  }

  bool AddWorkerInfo(rpc::AddWorkerInfoRequest request) {
    std::promise<bool> promise;
    client_->AddWorkerInfo(
        std::move(request),
        [&promise](const Status &status, const rpc::AddWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

 protected:
  // Gcs server
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;

  // Gcs client
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // Timeout waiting for gcs server reply, default is 5s
  const std::chrono::milliseconds timeout_ms_{5000};

  // Fake metrics for testing
  observability::FakeGauge actor_by_state_gauge_;
  observability::FakeGauge gcs_actor_by_state_gauge_;
  observability::FakeGauge running_job_gauge_;
  observability::FakeCounter finished_job_counter_;
  observability::FakeGauge job_duration_in_seconds_gauge_;
  observability::FakeGauge placement_group_gauge_;
  observability::FakeHistogram placement_group_creation_latency_in_ms_histogram_;
  observability::FakeHistogram placement_group_scheduling_latency_in_ms_histogram_;
  observability::FakeGauge placement_group_count_gauge_;
  observability::FakeGauge task_events_reported_gauge_;
  observability::FakeGauge task_events_dropped_gauge_;
  observability::FakeGauge task_events_stored_gauge_;
  observability::FakeHistogram storage_operation_latency_in_ms_histogram_;
  observability::FakeCounter storage_operation_count_counter_;
  observability::FakeCounter fake_dropped_events_counter_;
  observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;

  // Fake metrics struct
  gcs::GcsServerMetrics fake_metrics_;
};

TEST_F(GcsServerTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = GenActorTableData(job_id);
  // TODO(sand): Add tests that don't require checkponit.
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data->job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));
}

TEST_F(GcsServerTest, TestJobGarbageCollection) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  auto actor_table_data = GenActorTableData(job_id);

  // Register detached actor for job
  auto detached_actor_table_data = GenActorTableData(job_id);
  detached_actor_table_data->set_is_detached(true);

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data->job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));

  std::function<bool()> condition_func = [this, &actor_table_data]() -> bool {
    return !GetActorInfo(actor_table_data->actor_id()).has_value();
  };
  ASSERT_TRUE(WaitForCondition(condition_func, 10 * 1000));
}

TEST_F(GcsServerTest, TestNodeInfo) {
  // Create gcs node info
  auto gcs_node_info = GenNodeInfo();

  // Register node info
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));
  std::vector<rpc::GcsNodeInfo> node_info_list = GetAllNodeInfo();
  ASSERT_EQ(node_info_list.size(), 1);
  ASSERT_EQ(node_info_list[0].state(), rpc::GcsNodeInfo::ALIVE);

  // Unregister node info
  rpc::UnregisterNodeRequest unregister_node_request;
  unregister_node_request.set_node_id(gcs_node_info->node_id());
  rpc::NodeDeathInfo node_death_info;
  node_death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  std::string reason_message = "Terminate node for testing.";
  node_death_info.set_reason_message(reason_message);
  unregister_node_request.mutable_node_death_info()->CopyFrom(node_death_info);
  ASSERT_TRUE(UnregisterNode(unregister_node_request));
  node_info_list = GetAllNodeInfo();
  ASSERT_EQ(node_info_list.size(), 1);
  ASSERT_TRUE(node_info_list[0].state() == rpc::GcsNodeInfo::DEAD);
  ASSERT_TRUE(node_info_list[0].death_info().reason() ==
              rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  ASSERT_TRUE(node_info_list[0].death_info().reason_message() == reason_message);
}

TEST_F(GcsServerTest, TestNodeInfoFilters) {
  // Create gcs node info
  auto node1 = GenNodeInfo(1, "127.0.0.1", "node1");
  auto node2 = GenNodeInfo(2, "127.0.0.2", "node2");
  auto node3 = GenNodeInfo(3, "127.0.0.3", "node3");

  // Register node infos
  for (auto &node : {node1, node2, node3}) {
    rpc::RegisterNodeRequest register_node_info_request;
    register_node_info_request.mutable_node_info()->CopyFrom(*node);
    ASSERT_TRUE(RegisterNode(register_node_info_request));
  }

  // Kill node3
  rpc::UnregisterNodeRequest unregister_node_request;
  unregister_node_request.set_node_id(node3->node_id());
  rpc::NodeDeathInfo node_death_info;
  node_death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  std::string reason_message = "Terminate node for testing.";
  node_death_info.set_reason_message(reason_message);
  unregister_node_request.mutable_node_death_info()->CopyFrom(node_death_info);
  ASSERT_TRUE(UnregisterNode(unregister_node_request));

  {
    // Get all
    rpc::GetAllNodeInfoRequest request;
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 3);
    ASSERT_EQ(reply.num_filtered(), 0);
    ASSERT_EQ(reply.total(), 3);
  }
  {
    // Get 2 by node id
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_id(node2->node_id());
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }
  {
    // Get by state == ALIVE
    rpc::GetAllNodeInfoRequest request;
    request.set_state_filter(rpc::GcsNodeInfo::ALIVE);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by state == DEAD
    rpc::GetAllNodeInfoRequest request;
    request.set_state_filter(rpc::GcsNodeInfo::DEAD);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_name
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_name("node1");
    request.add_node_selectors()->set_node_name("node2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_ip_address
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_ip_address("127.0.0.1");
    request.add_node_selectors()->set_node_ip_address("127.0.0.2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_id and node_name
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_name("node2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by node_id and state filter
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_id(node3->node_id());
    request.set_state_filter(rpc::GcsNodeInfo::ALIVE);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by node_id, node_name and state filter
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_name("node3");
    request.set_state_filter(rpc::GcsNodeInfo::DEAD);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }
}

TEST_F(GcsServerTest, TestWorkerInfo) {
  // Report worker failure
  auto worker_failure_data = GenWorkerTableData();
  worker_failure_data->mutable_worker_address()->set_ip_address("127.0.0.1");
  worker_failure_data->mutable_worker_address()->set_port(5566);
  rpc::ReportWorkerFailureRequest report_worker_failure_request;
  report_worker_failure_request.mutable_worker_failure()->CopyFrom(*worker_failure_data);
  ASSERT_TRUE(ReportWorkerFailure(report_worker_failure_request));
  std::vector<rpc::WorkerTableData> worker_table_data = GetAllWorkerInfo();
  ASSERT_EQ(worker_table_data.size(), 1);

  // Add worker info
  auto worker_data = GenWorkerTableData();
  worker_data->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
  rpc::AddWorkerInfoRequest add_worker_request;
  add_worker_request.mutable_worker_data()->CopyFrom(*worker_data);
  ASSERT_TRUE(AddWorkerInfo(add_worker_request));
  ASSERT_EQ(GetAllWorkerInfo().size(), 2);

  // Get worker info
  std::optional<rpc::WorkerTableData> result =
      GetWorkerInfo(worker_data->worker_address().worker_id());
  ASSERT_TRUE(result->worker_address().worker_id() ==
              worker_data->worker_address().worker_id());
}
// TODO(sang): Add tests after adding asyncAdd

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
