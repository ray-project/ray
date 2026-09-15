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

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/constants.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client_kv.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/util/clock.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

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
            /*redis_request_payload_bytes_sum=*/redis_request_payload_bytes_sum_,
            /*redis_response_payload_bytes_sum=*/redis_response_payload_bytes_sum_,
            /*redis_command_count_counter=*/redis_command_count_counter_,
            /*resource_usage_gauge=*/fake_resource_usage_gauge_,
            fake_scheduler_placement_time_ms_histogram_,
            /*health_check_rpc_latency_ms_histogram=*/
            fake_health_check_rpc_latency_ms_histogram_,
            /*io_context_monitor_latency_ms_gauge=*/
            fake_io_context_monitor_latency_ms_gauge_,
            /*io_context_monitor_unhealthy_counter=*/
            fake_io_context_monitor_unhealthy_counter_,
        } {
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }

  virtual ~GcsServerTest() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    RayConfig::instance().io_context_monitor_healthy_deadline_ms() = 5000;

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

    StartMainIOServiceThread();

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(
        io_service_, /*record_stats=*/false, /*local_address=*/""));
    client_.reset(
        new rpc::GcsRpcClient("0.0.0.0", gcs_server_->GetPort(), *client_call_manager_));

    // Create health check stub.
    auto channel =
        grpc::CreateChannel("localhost:" + std::to_string(gcs_server_->GetPort()),
                            grpc::InsecureChannelCredentials());
    health_check_stub_ = grpc::health::v1::Health::NewStub(channel);
  }

  void TearDown() override {
    io_service_.stop();
    rpc::DrainServerCallExecutor();
    gcs_server_->Stop();
    if (thread_io_service_ && thread_io_service_->joinable()) {
      thread_io_service_->join();
    }
    gcs_server_.reset();
    rpc::ResetServerCallExecutor();
  }

  // Issues a health Check RPC and returns the reported serving status, or
  // std::nullopt if the RPC itself failed (e.g. timed out).
  std::optional<grpc::health::v1::HealthCheckResponse::ServingStatus> CheckHealth(
      std::chrono::milliseconds timeout) {
    grpc::health::v1::HealthCheckRequest request;
    grpc::health::v1::HealthCheckResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);
    auto status = health_check_stub_->Check(&context, request, &response);
    if (!status.ok()) {
      return std::nullopt;
    }
    return response.status();
  }

  // Polls the health check until it reports `expected` or the timeout elapses,
  // returning whether `expected` was observed.
  bool WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::ServingStatus expected,
                           std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
      if (CheckHealth(std::chrono::milliseconds(1000)) == expected) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }

  void StartMainIOServiceThread() {
    thread_io_service_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });
  }

  bool AddJob(rpc::AddJobRequest request) {
    std::promise<bool> promise;
    client_->AddJob(std::move(request),
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  bool MarkJobFinished(rpc::MarkJobFinishedRequest request) {
    std::promise<bool> promise;
    client_->MarkJobFinished(
        std::move(request),
        [&promise](const Status &status, const rpc::MarkJobFinishedReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), client_timeout_ms_);
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
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
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

    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  bool UnregisterNode(rpc::UnregisterNodeRequest request) {
    std::promise<bool> promise;
    client_->UnregisterNode(
        std::move(request),
        [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), client_timeout_ms_);
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
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
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
    return WaitReady(promise.get_future(), client_timeout_ms_);
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
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
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
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
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
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  // Reads a key from the shared KV store, or nullopt if it is absent.
  std::optional<std::string> InternalKVGet(const std::string &ns,
                                           const std::string &key) {
    rpc::InternalKVGetRequest request;
    request.set_namespace_(ns);
    request.set_key(key);
    std::promise<bool> promise;
    std::optional<std::string> value;
    client_->InternalKVGet(
        std::move(request),
        [&promise, &value](const Status &status, const rpc::InternalKVGetReply &reply) {
          if (status.ok()) {
            value = reply.value();
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return value;
  }

 protected:
  // Server-related fields.
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;

  // Client-related fields.
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::unique_ptr<grpc::health::v1::Health::Stub> health_check_stub_;
  const std::chrono::milliseconds client_timeout_ms_{5000};

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
  observability::FakeCounter redis_request_payload_bytes_sum_;
  observability::FakeCounter redis_response_payload_bytes_sum_;
  observability::FakeCounter redis_command_count_counter_;
  observability::FakeCounter fake_dropped_events_counter_;
  observability::FakeGauge fake_resource_usage_gauge_;
  observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  observability::FakeHistogram fake_health_check_rpc_latency_ms_histogram_;
  observability::FakeGauge fake_io_context_monitor_latency_ms_gauge_;
  observability::FakeCounter fake_io_context_monitor_unhealthy_counter_;

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

TEST_F(GcsServerTest, HealthCheckSucceeds) {
  // The IOContextMonitor drives the serving status; poll until it reports SERVING.
  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));
}

TEST_F(GcsServerTest, HealthCheckReflectsMainIOContextHealth) {
  // Healthy while the main io_context is running.
  ASSERT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));

  // Block the main io_context by occupying its (single-threaded) event loop with a
  // task that waits until released. The IOContextMonitor's probe can no longer
  // complete, so once it exceeds the healthy deadline the GCS reports NOT_SERVING.
  // The health check itself still responds since it runs on gRPC's own threads.
  // We block rather than stop the io_context so it keeps running on its original
  // thread (GCS components are pinned to it via thread checkers).
  std::promise<void> release;
  std::future<void> released = release.get_future();
  io_service_.post([&released]() { released.wait(); }, "BlockMainIOContextForTest");

  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::NOT_SERVING,
                                  std::chrono::seconds(30)));

  // Release the io_context; probes complete again and the GCS recovers to SERVING.
  release.set_value();
  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(30)));
}

// Owns a GcsServer plus the io_context and thread it runs on, so several servers can
// coexist in one test process.
class GcsServerWithThread {
 public:
  GcsServerWithThread(const gcs::GcsServerConfig &config,
                      const gcs::GcsServerMetrics &metrics)
      : server_(std::make_unique<gcs::GcsServer>(config, metrics, io_service_)) {}

  ~GcsServerWithThread() { Stop(); }

  void Start() {
    server_->Start();
    thread_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });
  }

  void Stop() {
    if (!server_) {
      return;
    }
    io_service_.stop();
    server_->Stop();
    if (thread_ && thread_->joinable()) {
      thread_->join();
    }
    server_.reset();
  }

  // Polls until the server finishes DoStart() or the timeout elapses.
  bool WaitForStarted(std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (server_->IsStarted()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return server_->IsStarted();
  }

  gcs::GcsServer &server() { return *server_; }
  instrumented_io_context &io_service() { return io_service_; }

  // Owned here so the client and its call manager cannot outlive the io context they
  // borrow. Lazy because tests that only inspect server state need no connection.
  rpc::GcsRpcClient &Client() {
    if (client_ == nullptr) {
      call_manager_ = std::make_unique<rpc::ClientCallManager>(
          io_service_, /*record_stats=*/false, /*local_address=*/"");
      client_ = std::make_unique<rpc::GcsRpcClient>(
          "0.0.0.0", server_->GetPort(), *call_manager_);
    }
    return *client_;
  }

 private:
  instrumented_io_context io_service_;
  std::unique_ptr<gcs::GcsServer> server_;
  std::unique_ptr<rpc::ClientCallManager> call_manager_;
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<std::thread> thread_;
};

gcs::GcsServerConfig MakeGcsServerConfig(const std::string &name, bool leader_elect) {
  gcs::GcsServerConfig config;
  config.grpc_server_port = 0;
  config.grpc_server_name = name;
  config.grpc_server_thread_num = 1;
  config.redis_address = "127.0.0.1";
  config.node_ip_address = "127.0.0.1";
  config.enable_sharding_conn = false;
  config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  config.ray_leader_elect_enabled = leader_elect;
  return config;
}

// Guards the WriteGcsPid()/WriteAutoscalerV2Flag() extraction: an active GCS must still
// perform both shared-storage writes during startup.
TEST_F(GcsServerTest, TestActiveWritesSharedStorage) {
  EXPECT_TRUE(InternalKVGet("", kGcsPidKey).has_value());
  EXPECT_TRUE(InternalKVGet(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey)
                  .has_value());
}

// A passive GCS boots against storage that an active GCS has already initialized: it
// adopts the existing cluster ID, serves health checks, and reports is_leader=false.
TEST_F(GcsServerTest, TestPassiveServerReadiness) {
  // The fixture's active GCS has already written the cluster ID.
  ASSERT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));

  gcs::GcsServerConfig passive_config =
      MakeGcsServerConfig("MockedPassiveGcsServer", /*leader_elect=*/true);

  GcsServerWithThread passive(passive_config, fake_metrics_);
  passive.Start();
  ASSERT_TRUE(passive.WaitForStarted(std::chrono::seconds(30)));

  EXPECT_FALSE(passive.server().IsLeader());
  // It adopted the active GCS's cluster ID rather than minting a new one.
  EXPECT_EQ(passive.server().GetClusterId(), gcs_server_->GetClusterId());

  rpc::ClientCallManager passive_call_manager(
      passive.io_service(), /*record_stats=*/false, /*local_address=*/"");
  rpc::GcsRpcClient passive_client(
      "0.0.0.0", passive.server().GetPort(), passive_call_manager);

  std::promise<bool> promise;
  std::optional<bool> is_leader;
  passive_client.CheckAlive(
      rpc::CheckAliveRequest(),
      [&promise, &is_leader](const Status &status, const rpc::CheckAliveReply &reply) {
        RAY_CHECK_OK(status);
        if (reply.has_is_leader()) {
          is_leader = reply.is_leader();
        }
        promise.set_value(true);
      });
  ASSERT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  ASSERT_TRUE(is_leader.has_value());
  EXPECT_FALSE(*is_leader);

  passive.Stop();
}

// Starts no GCS of its own, so storage is empty and nothing has claimed leadership.
// Holds the helpers the active-passive tests share.
class GcsLeaderElectionTestBase : public GcsServerTest {
 public:
  // Skip the base SetUp: it starts an active GCS, which would write the cluster ID.
  void SetUp() override { TestSetupUtil::FlushAllRedisServers(); }

  void TearDown() override {
    rpc::DrainServerCallExecutor();
    rpc::ResetServerCallExecutor();
  }

 protected:
  // Started but not waited on: a passive GCS does not finish starting until some leader
  // has written the cluster ID.
  std::unique_ptr<GcsServerWithThread> MakePassiveServer(const std::string &name) {
    auto server = std::make_unique<GcsServerWithThread>(
        MakeGcsServerConfig(name, /*leader_elect=*/true), fake_metrics_);
    server->Start();
    return server;
  }

  // For storage that already has a cluster ID, so the passive GCS can finish starting.
  std::unique_ptr<GcsServerWithThread> StartPassiveServer(
      const std::string &name = "MockedPassiveGcsServer") {
    auto server = MakePassiveServer(name);
    EXPECT_TRUE(server->WaitForStarted(std::chrono::seconds(30)));
    return server;
  }

  // PromoteToLeader() must run on the promoted server's own io context. The leader
  // election client will post it the same way once it is wired up.
  void Promote(gcs::GcsServer &server, instrumented_io_context &io_service) {
    std::promise<void> posted;
    io_service.post(
        [&server, &posted] {
          server.PromoteToLeader();
          posted.set_value();
        },
        "test.PromoteToLeader");
    posted.get_future().wait();
  }

  void Promote(GcsServerWithThread &server) {
    Promote(server.server(), server.io_service());
  }

  // Promotion loads the GCS tables asynchronously and only then flips is_leader_.
  bool WaitForLeader(GcsServerWithThread &server, std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && !server.server().IsLeader()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return server.server().IsLeader();
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfoFrom(rpc::GcsRpcClient &client) {
    std::vector<rpc::GcsNodeInfo> node_info_list;
    std::promise<bool> promise;
    client.GetAllNodeInfo(
        rpc::GetAllNodeInfoRequest(),
        [&node_info_list, &promise](const Status &status,
                                    const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (const auto &node_info : reply.node_info_list()) {
            node_info_list.push_back(node_info);
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return node_info_list;
  }

  Status RegisterNodeOn(rpc::GcsRpcClient &client, const rpc::GcsNodeInfo &node_info) {
    rpc::RegisterNodeRequest request;
    request.mutable_node_info()->CopyFrom(node_info);
    std::promise<Status> promise;
    client.RegisterNode(std::move(request),
                        [&promise](const Status &status, const rpc::RegisterNodeReply &) {
                          promise.set_value(status);
                        });
    auto future = promise.get_future();
    EXPECT_EQ(future.wait_for(std::chrono::milliseconds(client_timeout_ms_)),
              std::future_status::ready);
    return future.get();
  }

  // The helpers below talk to Redis directly rather than through a GCS. A server would
  // answer reads from memory, and starting one just to read would itself write the very
  // keys some of these tests are about.
  std::unique_ptr<gcs::RedisStoreClient> MakeStoreClient(instrumented_io_context &io,
                                                         ClockInterface &clock) {
    gcs::RedisClientOptions options{"127.0.0.1",
                                    TEST_REDIS_SERVER_PORTS.front(),
                                    /*username=*/"",
                                    /*password=*/"",
                                    /*enable_ssl=*/false};
    return std::make_unique<gcs::RedisStoreClient>(io, options, clock);
  }

  absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> NodesInStorage() {
    instrumented_io_context io_context("TestStorage");
    Clock clock;
    gcs::GcsTableStorage storage(MakeStoreClient(io_context, clock));
    absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> nodes;
    bool done = false;
    storage.NodeTable().GetAll(
        {[&nodes, &done](absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> result) {
           nodes = std::move(result);
           done = true;
         },
         io_context});
    RunUntil(io_context, done);
    return nodes;
  }

  std::optional<std::string> StorageGet(const std::string &ns, const std::string &key) {
    instrumented_io_context io_context("TestStorage");
    Clock clock;
    gcs::StoreClientInternalKV kv(MakeStoreClient(io_context, clock));
    bool done = false;
    std::optional<std::string> result;
    kv.Get(ns,
           key,
           {[&done, &result](std::optional<std::string> value) {
              result = std::move(value);
              done = true;
            },
            io_context});
    RunUntil(io_context, done);
    return result;
  }

  bool StorageHas(const std::string &ns, const std::string &key) {
    return StorageGet(ns, key).has_value();
  }

  void PutInStorage(const std::string &ns, const std::string &key, std::string value) {
    instrumented_io_context io_context("TestStorage");
    Clock clock;
    gcs::StoreClientInternalKV kv(MakeStoreClient(io_context, clock));
    bool done = false;
    kv.Put(ns,
           key,
           std::move(value),
           /*overwrite=*/true,
           {[&done](bool) { done = true; }, io_context});
    RunUntil(io_context, done);
  }

 private:
  static void RunUntil(instrumented_io_context &io_context, const bool &done) {
    while (!done) {
      io_context.run_one();
    }
  }
};

// A cluster coming up for the first time: storage is empty, so both candidates park in
// the cluster-ID wait and neither may write anything. Promoting one has to release its
// own wait rather than deadlock -- that wait only ends when a leader writes the ID, and
// that leader is now itself -- perform the shared-storage writes it skipped as a
// passive, and thereby unblock the other candidate.
TEST_F(GcsLeaderElectionTestBase, TestColdStartPromotionBootsWinnerAndLoser) {
  // Private to GcsServer::GetOrGenerateClusterId. Getting it wrong here surfaces as the
  // post-promotion assertion failing, not as a silent pass.
  const std::string cluster_id_ns = "cluster";

  auto winner = MakePassiveServer("MockedWinnerGcsServer");
  auto loser = MakePassiveServer("MockedLoserGcsServer");

  // Had either written the cluster ID, it would have finished starting. The retry runs
  // every second, so this window covers several attempts for both.
  EXPECT_FALSE(winner->WaitForStarted(std::chrono::seconds(5)));
  EXPECT_FALSE(loser->server().IsStarted());
  ASSERT_FALSE(StorageHas(cluster_id_ns, kClusterIdKey));
  ASSERT_FALSE(StorageHas("", kGcsPidKey));
  ASSERT_FALSE(StorageHas(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey));

  Promote(*winner);

  EXPECT_TRUE(winner->WaitForStarted(std::chrono::seconds(30)));
  EXPECT_TRUE(WaitForCondition(
      [this, &cluster_id_ns]() {
        return StorageHas(cluster_id_ns, kClusterIdKey) && StorageHas("", kGcsPidKey) &&
               StorageHas(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey);
      },
      /*timeout_ms=*/30000));

  // The loser adopts the ID the winner minted and finishes starting, still passive.
  EXPECT_TRUE(loser->WaitForStarted(std::chrono::seconds(30)));
  EXPECT_FALSE(loser->server().IsLeader());
  EXPECT_EQ(loser->server().GetClusterId(), winner->server().GetClusterId());
}

// Adds a promoted leader, for the cases that need a standing leader to observe or to
// initialize storage. Both heads run with leader election enabled and start as
// candidates, which is the only topology a real cluster produces.
class GcsLeaderElectionTest : public GcsLeaderElectionTestBase {
 public:
  void SetUp() override {
    GcsLeaderElectionTestBase::SetUp();
    leader_ = MakePassiveServer("MockedLeaderGcsServer");
    // Cold start: storage has no cluster ID, so this head stays parked until it wins
    // the election, then mints the ID itself. Same as a real cluster coming up.
    Promote(*leader_);
    ASSERT_TRUE(leader_->WaitForStarted(std::chrono::seconds(30)));
    ASSERT_TRUE(leader_->server().IsLeader());
  }

  void TearDown() override {
    leader_.reset();
    GcsLeaderElectionTestBase::TearDown();
  }

 protected:
  rpc::GcsRpcClient &LeaderClient() { return leader_->Client(); }

  std::unique_ptr<GcsServerWithThread> leader_;
};

// What a passive gains by being promoted: the node table it never loaded, and an RPC
// gate that lets mutations through. A leader missing either one is not running the
// cluster.
TEST_F(GcsLeaderElectionTest, TestPromotionActivatesTheServer) {
  // Seed storage through the current leader.
  auto seeded_node = GenNodeInfo(1, "127.0.0.1", "seeded_node");
  ASSERT_TRUE(RegisterNodeOn(LeaderClient(), *seeded_node).ok());

  auto passive = StartPassiveServer();

  // The node table was never loaded, so the seeded node is invisible here even though
  // the leader already persisted it.
  EXPECT_TRUE(GetAllNodeInfoFrom(passive->Client()).empty());

  auto rejected_node = GenNodeInfo(2, "127.0.0.2", "rejected_node");
  EXPECT_EQ(RegisterNodeOn(passive->Client(), *rejected_node).code(),
            StatusCode::GcsPassive);

  Promote(*passive);
  ASSERT_TRUE(WaitForLeader(*passive, std::chrono::seconds(30)));

  // Hydrated from storage, so the seeded node is now visible.
  auto nodes = GetAllNodeInfoFrom(passive->Client());
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].node_id(), seeded_node->node_id());
  EXPECT_EQ(nodes[0].state(), rpc::GcsNodeInfo::ALIVE);

  // The gate is open: the registration that was rejected while passive now succeeds.
  ASSERT_TRUE(RegisterNodeOn(passive->Client(), *rejected_node).ok());
  EXPECT_EQ(GetAllNodeInfoFrom(passive->Client()).size(), 2);

  passive->Stop();
}

// The head node handover, which is what a failover comes down to: the head this GCS
// cached while passive has to become durable, and the head it replaces -- still recorded
// ALIVE in storage, and loaded back by hydration -- has to be retired.
TEST_F(GcsLeaderElectionTest, TestPromotionHandsOverTheHeadNode) {
  // The previous leader's head, persisted before this GCS takes over.
  auto stale_head = GenNodeInfo(1, "127.0.0.2", "stale_head");
  stale_head->set_is_head_node(true);
  const NodeID stale_id = NodeID::FromBinary(stale_head->node_id());
  ASSERT_TRUE(RegisterNodeOn(LeaderClient(), *stale_head).ok());

  auto passive = StartPassiveServer();

  // This GCS's own head, on a different machine. Allowed through so the colocated
  // services can start; a remote worker node is not.
  auto new_head = GenNodeInfo(2, "127.0.0.3", "new_head");
  new_head->set_is_head_node(true);
  const NodeID new_id = NodeID::FromBinary(new_head->node_id());
  ASSERT_TRUE(RegisterNodeOn(passive->Client(), *new_head).ok());
  auto worker_node = GenNodeInfo(3, "127.0.0.4", "worker_node");
  EXPECT_EQ(RegisterNodeOn(passive->Client(), *worker_node).code(),
            StatusCode::GcsPassive);

  // Visible, but only from the cache: nothing reached storage.
  auto before = GetAllNodeInfoFrom(passive->Client());
  ASSERT_EQ(before.size(), 1);
  EXPECT_EQ(before[0].node_id(), new_head->node_id());
  EXPECT_FALSE(NodesInStorage().contains(new_id));

  Promote(*passive);
  ASSERT_TRUE(WaitForLeader(*passive, std::chrono::seconds(30)));

  // Neither head has a raylet behind it, so health checks would eventually mark both
  // dead on their own: ~20s here (health_check_initial_delay_ms +
  // health_check_failure_threshold * health_check_period_ms). Promotion takes well under
  // a second, so keep this timeout far below that -- raising it would let a broken
  // ordering pass on the health checker's back.
  EXPECT_TRUE(WaitForCondition(
      [this, stale_id, new_id]() {
        auto stored = NodesInStorage();
        auto stale = stored.find(stale_id);
        auto fresh = stored.find(new_id);
        return stale != stored.end() && stale->second.state() == rpc::GcsNodeInfo::DEAD &&
               fresh != stored.end() && fresh->second.state() == rpc::GcsNodeInfo::ALIVE;
      },
      /*timeout_ms=*/10000));

  int alive_heads = 0;
  for (const auto &entry : NodesInStorage()) {
    if (entry.second.is_head_node() && entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      ++alive_heads;
    }
  }
  EXPECT_EQ(alive_heads, 1);

  passive->Stop();
}

// The active-only keys across a takeover. A passive GCS has to leave the leader's values
// alone while passive and has to claim them once promoted, rather than leaving a dead
// leader's behind. Both GCS run in this one process, so their real pid and autoscaler
// flag are identical; stamping a sentinel first is what makes either half observable.
TEST_F(GcsLeaderElectionTest, TestPromotionRewritesActiveOnlyKeys) {
  PutInStorage("", kGcsPidKey, "leader-pid");
  PutInStorage(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey, "leader-flag");

  // Running all the way through DoStart is the steady state of a standby: it joins a
  // cluster someone else initialized and then waits to be promoted.
  auto passive = StartPassiveServer();
  EXPECT_EQ(StorageGet("", kGcsPidKey), "leader-pid");
  EXPECT_EQ(StorageGet(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey),
            "leader-flag");

  Promote(*passive);
  ASSERT_TRUE(WaitForLeader(*passive, std::chrono::seconds(30)));

  const auto expected_v2_flag =
      std::to_string(static_cast<int>(RayConfig::instance().enable_autoscaler_v2()));
  EXPECT_TRUE(WaitForCondition(
      [this, &expected_v2_flag]() {
        return StorageGet("", kGcsPidKey) == std::to_string(getpid()) &&
               StorageGet(kGcsAutoscalerStateNamespace, kGcsAutoscalerV2EnabledKey) ==
                   expected_v2_flag;
      },
      /*timeout_ms=*/30000));

  passive->Stop();
}

// Promotion must tolerate being driven more than once: the elector re-invokes
// on_started_leading on every renewal, and a GCS that never was passive has nothing to
// promote out of. The second call is issued from the same handler as the first, so it
// also covers the harder case where it lands while the first is still loading tables:
// the IsLeader() guard is still open then, and a second load would hydrate twice.
TEST_F(GcsLeaderElectionTest, TestPromotionIsIdempotent) {
  auto seeded_node = GenNodeInfo(1, "127.0.0.1", "seeded_node");
  ASSERT_TRUE(RegisterNodeOn(LeaderClient(), *seeded_node).ok());

  auto passive = StartPassiveServer();

  std::promise<void> posted;
  passive->io_service().post(
      [&passive, &posted] {
        passive->server().PromoteToLeader();
        passive->server().PromoteToLeader();
        posted.set_value();
      },
      "test.PromoteToLeaderTwice");
  posted.get_future().wait();
  ASSERT_TRUE(WaitForLeader(*passive, std::chrono::seconds(30)));

  // Hydrated exactly once: a second load would re-apply the same node table.
  EXPECT_EQ(GetAllNodeInfoFrom(passive->Client()).size(), 1);

  // Once promoted, further calls are no-ops.
  Promote(*passive);
  EXPECT_TRUE(passive->server().IsLeader());

  // And the head that is already the leader has nothing to promote out of.
  Promote(*leader_);
  EXPECT_TRUE(leader_->server().IsLeader());

  passive->Stop();
}

}  // namespace ray
