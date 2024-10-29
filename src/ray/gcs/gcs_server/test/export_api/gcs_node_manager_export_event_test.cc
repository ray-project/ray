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

#include <chrono>
#include <memory>
#include <thread>

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/util/event.h"
// clang-format on

namespace ray {

std::string GenerateLogDir() {
  std::string log_dir_generate = std::string(5, ' ');
  FillRandom(&log_dir_generate);
  std::string log_dir = "event" + StringToHex(log_dir_generate);
  return log_dir;
}

class GcsNodeManagerExportAPITest : public ::testing::Test {
 public:
  GcsNodeManagerExportAPITest() {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &) { return raylet_client_; });
    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);

    RayConfig::instance().initialize(
        R"(
{
  "enable_export_api_write": true
}
  )");
    log_dir_ = GenerateLogDir();
    const std::vector<ray::SourceTypeVariant> source_types = {
        rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_NODE};
    RayEventInit_(source_types,
                  absl::flat_hash_map<std::string, std::string>(),
                  log_dir_,
                  "warning",
                  false);
  }

  virtual ~GcsNodeManagerExportAPITest() {
    io_service_.stop();
    EventManager::Instance().ClearReporters();
    std::filesystem::remove_all(log_dir_.c_str());
  }

 protected:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<rpc::NodeManagerClientPool> client_pool_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  instrumented_io_context io_service_;
  std::string log_dir_;
};

TEST_F(GcsNodeManagerExportAPITest, TestExportEventRegisterNode) {
  // Test export event is written when a node is added with HandleRegisterNode
  gcs::GcsNodeManager node_manager(
      gcs_publisher_, gcs_table_storage_, client_pool_, ClusterID::Nil());
  auto node = Mocker::GenNodeInfo();

  rpc::RegisterNodeRequest register_request;
  register_request.mutable_node_info()->CopyFrom(*node);
  rpc::RegisterNodeReply register_reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};

  node_manager.HandleRegisterNode(register_request, &register_reply, send_reply_callback);
  io_service_.poll();

  std::vector<std::string> vc;
  Mocker::ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_NODE.log");
  ASSERT_EQ((int)vc.size(), 1);
  json event_data = json::parse(vc[0])["event_data"].get<json>();
  ASSERT_EQ(event_data["state"], "ALIVE");
}

TEST_F(GcsNodeManagerExportAPITest, TestExportEventUnregisterNode) {
  // Test export event is written when a node is removed with HandleUnregisterNode
  gcs::GcsNodeManager node_manager(
      gcs_publisher_, gcs_table_storage_, client_pool_, ClusterID::Nil());
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  node_manager.AddNode(node);

  rpc::UnregisterNodeRequest unregister_request;
  unregister_request.set_node_id(node_id.Binary());
  unregister_request.mutable_node_death_info()->set_reason(
      rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
  unregister_request.mutable_node_death_info()->set_reason_message("mock reason message");
  rpc::UnregisterNodeReply unregister_reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};

  node_manager.HandleUnregisterNode(
      unregister_request, &unregister_reply, send_reply_callback);
  io_service_.poll();

  std::vector<std::string> vc;
  Mocker::ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_NODE.log");
  ASSERT_EQ((int)vc.size(), 1);
  json event_data = json::parse(vc[0])["event_data"].get<json>();
  ASSERT_EQ(event_data["state"], "DEAD");
  // Verify death cause for last node DEAD event
  ASSERT_EQ(event_data["death_info"]["reason"], "UNEXPECTED_TERMINATION");
  ASSERT_EQ(event_data["death_info"]["reason_message"], "mock reason message");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}