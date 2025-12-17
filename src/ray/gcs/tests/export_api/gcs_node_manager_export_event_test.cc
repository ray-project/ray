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

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "mock/ray/pubsub/publisher.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/event.h"
#include "ray/util/string_utils.h"

using json = nlohmann::json;

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
    auto raylet_client = std::make_shared<rpc::FakeRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [raylet_client = std::move(raylet_client)](const rpc::Address &) {
          return raylet_client;
        });
    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_unique<gcs::GcsTableStorage>(
        std::make_unique<gcs::InMemoryStoreClient>());

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
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<rpc::RayletClientPool> client_pool_;
  std::shared_ptr<pubsub::GcsPublisher> gcs_publisher_;
  instrumented_io_context io_service_;
  std::string log_dir_;
};

TEST_F(GcsNodeManagerExportAPITest, TestExportEventRegisterNode) {
  // Test export event is written when a node is added with HandleRegisterNode
  observability::FakeRayEventRecorder fake_ray_event_recorder;
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   io_service_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   /*ray_event_recorder=*/fake_ray_event_recorder,
                                   /*session_name=*/"");
  auto node = GenNodeInfo();

  rpc::RegisterNodeRequest register_request;
  register_request.mutable_node_info()->CopyFrom(*node);
  rpc::RegisterNodeReply register_reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};

  node_manager.HandleRegisterNode(register_request, &register_reply, send_reply_callback);
  io_service_.poll();

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_NODE.log");
  ASSERT_EQ((int)vc.size(), 1);
  json event_data = json::parse(vc[0])["event_data"].get<json>();
  ASSERT_EQ(event_data["state"], "ALIVE");
}

TEST_F(GcsNodeManagerExportAPITest, TestExportEventUnregisterNode) {
  // Test export event is written when a node is removed with HandleUnregisterNode
  observability::FakeRayEventRecorder fake_ray_event_recorder;
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   io_service_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   /*ray_event_recorder=*/fake_ray_event_recorder,
                                   /*session_name=*/"");
  auto node = GenNodeInfo();
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
  ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_NODE.log");
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
