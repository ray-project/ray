// Copyright 2022 The Ray Authors.
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

// clang-format off
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "ray/common/ray_syncer/ray_syncer.h"
#include "mock/ray/common/ray_syncer/ray_syncer.h"
// clang-format on

using namespace ray::syncer;
using ray::NodeID;
using ::testing::_;
using ::testing::Invoke;
using ::testing::WithArg;

namespace ray {
namespace syncer {

RaySyncMessage MakeMessage(RayComponentId cid, int64_t version, const NodeID &id) {
  auto msg = RaySyncMessage();
  msg.set_version(version);
  msg.set_component_id(cid);
  msg.set_node_id(id.Binary());
  return msg;
}

class RaySyncerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    for (size_t cid = 0; cid < reporters_.size(); ++cid) {
      receivers_[cid] = std::make_unique<MockReceiverInterface>();
      auto &reporter = reporters_[cid];
      reporter = std::make_unique<MockReporterInterface>();
      auto take_snapshot =
          [this, cid](int64_t curr_version) mutable -> std::optional<RaySyncMessage> {
        if (curr_version >= local_versions_[cid]) {
          return std::nullopt;
        } else {
          auto msg = RaySyncMessage();
          msg.set_component_id(static_cast<RayComponentId>(cid));
          msg.set_version(++local_versions_[cid]);
          return std::make_optional(std::move(msg));
        }
      };
      EXPECT_CALL(*reporter, Snapshot(_, _))
          .WillRepeatedly(WithArg<0>(Invoke(take_snapshot)));
      ++cid;
    }
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::io_context::work work(io_context_);
      io_context_.run();
    });
    local_id_ = NodeID::FromRandom();
    syncer_ = std::make_unique<RaySyncer>(io_context_, local_id_.Binary());
  }

  MockReporterInterface *GetReporter(RayComponentId cid) {
    return reporters_[static_cast<size_t>(cid)].get();
  }

  MockReceiverInterface *GetReceiver(RayComponentId cid) {
    return receivers_[static_cast<size_t>(cid)].get();
  }

  int64_t &LocalVersion(RayComponentId cid) {
    return local_versions_[static_cast<size_t>(cid)];
  }

  void TearDown() override {
    io_context_.stop();
    thread_->join();
  }

  Array<int64_t> local_versions_ = {0};
  Array<std::unique_ptr<MockReporterInterface>> reporters_ = {nullptr};
  Array<std::unique_ptr<MockReceiverInterface>> receivers_ = {nullptr};

  instrumented_io_context io_context_;
  std::unique_ptr<std::thread> thread_;

  std::unique_ptr<RaySyncer> syncer_;
  NodeID local_id_;
};

TEST_F(RaySyncerTest, NodeStateGetSnapshot) {
  auto node_status = std::make_unique<NodeState>();
  node_status->SetComponents(RayComponentId::RESOURCE_MANAGER, nullptr, nullptr);
  ASSERT_EQ(std::nullopt, node_status->GetSnapshot(RayComponentId::RESOURCE_MANAGER));
  ASSERT_EQ(std::nullopt, node_status->GetSnapshot(RayComponentId::SCHEDULER));

  auto reporter = std::make_unique<MockReporterInterface>();
  ASSERT_TRUE(node_status->SetComponents(RayComponentId::RESOURCE_MANAGER,
                                         GetReporter(RayComponentId::RESOURCE_MANAGER),
                                         nullptr));

  // Take a snapshot
  ASSERT_EQ(std::nullopt, node_status->GetSnapshot(RayComponentId::SCHEDULER));
  auto msg = node_status->GetSnapshot(RayComponentId::RESOURCE_MANAGER);
  ASSERT_EQ(LocalVersion(RayComponentId::RESOURCE_MANAGER), msg->version());
  // Revert one version back.
  LocalVersion(RayComponentId::RESOURCE_MANAGER) -= 1;
  msg = node_status->GetSnapshot(RayComponentId::RESOURCE_MANAGER);
  ASSERT_EQ(std::nullopt, msg);
}

TEST_F(RaySyncerTest, NodeStateConsume) {
  auto node_status = std::make_unique<NodeState>();
  node_status->SetComponents(RayComponentId::RESOURCE_MANAGER,
                             nullptr,
                             GetReceiver(RayComponentId::RESOURCE_MANAGER));
  auto from_node_id = NodeID::FromRandom();
  // The first time receiver the message
  auto msg = MakeMessage(RayComponentId::RESOURCE_MANAGER, 0, from_node_id);
  ASSERT_TRUE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));

  msg.set_version(1);
  ASSERT_TRUE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
}

TEST_F(RaySyncerTest, NodeSyncConnection) {
  auto node_id = NodeID::FromRandom();
  NodeSyncConnection sync_connection(*syncer_, io_context_, node_id.Binary());
  auto from_node_id = NodeID::FromRandom();
  auto msg = MakeMessage(RayComponentId::RESOURCE_MANAGER, 0, from_node_id);

  // First push will succeed and the second one will be deduplicated.
  ASSERT_TRUE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_EQ(1, sync_connection.sending_queue_.size());
  ASSERT_EQ(1, sync_connection.node_versions_.size());
  ASSERT_EQ(0,
            sync_connection
                .node_versions_[from_node_id.Binary()][RayComponentId::RESOURCE_MANAGER]);

  msg.set_version(2);
  ASSERT_TRUE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  // The previous message is deleted.
  ASSERT_EQ(1, sync_connection.sending_queue_.size());
  ASSERT_EQ(1, sync_connection.node_versions_.size());
  ASSERT_EQ(2,
            sync_connection
                .node_versions_[from_node_id.Binary()][RayComponentId::RESOURCE_MANAGER]);
}

struct SyncerServer {
  SyncerServer(std::string port) {
    // Setup io context
    thread = std::make_unique<std::thread>([this] {
      boost::asio::io_context::work work(io_context);
      io_context.run();
    });
    auto node_id = NodeID::FromRandom();
    // Setup syncer and grpc server
    syncer = std::make_unique<RaySyncer>(io_context, node_id.Binary());
    auto server_address = std::string("0.0.0.0:") + port;
    grpc::ServerBuilder builder;
    service = std::make_unique<RaySyncerService>(*syncer);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
  }
  ~SyncerServer() {
    io_context.stop();
    thread->join();
  }
  std::unique_ptr<RaySyncerService> service;
  std::unique_ptr<RaySyncer> syncer;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<std::thread> thread;
  instrumented_io_context io_context;
};

TEST(SyncerServerE2E, Basic) { auto server = SyncerServer("9990"); }

}  // namespace syncer
}  // namespace ray
