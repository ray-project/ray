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
#include <chrono>
#include <sstream>
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/util/json_util.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "ray/common/ray_syncer/ray_syncer.h"
#include "mock/ray/common/ray_syncer/ray_syncer.h"
// clang-format on

using namespace std::chrono;
using namespace ray::syncer;
using ray::NodeID;
using ::testing::_;
using ::testing::Eq;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

namespace ray {
namespace syncer {

constexpr size_t kTestComponents = 1;

RaySyncMessage MakeMessage(MessageType cid, int64_t version, const NodeID &id) {
  auto msg = RaySyncMessage();
  msg.set_version(version);
  msg.set_message_type(cid);
  msg.set_node_id(id.Binary());
  return msg;
}

class RaySyncerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    local_versions_.fill(0);
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
          msg.set_message_type(static_cast<MessageType>(cid));
          msg.set_version(++local_versions_[cid]);
          return std::make_optional(std::move(msg));
        }
      };
      ON_CALL(*reporter, CreateSyncMessage(_, _))
          .WillByDefault(WithArg<0>(Invoke(take_snapshot)));
    }
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::io_context::work work(io_context_);
      io_context_.run();
    });
    local_id_ = NodeID::FromRandom();
    syncer_ = std::make_unique<RaySyncer>(io_context_, local_id_.Binary());
  }

  MockReporterInterface *GetReporter(MessageType cid) {
    return reporters_[static_cast<size_t>(cid)].get();
  }

  MockReceiverInterface *GetReceiver(MessageType cid) {
    return receivers_[static_cast<size_t>(cid)].get();
  }

  int64_t &LocalVersion(MessageType cid) {
    return local_versions_[static_cast<size_t>(cid)];
  }

  void TearDown() override {
    io_context_.stop();
    thread_->join();
  }

  std::array<int64_t, kTestComponents> local_versions_;
  std::array<std::unique_ptr<MockReporterInterface>, kTestComponents> reporters_ = {
      nullptr};
  std::array<std::unique_ptr<MockReceiverInterface>, kTestComponents> receivers_ = {
      nullptr};

  instrumented_io_context io_context_;
  std::unique_ptr<std::thread> thread_;

  std::unique_ptr<RaySyncer> syncer_;
  NodeID local_id_;
};

TEST_F(RaySyncerTest, NodeStateCreateSyncMessage) {
  auto node_status = std::make_unique<NodeState>();
  node_status->SetComponent(MessageType::RESOURCE_VIEW, nullptr, nullptr);
  ASSERT_EQ(std::nullopt, node_status->CreateSyncMessage(MessageType::RESOURCE_VIEW));

  auto reporter = std::make_unique<MockReporterInterface>();
  ASSERT_TRUE(node_status->SetComponent(
      MessageType::RESOURCE_VIEW, GetReporter(MessageType::RESOURCE_VIEW), nullptr));

  // Take a snapshot
  auto msg = node_status->CreateSyncMessage(MessageType::RESOURCE_VIEW);
  ASSERT_EQ(LocalVersion(MessageType::RESOURCE_VIEW), msg->version());
  // Revert one version back.
  LocalVersion(MessageType::RESOURCE_VIEW) -= 1;
  msg = node_status->CreateSyncMessage(MessageType::RESOURCE_VIEW);
  ASSERT_EQ(std::nullopt, msg);
}

TEST_F(RaySyncerTest, NodeStateConsume) {
  auto node_status = std::make_unique<NodeState>();
  node_status->SetComponent(
      MessageType::RESOURCE_VIEW, nullptr, GetReceiver(MessageType::RESOURCE_VIEW));
  auto from_node_id = NodeID::FromRandom();
  // The first time receiver the message
  auto msg = MakeMessage(MessageType::RESOURCE_VIEW, 0, from_node_id);
  ASSERT_TRUE(node_status->ConsumeSyncMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeSyncMessage(std::make_shared<RaySyncMessage>(msg)));

  msg.set_version(1);
  ASSERT_TRUE(node_status->ConsumeSyncMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeSyncMessage(std::make_shared<RaySyncMessage>(msg)));
}

TEST_F(RaySyncerTest, NodeSyncConnection) {
  auto node_id = NodeID::FromRandom();

  MockNodeSyncConnection sync_connection(
      io_context_,
      node_id.Binary(),
      [](std::shared_ptr<ray::rpc::syncer::RaySyncMessage>) {});
  auto from_node_id = NodeID::FromRandom();
  auto msg = MakeMessage(MessageType::RESOURCE_VIEW, 0, from_node_id);

  // First push will succeed and the second one will be deduplicated.
  ASSERT_TRUE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_EQ(1, sync_connection.sending_buffer_.size());
  ASSERT_EQ(0, sync_connection.sending_buffer_.begin()->second->version());
  ASSERT_EQ(1, sync_connection.node_versions_.size());
  ASSERT_EQ(
      0,
      sync_connection.node_versions_[from_node_id.Binary()][MessageType::RESOURCE_VIEW]);

  msg.set_version(2);
  ASSERT_TRUE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(sync_connection.PushToSendingQueue(std::make_shared<RaySyncMessage>(msg)));
  // The previous message is deleted.
  ASSERT_EQ(1, sync_connection.sending_buffer_.size());
  ASSERT_EQ(1, sync_connection.node_versions_.size());
  ASSERT_EQ(2, sync_connection.sending_buffer_.begin()->second->version());
  ASSERT_EQ(
      2,
      sync_connection.node_versions_[from_node_id.Binary()][MessageType::RESOURCE_VIEW]);
}

struct SyncerServerTest {
  SyncerServerTest(std::string port) {
    this->server_port = port;
    // Setup io context
    auto node_id = NodeID::FromRandom();
    for (auto &v : local_versions) {
      v = 0;
    }
    // Setup syncer and grpc server
    syncer = std::make_unique<RaySyncer>(io_context, node_id.Binary());

    auto server_address = std::string("0.0.0.0:") + port;
    grpc::ServerBuilder builder;
    service = std::make_unique<RaySyncerService>(*syncer);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();

    for (size_t cid = 0; cid < reporters.size(); ++cid) {
      auto snapshot_received = [this](std::shared_ptr<const RaySyncMessage> message) {
        auto iter = received_versions.find(message->node_id());
        if (iter == received_versions.end()) {
          for (auto &v : received_versions[message->node_id()]) {
            v = 0;
          }
          iter = received_versions.find(message->node_id());
        }

        received_versions[message->node_id()][message->message_type()] =
            message->version();
        message_consumed[message->node_id()]++;
      };
      receivers[cid] = std::make_unique<MockReceiverInterface>();
      EXPECT_CALL(*receivers[cid], ConsumeSyncMessage(_))
          .WillRepeatedly(WithArg<0>(Invoke(snapshot_received)));
      auto &reporter = reporters[cid];
      auto take_snapshot =
          [this, cid](int64_t version_after) mutable -> std::optional<RaySyncMessage> {
        if (local_versions[cid] <= version_after) {
          return std::nullopt;
        } else {
          auto msg = RaySyncMessage();
          msg.set_message_type(static_cast<MessageType>(cid));
          msg.set_version(local_versions[cid]);
          msg.set_node_id(syncer->GetLocalNodeID());
          snapshot_taken++;
          return std::make_optional(std::move(msg));
        }
      };
      reporter = std::make_unique<MockReporterInterface>();
      EXPECT_CALL(*reporter, CreateSyncMessage(_, Eq(cid)))
          .WillRepeatedly(WithArg<0>(Invoke(take_snapshot)));
      syncer->Register(
          static_cast<MessageType>(cid), reporter.get(), receivers[cid].get());
    }
    thread = std::make_unique<std::thread>([this] {
      boost::asio::io_context::work work(io_context);
      io_context.run();
    });
  }

  void WaitSendingFlush() {
    while (true) {
      std::promise<bool> p;
      auto f = p.get_future();
      io_context.post(
          [&p, this]() mutable {
            for (const auto &[node_id, conn] : syncer->sync_connections_) {
              if (!conn->sending_buffer_.empty()) {
                p.set_value(false);
                RAY_LOG(INFO) << NodeID::FromBinary(syncer->GetLocalNodeID()) << ": "
                              << "Waiting for message on " << NodeID::FromBinary(node_id)
                              << " to be sent."
                              << " Remainings " << conn->sending_buffer_.size();
                return;
              }
            }
            p.set_value(true);
          },
          "TEST");
      if (f.get()) {
        return;
      } else {
        std::this_thread::sleep_for(1s);
      }
    }
  }

  bool WaitUntil(std::function<bool()> predicate, int64_t time_s) {
    auto start = steady_clock::now();

    while (duration_cast<seconds>(steady_clock::now() - start).count() <= time_s) {
      std::promise<bool> p;
      auto f = p.get_future();
      io_context.post([&p, predicate]() mutable { p.set_value(predicate()); }, "TEST");
      if (f.get()) {
        return true;
      } else {
        std::this_thread::sleep_for(1s);
      }
    }
    return false;
  }

  ~SyncerServerTest() {
    service.reset();
    server.reset();
    io_context.stop();
    thread->join();
    syncer.reset();
  }

  int64_t GetNumConsumedMessages(const std::string &node_id) const {
    auto iter = message_consumed.find(node_id);
    if (iter == message_consumed.end()) {
      return 0;
    } else {
      return iter->second;
    }
  }

  std::array<std::atomic<int64_t>, kTestComponents> _v;
  const std::array<std::atomic<int64_t>, kTestComponents> &GetReceivedVersions(
      const std::string &node_id) {
    auto iter = received_versions.find(node_id);
    if (iter == received_versions.end()) {
      for (auto &v : _v) {
        v.store(-1);
      }
      return _v;
    }
    return iter->second;
  }
  std::unique_ptr<RaySyncerService> service;
  std::unique_ptr<RaySyncer> syncer;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<std::thread> thread;
  instrumented_io_context io_context;
  std::string server_port;
  std::array<std::atomic<int64_t>, kTestComponents> local_versions;
  std::array<std::unique_ptr<MockReporterInterface>, kTestComponents> reporters = {
      nullptr};
  int64_t snapshot_taken = 0;

  std::unordered_map<std::string, std::array<std::atomic<int64_t>, kTestComponents>>
      received_versions;
  std::unordered_map<std::string, std::atomic<int64_t>> message_consumed;
  std::array<std::unique_ptr<MockReceiverInterface>, kTestComponents> receivers = {
      nullptr};
};

// Useful for debugging
// std::ostream &operator<<(std::ostream &os, const SyncerServerTest &server) {
//   auto dump_array = [&os](const std::array<int64_t, kTestComponents> &v,
//                           std::string label,
//                           int indent) mutable -> std::ostream & {
//     os << std::string('\t', indent);
//     os << label << ": ";
//     for (size_t i = 0; i < v.size(); ++i) {
//       os << v[i];
//       if (i + 1 != v.size()) {
//         os << ", ";
//       }
//     }
//     return os;
//   };
//   os << "NodeID: " << NodeID::FromBinary(server.syncer->GetLocalNodeID()) << std::endl;
//   dump_array(server.local_versions, "LocalVersions:", 1) << std::endl;
//   for (auto [node_id, versions] : server.received_versions) {
//     os << "\tFromNodeID: " << NodeID::FromBinary(node_id) << std::endl;
//     dump_array(versions, "RemoteVersions:", 2) << std::endl;
//   }
//   return os;
// }

std::shared_ptr<grpc::Channel> MakeChannel(std::string port) {
  grpc::ChannelArguments argument;
  argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY,
                  ::RayConfig::instance().grpc_enable_http_proxy() ? 1 : 0);
  argument.SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
  argument.SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());

  return grpc::CreateCustomChannel(
      "localhost:" + port, grpc::InsecureChannelCredentials(), argument);
}

using TClusterView = absl::flat_hash_map<
    std::string,
    std::array<std::shared_ptr<const RaySyncMessage>, kComponentArraySize>>;

TEST(SyncerTest, Test1To1) {
  auto s1 = SyncerServerTest("19990");

  auto s2 = SyncerServerTest("19991");

  // Make sure the setup is correct
  ASSERT_NE(nullptr, s1.receivers[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s2.receivers[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s1.reporters[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s2.reporters[MessageType::RESOURCE_VIEW]);

  auto channel_to_s2 = MakeChannel("19991");

  s1.syncer->Connect(channel_to_s2);

  // Make sure s2 adds s1
  ASSERT_TRUE(s2.WaitUntil(
      [&s2]() {
        return s2.syncer->sync_connections_.size() == 1 && s2.snapshot_taken == 1;
      },
      5));

  // Make sure s1 adds s2
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() {
        return s1.syncer->sync_connections_.size() == 1 && s1.snapshot_taken == 1;
      },
      5));

  // s1 will only send 1 message to s2 because it only has one reporter
  ASSERT_TRUE(s2.WaitUntil(
      [&s2, node_id = s1.syncer->GetLocalNodeID()]() {
        return s2.GetNumConsumedMessages(node_id) == 1;
      },
      5));

  // s2 will send 2 messages to s1 because it has two reporters.
  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetLocalNodeID()]() {
        return s1.GetNumConsumedMessages(node_id) == 1;
      },
      5));

  // s2 local module version advance
  s2.local_versions[0] = 1;
  ASSERT_TRUE(s2.WaitUntil([&s2]() { return s2.snapshot_taken == 2; }, 2));

  // Make sure s2 send the new message to s1.
  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetLocalNodeID()]() {
        return s1.GetReceivedVersions(node_id)[MessageType::RESOURCE_VIEW] == 1 &&
               s1.GetNumConsumedMessages(node_id) == 2;
      },
      5));

  // Make sure no new messages are sent
  s2.local_versions[0] = 0;
  std::this_thread::sleep_for(1s);

  ASSERT_TRUE(s1.GetNumConsumedMessages(s2.syncer->GetLocalNodeID()) == 2);
  ASSERT_TRUE(s2.GetNumConsumedMessages(s1.syncer->GetLocalNodeID()) == 1);
  // Change it back
  s2.local_versions[0] = 1;

  // Make some random messages
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> rand_sleep(0, 10000);
  std::uniform_int_distribution<> choose_component(0, kTestComponents - 1);
  size_t s1_updated = 0;
  size_t s2_updated = 0;

  auto start = steady_clock::now();
  for (int i = 0; i < 10000; ++i) {
    if (choose_component(gen) == 0) {
      s1.local_versions[0]++;
      ++s1_updated;
    } else {
      s2.local_versions[choose_component(gen)]++;
      ++s2_updated;
    }
    if (rand_sleep(gen) < 5) {
      std::this_thread::sleep_for(1s);
    }
  }

  auto end = steady_clock::now();

  // Max messages can be send during this period of time.
  // +1 is for corner cases.
  auto max_sends =
      duration_cast<milliseconds>(end - start).count() /
          RayConfig::instance().raylet_report_resources_period_milliseconds() +
      1;

  ASSERT_TRUE(s1.WaitUntil(
      [&s1, &s2]() {
        return s1.GetReceivedVersions(s2.syncer->GetLocalNodeID()) == s2.local_versions &&
               s2.GetReceivedVersions(s1.syncer->GetLocalNodeID())[0] ==
                   s1.local_versions[0];
      },
      5));
  // s2 has two reporters + 3 for the ones send before the measure
  ASSERT_LE(s1.GetNumConsumedMessages(s2.syncer->GetLocalNodeID()), max_sends * 2 + 3);
  // s1 has one reporter + 1 for the one send before the measure
  ASSERT_LE(s2.GetNumConsumedMessages(s1.syncer->GetLocalNodeID()), max_sends + 3);
}

TEST(SyncerTest, Reconnect) {
  // This test is to check reconnect works.
  // Firstly
  //    s1 -> s3
  // Then,
  //    s2 -> s3
  // And we need to ensure s3 is connecting to s2

  auto s1 = SyncerServerTest("19990");
  auto s2 = SyncerServerTest("19991");
  auto s3 = SyncerServerTest("19992");

  s1.syncer->Connect(MakeChannel("19992"));

  // Make sure the setup is correct
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() {
        return s1.syncer->sync_connections_.size() == 1 && s1.snapshot_taken == 1;
      },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s3]() {
        return s3.syncer->sync_connections_.size() == 1 && s3.snapshot_taken == 1;
      },
      5));
  s2.syncer->Connect(MakeChannel("19992"));

  ASSERT_TRUE(s1.WaitUntil(
      [&s2]() {
        return s2.syncer->sync_connections_.size() == 1 && s2.snapshot_taken == 1;
      },
      5));
}

TEST(SyncerTest, Broadcast) {
  // This test covers the broadcast feature of ray syncer.
  auto s1 = SyncerServerTest("19990");
  auto s2 = SyncerServerTest("19991");
  auto s3 = SyncerServerTest("19992");
  // We need to make sure s1 is sending data to s3 for s2
  s1.syncer->Connect(MakeChannel("19991"));
  s1.syncer->Connect(MakeChannel("19992"));

  // Make sure the setup is correct
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() {
        return s1.syncer->sync_connections_.size() == 2 && s1.snapshot_taken == 1;
      },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s2]() {
        return s2.syncer->sync_connections_.size() == 1 && s2.snapshot_taken == 1;
      },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s3]() {
        return s3.syncer->sync_connections_.size() == 1 && s3.snapshot_taken == 1;
      },
      5));

  // Change the resource in s2 and make sure s1 && s3 are correct
  s2.local_versions[0] = 1;

  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetLocalNodeID()]() mutable {
        return s1.received_versions[node_id][0] == 1;
      },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s3, node_id = s2.syncer->GetLocalNodeID()]() mutable {
        return s3.received_versions[node_id][0] == 1;
      },
      5));
}

bool CompareViews(const std::vector<std::unique_ptr<SyncerServerTest>> &servers,
                  const std::vector<TClusterView> &views,
                  const std::vector<std::set<size_t>> &g) {
  // Check broadcasting is working
  // component id = 0
  // simply compare everything with server 0
  for (size_t i = 1; i < views.size(); ++i) {
    if (views[i].size() != views[0].size()) {
      RAY_LOG(ERROR) << "View size wrong: (" << i << ") :" << views[i].size() << " vs "
                     << views[0].size();
      return false;
    }

    for (const auto &[k, v] : views[0]) {
      auto iter = views[i].find(k);
      if (iter == views[i].end()) {
        return false;
      }
      const auto &vv = iter->second;

      if (!google::protobuf::util::MessageDifferencer::Equals(*v[0], *vv[0])) {
        RAY_LOG(ERROR) << i << ": FAIL RESOURCE: " << v[0] << ", " << vv[0];
        std::string dbg_message;
        google::protobuf::util::MessageToJsonString(*v[0], &dbg_message);
        RAY_LOG(ERROR) << "server[0] >> "
                       << NodeID::FromBinary(servers[0]->syncer->GetLocalNodeID()) << ": "
                       << dbg_message << " - " << NodeID::FromBinary(v[0]->node_id());
        dbg_message.clear();
        google::protobuf::util::MessageToJsonString(*vv[0], &dbg_message);
        RAY_LOG(ERROR) << "server[i] << "
                       << NodeID::FromBinary(servers[i]->syncer->GetLocalNodeID()) << ": "
                       << dbg_message << " - " << NodeID::FromBinary(vv[0]->node_id());
        return false;
      }
    }
  }

  return true;
}

bool TestCorrectness(std::function<TClusterView(RaySyncer &syncer)> get_cluster_view,
                     std::vector<std::unique_ptr<SyncerServerTest>> &servers,
                     const std::vector<std::set<size_t>> &g) {
  auto check = [&servers, get_cluster_view, &g]() {
    std::vector<TClusterView> views;
    for (auto &s : servers) {
      views.push_back(get_cluster_view(*(s->syncer)));
    }
    return CompareViews(servers, views, g);
  };

  for (auto &server : servers) {
    server->WaitSendingFlush();
  }

  for (size_t i = 0; i < 10; ++i) {
    if (!check()) {
      std::this_thread::sleep_for(1s);
    } else {
      break;
    }
  }

  if (!check()) {
    RAY_LOG(ERROR) << "Initial check failed";
    return false;
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> rand_sleep(0, 1000000);
  std::uniform_int_distribution<> choose_component(0, kTestComponents - 1);
  std::uniform_int_distribution<> choose_server(0, servers.size() - 1);

  for (size_t i = 0; i < 1000000; ++i) {
    auto server_idx = choose_server(gen);
    auto message_type = choose_component(gen);
    if (server_idx == 0) {
      message_type = 0;
    }
    servers[server_idx]->local_versions[message_type]++;
    // expect to sleep for 100 times for the whole loop.
    if (rand_sleep(gen) < 100) {
      std::this_thread::sleep_for(100ms);
    }
  }

  for (auto &server : servers) {
    server->WaitSendingFlush();
  }
  // Make sure everything is synced.
  for (size_t i = 0; i < 10; ++i) {
    if (!check()) {
      std::this_thread::sleep_for(1s);
    } else {
      break;
    }
  }

  return check();
}

TEST(SyncerTest, Test1ToN) {
  size_t base_port = 18990;
  std::vector<std::unique_ptr<SyncerServerTest>> servers;
  for (int i = 0; i < 20; ++i) {
    servers.push_back(std::make_unique<SyncerServerTest>(std::to_string(i + base_port)));
  }
  std::vector<std::set<size_t>> g(servers.size());
  for (size_t i = 1; i < servers.size(); ++i) {
    servers[0]->syncer->Connect(MakeChannel(servers[i]->server_port));
    g[0].insert(i);
  }

  auto get_cluster_view = [](RaySyncer &syncer) {
    std::promise<TClusterView> p;
    auto f = p.get_future();
    syncer.GetIOContext().post(
        [&p, &syncer]() mutable { p.set_value(syncer.node_state_->GetClusterView()); },
        "TEST");
    return f.get();
  };

  ASSERT_TRUE(TestCorrectness(get_cluster_view, servers, g));
}

TEST(SyncerTest, TestMToN) {
  size_t base_port = 18990;
  std::vector<std::unique_ptr<SyncerServerTest>> servers;
  for (int i = 0; i < 20; ++i) {
    servers.push_back(std::make_unique<SyncerServerTest>(std::to_string(i + base_port)));
  }
  std::vector<std::set<size_t>> g(servers.size());
  // Try to construct a tree based structure
  size_t i = 1;
  size_t curr = 0;
  while (i < servers.size()) {
    // try to connect to 2 servers per node.
    for (int k = 0; k < 2 && i < servers.size(); ++k, ++i) {
      servers[curr]->syncer->Connect(MakeChannel(servers[i]->server_port));
      g[curr].insert(i);
    }
    ++curr;
  }

  auto get_cluster_view = [](RaySyncer &syncer) {
    std::promise<TClusterView> p;
    auto f = p.get_future();
    syncer.GetIOContext().post(
        [&p, &syncer]() mutable { p.set_value(syncer.node_state_->GetClusterView()); },
        "TEST");
    return f.get();
  };
  ASSERT_TRUE(TestCorrectness(get_cluster_view, servers, g));
}

}  // namespace syncer
}  // namespace ray
