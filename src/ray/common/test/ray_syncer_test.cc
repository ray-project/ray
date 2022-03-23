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
          msg.set_component_id(static_cast<RayComponentId>(cid));
          msg.set_version(++local_versions_[cid]);
          return std::make_optional(std::move(msg));
        }
      };
      ON_CALL(*reporter, Snapshot(_, _)).WillByDefault(WithArg<0>(Invoke(take_snapshot)));
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

  Array<int64_t> local_versions_;
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
  SyncerServer(std::string port, bool has_scheduler_reporter = true) {
    this->server_port = port;
    bool has_scheduler_receiver = !has_scheduler_reporter;
    // Setup io context
    auto node_id = NodeID::FromRandom();
    local_versions.fill(0);

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
          received_versions[message->node_id()].fill(0);
          iter = received_versions.find(message->node_id());
        }

        received_versions[message->node_id()][message->component_id()] =
            message->version();
        message_consumed[message->node_id()]++;
      };

      if (has_scheduler_receiver ||
          static_cast<RayComponentId>(cid) != RayComponentId::SCHEDULER) {
        receivers[cid] = std::make_unique<MockReceiverInterface>();
        ON_CALL(*receivers[cid], Update(_))
            .WillByDefault(WithArg<0>(Invoke(snapshot_received)));
      }

      if (receivers[cid] != nullptr) {
        if (static_cast<RayComponentId>(cid) == RayComponentId::SCHEDULER) {
          ON_CALL(*receivers[cid], NeedBroadcast()).WillByDefault(Return(false));
        } else {
          ON_CALL(*receivers[cid], NeedBroadcast()).WillByDefault(Return(true));
        }
      }

      auto &reporter = reporters[cid];
      auto take_snapshot =
          [this, cid](int64_t version_after) mutable -> std::optional<RaySyncMessage> {
        if (local_versions[cid] <= version_after) {
          return std::nullopt;
        } else {
          auto msg = RaySyncMessage();
          msg.set_component_id(static_cast<RayComponentId>(cid));
          msg.set_version(local_versions[cid]);
          msg.set_node_id(syncer->GetNodeId());
          std::string dbg_message;
          google::protobuf::util::MessageToJsonString(msg, &dbg_message);
          RAY_LOG(INFO) << "Snapshot:" << dbg_message;
          snapshot_taken++;
          return std::make_optional(std::move(msg));
        }
      };
      if (has_scheduler_reporter ||
          static_cast<RayComponentId>(cid) != RayComponentId::SCHEDULER) {
        reporter = std::make_unique<MockReporterInterface>();
        ON_CALL(*reporter, Snapshot(_, Eq(cid)))
            .WillByDefault(WithArg<0>(Invoke(take_snapshot)));
      }
      syncer->Register(
          static_cast<RayComponentId>(cid), reporter.get(), receivers[cid].get());
    }
    thread = std::make_unique<std::thread>([this] {
      this->work = std::make_unique<boost::asio::io_context::work>(io_context);
      io_context.run();
    });
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

  ~SyncerServer() {
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

  Array<int64_t> GetReceivedVersions(const std::string &node_id) const {
    auto iter = received_versions.find(node_id);
    if (iter == received_versions.end()) {
      Array<int64_t> versions;
      versions.fill(-1);
      return versions;
    }
    return iter->second;
  }
  std::unique_ptr<boost::asio::io_context::work> work;
  std::unique_ptr<RaySyncerService> service;
  std::unique_ptr<RaySyncer> syncer;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<std::thread> thread;
  instrumented_io_context io_context;
  std::string server_port;
  Array<int64_t> local_versions;
  Array<std::unique_ptr<MockReporterInterface>> reporters = {nullptr};
  int64_t snapshot_taken = 0;

  std::unordered_map<std::string, Array<int64_t>> received_versions;
  std::unordered_map<std::string, int64_t> message_consumed;
  Array<std::unique_ptr<MockReceiverInterface>> receivers = {nullptr};
};

// Useful for debugging
std::ostream &operator<<(std::ostream &os, const SyncerServer &server) {
  auto dump_array = [&os](const Array<int64_t> &v,
                          std::string label,
                          int indent) mutable -> std::ostream & {
    os << std::string('\t', indent);
    os << label << ": ";
    for (size_t i = 0; i < v.size(); ++i) {
      os << v[i];
      if (i + 1 != v.size()) {
        os << ", ";
      }
    }
    return os;
  };
  os << "NodeID: " << NodeID::FromBinary(server.syncer->GetNodeId()) << std::endl;
  dump_array(server.local_versions, "LocalVersions:", 1) << std::endl;
  for (auto [node_id, versions] : server.received_versions) {
    os << "\tFromNodeID: " << NodeID::FromBinary(node_id) << std::endl;
    dump_array(versions, "RemoteVersions:", 2) << std::endl;
  }
  return os;
}

std::shared_ptr<grpc::Channel> MakeChannel(std::string port) {
  grpc::ChannelArguments argument;
  // Disable http proxy since it disrupts local connections. TODO(ekl) we should make
  // this configurable, or selectively set it for known local connections only.
  argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
  argument.SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
  argument.SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());

  return grpc::CreateCustomChannel(
      "localhost:" + port, grpc::InsecureChannelCredentials(), argument);
}

TEST(SyncerTest, Test1To1) {
  // s1: reporter: RayComponentId::RESOURCE_MANAGER
  // s1: receiver: RayComponentId::SCHEDULER, RayComponentId::RESOURCE_MANAGER
  auto s1 = SyncerServer("19990", false);

  // s2: reporter: RayComponentId::RESOURCE_MANAGER, RayComponentId::SCHEDULER
  // s2: receiver: RayComponentId::RESOURCE_MANAGER
  auto s2 = SyncerServer("19991", true);

  // Make sure the setup is correct
  ASSERT_NE(nullptr, s1.receivers[RayComponentId::SCHEDULER]);
  ASSERT_EQ(nullptr, s2.receivers[RayComponentId::SCHEDULER]);
  ASSERT_EQ(nullptr, s1.reporters[RayComponentId::SCHEDULER]);
  ASSERT_NE(nullptr, s2.reporters[RayComponentId::SCHEDULER]);

  ASSERT_NE(nullptr, s1.receivers[RayComponentId::RESOURCE_MANAGER]);
  ASSERT_NE(nullptr, s2.receivers[RayComponentId::RESOURCE_MANAGER]);
  ASSERT_NE(nullptr, s1.reporters[RayComponentId::RESOURCE_MANAGER]);
  ASSERT_NE(nullptr, s2.reporters[RayComponentId::RESOURCE_MANAGER]);

  auto channel_to_s2 = MakeChannel("19991");

  s1.syncer->Connect(channel_to_s2);

  // Make sure s2 adds s1n
  ASSERT_TRUE(s2.WaitUntil(
      [&s2]() {
        return s2.syncer->sync_connections_.size() == 1 && s2.snapshot_taken == 2;
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
      [&s2, node_id = s1.syncer->GetNodeId()]() {
        return s2.GetNumConsumedMessages(node_id) == 1;
      },
      5));

  // s2 will send 2 messages to s1 because it has two reporters.
  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetNodeId()]() {
        return s1.GetNumConsumedMessages(node_id) == 2;
      },
      5));

  // s2 local module version advance
  s2.local_versions[0] = 1;
  ASSERT_TRUE(s2.WaitUntil([&s2]() { return s2.snapshot_taken == 3; }, 2));

  // Make sure s2 send the new message to s1.
  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetNodeId()]() {
        return s1.GetReceivedVersions(node_id)[RayComponentId::RESOURCE_MANAGER] == 1 &&
               s1.GetNumConsumedMessages(node_id) == 3;
      },
      5));

  // Make sure no new messages are sent
  s2.local_versions[0] = 0;
  std::this_thread::sleep_for(1s);

  ASSERT_TRUE(s1.GetNumConsumedMessages(s2.syncer->GetNodeId()) == 3);
  ASSERT_TRUE(s2.GetNumConsumedMessages(s1.syncer->GetNodeId()) == 1);
  // Change it back
  s2.local_versions[0] = 1;

  // Make some random messages
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> rand_sleep(0, 10000);
  std::uniform_int_distribution<> choose_component(0, 1);
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
        std::stringstream ss;
        ss << "---" << std::endl;
        ss << s1 << std::endl;
        ss << s2 << std::endl;
        RAY_LOG(INFO) << ss.str();
        return s1.GetReceivedVersions(s2.syncer->GetNodeId()) == s2.local_versions &&
               s2.GetReceivedVersions(s1.syncer->GetNodeId())[0] == s1.local_versions[0];
      },
      5));
  // s2 has two reporters + 3 for the ones send before the measure
  ASSERT_LT(s1.GetNumConsumedMessages(s2.syncer->GetNodeId()), max_sends * 2 + 3);
  // s1 has one reporter + 1 for the one send before the measure
  ASSERT_LT(s2.GetNumConsumedMessages(s1.syncer->GetNodeId()), max_sends + 3);
}

TEST(SyncerTest, Test1ToN) {
  size_t base_port = 18990;
  std::vector<std::unique_ptr<SyncerServer>> servers;
  for (int i = 0; i < 100; ++i) {
    servers.push_back(
        std::make_unique<SyncerServer>(std::to_string(i + base_port), i != 0));
  }

  for (int i = 1; i < 100; ++i) {
    servers[0]->syncer->Connect(MakeChannel(servers[i]->server_port));
  }
  using TClusterView =
      absl::flat_hash_map<std::string, Array<std::shared_ptr<const RaySyncMessage>>>;
  auto get_cluster_view = [](RaySyncer &syncer) {
    std::promise<TClusterView> p;
    auto f = p.get_future();
    syncer.GetIOContext().post(
        [&p, &syncer]() mutable { p.set_value(syncer.node_state_->GetClusterView()); },
        "TEST");
    return f.get();
  };
  auto check = [&servers, get_cluster_view]() {
    std::vector<TClusterView> views;
    for (auto &s : servers) {
      views.push_back(get_cluster_view(*(s->syncer)));
    }

    for (size_t i = 1; i < views.size(); ++i) {
      if (views[i].size() != views[0].size()) {
        return false;
      }

      for (const auto &[k, v] : views[0]) {
        auto iter = views[i].find(k);
        if (iter == views[i].end()) {
          return false;
        }
        const auto &vv = iter->second;
        // It's about server[0]
        if (k == servers[0]->syncer->GetNodeId()) {
          if (v[1] || vv[1]) {
            RAY_LOG(ERROR) << "::: Schedule-1";
            RAY_LOG(ERROR) << i << "\t"
                           << "v[1]=" << v[1].get() << ", vv[1]=" << vv[1].get()
                           << ", v[0]=" << v[0].get() << ", vv[0]=" << vv[0].get();
            return false;
          }
        } else if (k == servers[i]->syncer->GetNodeId()) {
          // It's about server[i]
          if (vv[1] == nullptr || v[1] == nullptr) {
            RAY_LOG(ERROR) << "::: Schedule-2";
            RAY_LOG(ERROR) << i << "\t"
                           << "v[1]=" << v[1].get() << ", vv[1]=" << vv[1].get()
                           << ", v[0]=" << v[0].get() << ", vv[0]=" << vv[0].get();
            return false;
          }
        } else if (vv[1] != nullptr || v[1] == nullptr) {
          RAY_LOG(ERROR) << "::: Schedule-3";
          RAY_LOG(ERROR) << i << "\t"
                         << "v[1]=" << v[1].get() << ", vv[1]=" << vv[1].get()
                         << ", v[0]=" << v[0].get() << ", vv[0]=" << vv[0].get();
          return false;
        }

        if (v[0] == nullptr || vv[0] == nullptr) {
          RAY_LOG(ERROR) << "::: RESOURCE-1";
          RAY_LOG(ERROR) << i << "\t"
                         << "v[1]=" << v[1].get() << ", vv[1]=" << vv[1].get()
                         << ", v[0]=" << v[0].get() << ", vv[0]=" << vv[0].get();
          return false;
        }
        if (!google::protobuf::util::MessageDifferencer::Equals(*v[0], *vv[0])) {
          RAY_LOG(ERROR) << "::: RESOURCE-2";
          RAY_LOG(ERROR) << i << "\t"
                         << "v[1]=" << v[1].get() << ", vv[1]=" << vv[1].get()
                         << ", v[0]=" << v[0].get() << ", vv[0]=" << vv[0].get();
          return false;
        }
      }
    }
    return true;
  };

  for (size_t i = 0; i < 5; ++i) {
    RAY_LOG(ERROR) << "CHECKING";
    if (!check()) {
      std::this_thread::sleep_for(1s);
    } else {
      break;
    }
  }
  ASSERT_TRUE(check());
}

// TEST(SyncerTest, TestMToN) { auto server = SyncerServer("9990"); }

}  // namespace syncer
}  // namespace ray
