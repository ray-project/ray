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
#include "mock/ray/ray_syncer/ray_syncer.h"

#include <gmock/gmock.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/ray_syncer/node_state.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/ray_syncer/ray_syncer_client.h"
#include "ray/ray_syncer/ray_syncer_server.h"
#include "ray/rpc/authentication/authentication_token.h"
#include "ray/rpc/grpc_server.h"
#include "ray/util/env.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

using ray::NodeID;
using ::testing::_;
using ::testing::Eq;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

namespace ray {
namespace syncer {

constexpr size_t kTestComponents = 1;

using work_guard_type =
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

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
    work_guard_ = std::make_unique<work_guard_type>(io_context_.get_executor());
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
    thread_ = std::make_unique<std::thread>([this]() { io_context_.run(); });
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
    work_guard_->reset();
    io_context_.stop();
    thread_->join();
  }

  std::array<int64_t, kTestComponents> local_versions_;
  std::array<std::unique_ptr<MockReporterInterface>, kTestComponents> reporters_ = {
      nullptr};
  std::array<std::unique_ptr<MockReceiverInterface>, kTestComponents> receivers_ = {
      nullptr};

  instrumented_io_context io_context_;
  std::unique_ptr<work_guard_type> work_guard_;
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

struct MockReactor {
  void StartRead(RaySyncMessage *) { ++read_cnt; }

  void StartWrite(const RaySyncMessage *,
                  grpc::WriteOptions opts = grpc::WriteOptions()) {
    ++write_cnt;
  }

  virtual void OnWriteDone(bool ok) {}
  virtual void OnReadDone(bool ok) {}

  size_t read_cnt = 0;
  size_t write_cnt = 0;
};

TEST_F(RaySyncerTest, RaySyncerBidiReactorBase) {
  auto node_id = NodeID::FromRandom();

  MockRaySyncerBidiReactorBase<MockReactor> sync_reactor(
      io_context_,
      node_id.Binary(),
      [](std::shared_ptr<const ray::rpc::syncer::RaySyncMessage>) {});
  auto from_node_id = NodeID::FromRandom();
  auto msg = MakeMessage(MessageType::RESOURCE_VIEW, 0, from_node_id);
  auto msg_ptr1 = std::make_shared<RaySyncMessage>(msg);
  msg.set_version(2);
  auto msg_ptr2 = std::make_shared<RaySyncMessage>(msg);
  msg.set_version(3);
  auto msg_ptr3 = std::make_shared<RaySyncMessage>(msg);

  // First push will succeed and the second one will be deduplicated.
  ASSERT_TRUE(sync_reactor.PushToSendingQueue(msg_ptr1));
  ASSERT_FALSE(sync_reactor.PushToSendingQueue(msg_ptr1));
  ASSERT_EQ(0, sync_reactor.sending_buffer_.size());

  ASSERT_TRUE(sync_reactor.PushToSendingQueue(msg_ptr2));
  ASSERT_EQ(1, sync_reactor.sending_buffer_.size());
  ASSERT_EQ(1, sync_reactor.node_versions_.size());
  ASSERT_EQ(2, sync_reactor.sending_buffer_.begin()->second->version());
  ASSERT_EQ(
      2, sync_reactor.node_versions_[from_node_id.Binary()][MessageType::RESOURCE_VIEW]);

  ASSERT_TRUE(sync_reactor.PushToSendingQueue(msg_ptr3));
  ASSERT_EQ(1, sync_reactor.sending_buffer_.size());
  ASSERT_EQ(1, sync_reactor.node_versions_.size());
  ASSERT_EQ(3, sync_reactor.sending_buffer_.begin()->second->version());
  ASSERT_EQ(
      3, sync_reactor.node_versions_[from_node_id.Binary()][MessageType::RESOURCE_VIEW]);
}

struct SyncerServerTest {
  explicit SyncerServerTest(std::string port)
      : SyncerServerTest(
            std::move(port), /*node_id=*/NodeID::FromRandom(), /*ray_sync_observer=*/{}) {
  }

  SyncerServerTest(std::string port,
                   NodeID node_id,
                   RpcCompletionCallback ray_sync_observer)
      : work_guard(io_context.get_executor()) {
    this->server_port = port;
    // Setup io context
    for (auto &v : local_versions) {
      v = 0;
    }
    // Setup syncer and grpc server
    syncer = std::make_unique<RaySyncer>(
        io_context, node_id.Binary(), std::move(ray_sync_observer));
    thread = std::make_unique<std::thread>([this] { io_context.run(); });

    auto server_address = BuildAddress("0.0.0.0", port);
    grpc::ServerBuilder builder;
    service = std::make_unique<RaySyncerService>(*syncer);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();

    for (size_t cid = 0; cid < reporters.size(); ++cid) {
      auto snapshot_received = [this,
                                node_id](std::shared_ptr<const RaySyncMessage> message) {
        RAY_LOG(DEBUG) << "Message received: from "
                       << NodeID::FromBinary(message->node_id()) << " to " << node_id;
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
        RAY_LOG(DEBUG) << "Message consumed from "
                       << NodeID::FromBinary(message->node_id())
                       << ", local_id=" << node_id;
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
  }

  void WaitSendingFlush() {
    while (true) {
      std::promise<bool> p;
      auto f = p.get_future();
      io_context.post(
          [&p, this]() mutable {
            for (const auto &[node_id, conn] : syncer->sync_reactors_) {
              auto ptr = dynamic_cast<RayServerBidiReactor *>(conn);
              size_t remainings = 0;
              if (ptr == nullptr) {
                remainings =
                    dynamic_cast<RayClientBidiReactor *>(conn)->sending_buffer_.size();
              } else {
                remainings = ptr->sending_buffer_.size();
              }

              if (remainings != 0) {
                p.set_value(false);
                RAY_LOG(INFO) << NodeID::FromBinary(syncer->GetLocalNodeID()) << ": "
                              << "Waiting for message on " << NodeID::FromBinary(node_id)
                              << " to be sent."
                              << " Remainings " << remainings;
                return;
              }
            }
            p.set_value(true);
          },
          "TEST");
      if (f.get()) {
        return;
      } else {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
  }

  bool WaitUntil(std::function<bool()> predicate, int64_t time_s) {
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start)
               .count() <= time_s) {
      std::promise<bool> p;
      auto f = p.get_future();
      io_context.post([&p, predicate]() mutable { p.set_value(predicate()); }, "TEST");
      if (f.get()) {
        return true;
      } else {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
    return false;
  }

  void Stop() {
    for (auto node_id : syncer->GetAllConnectedNodeIDs()) {
      syncer->Disconnect(node_id);
    }

    server->Shutdown();

    io_context.stop();
    thread->join();

    server.reset();
    service.reset();

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
  work_guard_type work_guard;
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
      BuildAddress("localhost", port), grpc::InsecureChannelCredentials(), argument);
}

using TClusterView = absl::flat_hash_map<
    std::string,
    std::array<std::shared_ptr<const RaySyncMessage>, kComponentArraySize>>;

class SyncerTest : public ::testing::Test {
 public:
  SyncerServerTest &MakeServer(std::string port) {
    servers.emplace_back(std::make_unique<SyncerServerTest>(port));
    return *servers.back();
  }

  SyncerServerTest &MakeServer(std::string port,
                               NodeID node_id,
                               RpcCompletionCallback on_rpc_completion) {
    servers.emplace_back(std::make_unique<SyncerServerTest>(
        port, std::move(node_id), std::move(on_rpc_completion)));
    return *servers.back();
  }

 protected:
  void TearDown() override {
    // Drain all grpc requests.
    for (auto &s : servers) {
      s->Stop();
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  std::vector<std::unique_ptr<SyncerServerTest>> servers;
};

TEST_F(SyncerTest, Test1To1) {
  // Generate node ids for checking.
  NodeID node_id1 = NodeID::FromRandom();
  NodeID node_id2 = NodeID::FromRandom();

  // Used to check the number of messages consumed for two servers.
  int s1_observer_cb_call_cnt = 0;
  int s2_observer_cb_call_cnt = 0;

  // Register observer callback for syncers.
  auto syncer_observer_cb = [&](const NodeID &node_id) {
    if (node_id == node_id1) {
      ++s1_observer_cb_call_cnt;
    } else if (node_id == node_id2) {
      ++s2_observer_cb_call_cnt;
    }
  };

  auto &s1 = MakeServer("19990", node_id1, syncer_observer_cb);
  auto &s2 = MakeServer("19991", node_id2, syncer_observer_cb);

  // Make sure the setup is correct
  ASSERT_NE(nullptr, s1.receivers[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s2.receivers[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s1.reporters[MessageType::RESOURCE_VIEW]);
  ASSERT_NE(nullptr, s2.reporters[MessageType::RESOURCE_VIEW]);
  RAY_LOG(DEBUG) << "s1: " << NodeID::FromBinary(s1.syncer->GetLocalNodeID());
  RAY_LOG(DEBUG) << "s2: " << NodeID::FromBinary(s2.syncer->GetLocalNodeID());

  auto channel_to_s2 = MakeChannel("19991");

  s1.syncer->Connect(s2.syncer->GetLocalNodeID(), channel_to_s2);

  // Make sure s2 adds s1
  ASSERT_TRUE(s2.WaitUntil(
      [&s2]() { return s2.syncer->sync_reactors_.size() == 1 && s2.snapshot_taken == 1; },
      5));

  // Make sure s1 adds s2
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() { return s1.syncer->sync_reactors_.size() == 1 && s1.snapshot_taken == 1; },
      5));

  // s1 will only send 1 message to s2 because it only has one reporter
  ASSERT_TRUE(s2.WaitUntil(
      [&s2, node_id = s1.syncer->GetLocalNodeID()]() {
        RAY_LOG(DEBUG) << NodeID::FromBinary(node_id) << " - "
                       << s2.GetNumConsumedMessages(node_id);
        return s2.GetNumConsumedMessages(node_id) == 1;
      },
      5));

  // s2 will send 2 messages to s1 because it has two reporters.
  ASSERT_TRUE(s1.WaitUntil(
      [&s1, node_id = s2.syncer->GetLocalNodeID()]() {
        RAY_LOG(DEBUG) << "Num of messages from " << NodeID::FromBinary(node_id) << " to "
                       << NodeID::FromBinary(s1.syncer->GetLocalNodeID()) << " is "
                       << s1.GetNumConsumedMessages(node_id);
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
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_EQ(s1.GetNumConsumedMessages(s2.syncer->GetLocalNodeID()), 2);
  ASSERT_EQ(s2.GetNumConsumedMessages(s1.syncer->GetLocalNodeID()), 1);
  // Change it back
  s2.local_versions[0] = 1;

  // Make some random messages
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> rand_sleep(0, 10000);
  std::uniform_int_distribution<> choose_component(0, kTestComponents - 1);

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 10000; ++i) {
    if (choose_component(gen) == 0) {
      s1.local_versions[0]++;
    } else {
      s2.local_versions[choose_component(gen)]++;
    }
    if (rand_sleep(gen) < 5) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  auto end = std::chrono::steady_clock::now();

  // Max messages can be send during this period of time.
  // +1 is for corner cases.
  auto max_sends =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() /
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

  // Make sure registered callbacks have been called.
  ASSERT_GT(s1_observer_cb_call_cnt, 0);
  ASSERT_GT(s2_observer_cb_call_cnt, 0);
}

TEST_F(SyncerTest, Reconnect) {
  // This test is to check reconnect works.
  auto &s1 = MakeServer("19990");
  auto &s2 = MakeServer("19991");

  s1.syncer->Connect(s2.syncer->GetLocalNodeID(), MakeChannel("19991"));

  // Make sure the setup is correct
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() { return s1.syncer->sync_reactors_.size() == 1 && s1.snapshot_taken == 1; },
      5));
  ASSERT_TRUE(s2.WaitUntil(
      [&s2]() { return s2.syncer->sync_reactors_.size() == 1 && s2.snapshot_taken == 1; },
      5));

  s1.syncer->Disconnect(s2.syncer->GetLocalNodeID());
  s1.syncer->Connect(s2.syncer->GetLocalNodeID(), MakeChannel("19991"));
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() { return s1.syncer->sync_reactors_.size() == 1 && s1.snapshot_taken == 1; },
      5));

  s1.local_versions[0] = 1;
  ASSERT_TRUE(s2.WaitUntil(
      [&s2, node_id = s1.syncer->GetLocalNodeID()]() {
        return s2.received_versions[node_id][0] == 1;
      },
      5));
}

TEST_F(SyncerTest, Broadcast) {
  // This test covers the broadcast feature of ray syncer.
  auto &s1 = MakeServer("19990");
  auto &s2 = MakeServer("19991");
  auto &s3 = MakeServer("19992");
  // We need to make sure s1 is sending data to s3 for s2
  s1.syncer->Connect(s2.syncer->GetLocalNodeID(), MakeChannel("19991"));
  s1.syncer->Connect(s3.syncer->GetLocalNodeID(), MakeChannel("19992"));

  // Make sure the setup is correct
  ASSERT_TRUE(s1.WaitUntil(
      [&s1]() { return s1.syncer->sync_reactors_.size() == 2 && s1.snapshot_taken == 1; },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s2]() { return s2.syncer->sync_reactors_.size() == 1 && s2.snapshot_taken == 1; },
      5));

  ASSERT_TRUE(s1.WaitUntil(
      [&s3]() { return s3.syncer->sync_reactors_.size() == 1 && s3.snapshot_taken == 1; },
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
  ASSERT_EQ(
      0,
      s1.syncer->GetSyncMessage(s1.syncer->GetLocalNodeID(), MessageType::RESOURCE_VIEW)
          ->version());
  ASSERT_EQ(nullptr,
            s1.syncer->GetSyncMessage(NodeID::FromRandom().Binary(),
                                      MessageType::RESOURCE_VIEW));
  s1.syncer->Disconnect(s3.syncer->GetLocalNodeID());
  RAY_LOG(INFO) << "s1.id=" << NodeID::FromBinary(s1.syncer->GetLocalNodeID());
  RAY_LOG(INFO) << "s3.id=" << NodeID::FromBinary(s3.syncer->GetLocalNodeID());

  EXPECT_TRUE(s3.WaitUntil(
      [&s3, node_id = s1.syncer->GetLocalNodeID()]() mutable {
        return s3.syncer->node_state_->GetClusterView().count(node_id) == 0;
      },
      5));
  EXPECT_TRUE(s1.WaitUntil(
      [&s1, node_id = s3.syncer->GetLocalNodeID()]() mutable {
        return s1.syncer->node_state_->GetClusterView().count(node_id) == 0;
      },
      5));
}

bool CompareViews(const std::vector<SyncerServerTest *> &servers,
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
        RAY_CHECK(google::protobuf::util::MessageToJsonString(*v[0], &dbg_message).ok());
        RAY_LOG(ERROR) << "server[0] >> "
                       << NodeID::FromBinary(servers[0]->syncer->GetLocalNodeID()) << ": "
                       << dbg_message << " - " << NodeID::FromBinary(v[0]->node_id());
        dbg_message.clear();
        RAY_CHECK(google::protobuf::util::MessageToJsonString(*vv[0], &dbg_message).ok());
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
                     std::vector<SyncerServerTest *> &servers,
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
      std::this_thread::sleep_for(std::chrono::seconds(1));
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
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  for (auto &server : servers) {
    server->WaitSendingFlush();
  }
  // Make sure everything is synced.
  for (size_t i = 0; i < 10; ++i) {
    if (!check()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }

  return check();
}

TEST_F(SyncerTest, Test1ToN) {
  size_t base_port = 18990;
  std::vector<SyncerServerTest *> servers;
  for (int i = 0; i < 20; ++i) {
    servers.push_back(&MakeServer(std::to_string(i + base_port)));
  }
  std::vector<std::set<size_t>> g(servers.size());
  for (size_t i = 1; i < servers.size(); ++i) {
    servers[0]->syncer->Connect(servers[i]->syncer->GetLocalNodeID(),
                                MakeChannel(servers[i]->server_port));
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

TEST_F(SyncerTest, TestMToN) {
  size_t base_port = 18990;
  std::vector<SyncerServerTest *> servers;
  for (int i = 0; i < 20; ++i) {
    servers.push_back(&MakeServer(std::to_string(i + base_port)));
  }
  std::vector<std::set<size_t>> g(servers.size());
  // Try to construct a tree based structure
  size_t i = 1;
  size_t curr = 0;
  while (i < servers.size()) {
    // try to connect to 2 servers per node.
    for (int k = 0; k < 2 && i < servers.size(); ++k, ++i) {
      servers[curr]->syncer->Connect(servers[i]->syncer->GetLocalNodeID(),
                                     MakeChannel(servers[i]->server_port));
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

struct MockRaySyncerService : public ray::rpc::syncer::RaySyncer::CallbackService {
  MockRaySyncerService(
      instrumented_io_context &_io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> _message_processor,
      std::function<void(RaySyncerBidiReactor *reactor, bool)> _cleanup_cb)
      : message_processor(_message_processor),
        cleanup_cb(_cleanup_cb),
        node_id(NodeID::FromRandom()),
        io_context(_io_context) {}
  grpc::ServerBidiReactor<RaySyncMessage, RaySyncMessage> *StartSync(
      grpc::CallbackServerContext *context) override {
    reactor = new RayServerBidiReactor(context,
                                       io_context,
                                       node_id.Binary(),
                                       message_processor,
                                       cleanup_cb,
                                       std::nullopt);
    return reactor;
  }

  std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor;
  std::function<void(RaySyncerBidiReactor *reactor, bool)> cleanup_cb;
  NodeID node_id;
  instrumented_io_context &io_context;
  RayServerBidiReactor *reactor = nullptr;
};

class SyncerReactorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rpc_service_ = std::make_unique<MockRaySyncerService>(
        io_context_,
        [this](auto msg) { server_received_message.set_value(msg); },
        [this](RaySyncerBidiReactor *reactor, bool restart) {
          server_cleanup.set_value(std::make_pair(reactor->GetRemoteNodeID(), restart));
        });
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:18990", grpc::InsecureServerCredentials());
    builder.RegisterService(rpc_service_.get());
    server = builder.BuildAndStart();

    client_node_id = NodeID::FromRandom();
    cli_channel = MakeChannel("18990");
    auto cli_stub = ray::rpc::syncer::RaySyncer::NewStub(cli_channel);

    // `cli_reactor` will be deleted by `RayClientBidiReactor::OnDone`, so we need to use
    // `release()` to release ownership.
    cli_reactor =
        std::make_unique<RayClientBidiReactor>(
            rpc_service_->node_id.Binary(),
            client_node_id.Binary(),
            io_context_,
            [this](auto msg) { client_received_message.set_value(msg); },
            [this](RaySyncerBidiReactor *reactor, bool r) {
              client_cleanup.set_value(std::make_pair(reactor->GetRemoteNodeID(), r));
            },
            std::move(cli_stub))
            .release();
    cli_reactor->StartCall();

    work_guard_ = std::make_unique<work_guard_type>(io_context_.get_executor());
    thread_ = std::make_unique<std::thread>([this]() { io_context_.run(); });

    auto start = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start)
               .count() <= 5) {
      RAY_LOG(INFO) << "Waiting: "
                    << std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
      if (rpc_service_->reactor != nullptr) {
        break;
      };
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  void TearDown() override {
    io_context_.stop();
    thread_->join();
  }

  std::pair<RayServerBidiReactor *, RayClientBidiReactor *> GetReactors() {
    return std::make_pair(rpc_service_->reactor, cli_reactor);
  }

  std::pair<std::string, std::string> GetNodeID() {
    return std::make_pair(rpc_service_->node_id.Binary(), client_node_id.Binary());
  }

  void ResetPromise() {
    server_received_message = std::promise<std::shared_ptr<const RaySyncMessage>>();
    client_received_message = std::promise<std::shared_ptr<const RaySyncMessage>>();
    server_cleanup = std::promise<std::pair<std::string, bool>>();
    client_cleanup = std::promise<std::pair<std::string, bool>>();
  }

  instrumented_io_context io_context_;
  std::unique_ptr<work_guard_type> work_guard_;
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<MockRaySyncerService> rpc_service_;
  std::unique_ptr<grpc::Server> server;
  std::promise<std::shared_ptr<const RaySyncMessage>> server_received_message;
  std::promise<std::shared_ptr<const RaySyncMessage>> client_received_message;
  std::promise<std::pair<std::string, bool>> server_cleanup;
  std::promise<std::pair<std::string, bool>> client_cleanup;

  grpc::ClientContext cli_context;
  RayClientBidiReactor *cli_reactor;
  std::shared_ptr<grpc::Channel> cli_channel;
  NodeID client_node_id;
};

TEST_F(SyncerReactorTest, TestReactor) {
  auto [s, c] = GetReactors();
  auto [node_s, node_c] = GetNodeID();
  ASSERT_TRUE(s != nullptr);
  ASSERT_TRUE(c != nullptr);

  auto msg_s = std::make_shared<RaySyncMessage>();
  msg_s->set_version(1);
  msg_s->set_node_id(node_s);

  s->PushToSendingQueue(msg_s);

  auto msg_c = std::make_shared<RaySyncMessage>();
  msg_c->set_version(2);
  msg_c->set_node_id(node_c);

  c->PushToSendingQueue(msg_c);
  // Make sure sending is working
  auto server_received = server_received_message.get_future().get();
  auto client_received = client_received_message.get_future().get();
  ResetPromise();
  ASSERT_EQ(server_received->version(), 2);
  ASSERT_EQ(server_received->node_id(), node_c);
  ASSERT_EQ(client_received->version(), 1);
  ASSERT_EQ(client_received->node_id(), node_s);

  s->Disconnect();
  auto c_cleanup = client_cleanup.get_future().get();
  ASSERT_EQ(node_s, c_cleanup.first);
  ASSERT_EQ(false, c_cleanup.second);
}

TEST_F(SyncerReactorTest, TestReactorFailure) {
  auto [s, c] = GetReactors();
  auto [node_s, node_c] = GetNodeID();
  ASSERT_TRUE(s != nullptr);
  ASSERT_TRUE(c != nullptr);
  *s->disconnected_ = true;
  s->Finish(grpc::Status::CANCELLED);
  auto c_cleanup = client_cleanup.get_future().get();
  ASSERT_EQ(node_s, c_cleanup.first);
  ASSERT_EQ(true, c_cleanup.second);
}

// Authentication tests
class SyncerAuthenticationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clear any existing environment variables and reset state
    ray::UnsetEnv("RAY_AUTH_TOKEN");
    ray::rpc::AuthenticationTokenLoader::instance().ResetCache();
    RayConfig::instance().auth_mode() = "disabled";
  }

  void TearDown() override {
    ray::UnsetEnv("RAY_AUTH_TOKEN");
    ray::rpc::AuthenticationTokenLoader::instance().ResetCache();
    RayConfig::instance().auth_mode() = "disabled";
  }

  struct AuthenticatedSyncerServerTest {
    std::string server_port;
    instrumented_io_context io_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
    std::unique_ptr<std::thread> thread;
    std::unique_ptr<RaySyncer> syncer;
    std::unique_ptr<RaySyncerService> service;
    std::unique_ptr<grpc::Server> server;

    AuthenticatedSyncerServerTest(const std::string &port, const std::string &token)
        : server_port(port), work_guard(io_context.get_executor()) {
      // Setup syncer and grpc server
      syncer = std::make_unique<RaySyncer>(io_context, NodeID::FromRandom().Binary());
      thread = std::make_unique<std::thread>([this] { io_context.run(); });

      // Create service with authentication token
      service = std::make_unique<RaySyncerService>(
          *syncer,
          token.empty() ? std::nullopt
                        : std::make_optional(ray::rpc::AuthenticationToken(token)));

      auto server_address = BuildAddress("0.0.0.0", port);
      grpc::ServerBuilder builder;
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(service.get());
      server = builder.BuildAndStart();
    }

    ~AuthenticatedSyncerServerTest() {
      server->Shutdown();
      server->Wait();
      work_guard.reset();
      io_context.stop();
      thread->join();
    }
  };

  std::unique_ptr<AuthenticatedSyncerServerTest> CreateAuthenticatedServer(
      const std::string &port, const std::string &token) {
    return std::make_unique<AuthenticatedSyncerServerTest>(port, token);
  }

  // Helper struct to manage client io_context and syncer
  struct ClientSyncer {
    instrumented_io_context io_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
    std::thread thread;
    std::unique_ptr<RaySyncer> syncer;
    std::string remote_node_id;

    ClientSyncer()
        : work_guard(boost::asio::make_work_guard(io_context.get_executor())),
          thread([this]() { io_context.run(); }) {
      syncer = std::make_unique<RaySyncer>(io_context, NodeID::FromRandom().Binary());
      remote_node_id = NodeID::FromRandom().Binary();
    }

    ~ClientSyncer() {
      if (syncer) {
        syncer->Disconnect(remote_node_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        syncer.reset();
      }
      work_guard.reset();
      io_context.stop();
      thread.join();
    }

    void Connect(const std::shared_ptr<grpc::Channel> &channel) {
      syncer->Connect(remote_node_id, channel);
    }
  };
};

TEST_F(SyncerAuthenticationTest, MatchingTokens) {
  // Test that connections succeed when client and server use the same token
  const std::string test_token = "matching-test-token-12345";

  // Set client token via environment variable
  ray::SetEnv("RAY_AUTH_TOKEN", test_token);
  // Enable token authentication
  RayConfig::instance().auth_mode() = "token";
  ray::rpc::AuthenticationTokenLoader::instance().ResetCache();

  // Create authenticated server
  auto server = CreateAuthenticatedServer("37892", test_token);

  // Create client with separate io_context
  ClientSyncer client;
  auto channel = grpc::CreateChannel(BuildAddress("0.0.0.0", "37892"),
                                     grpc::InsecureChannelCredentials());

  // Should connect successfully with matching token
  client.Connect(channel);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify connection is established
  ASSERT_GT(client.syncer->GetAllConnectedNodeIDs().size(), 0);
}

TEST_F(SyncerAuthenticationTest, MismatchedTokens) {
  // Test that connections fail when client and server use different tokens
  const std::string server_token = "server-token-12345";
  const std::string client_token = "different-client-token";

  // Set client token via environment variable
  ray::SetEnv("RAY_AUTH_TOKEN", client_token);
  // Enable token authentication
  RayConfig::instance().auth_mode() = "token";
  ray::rpc::AuthenticationTokenLoader::instance().ResetCache();

  // Create authenticated server with different token
  auto server = CreateAuthenticatedServer("37893", server_token);

  // Create client with separate io_context
  ClientSyncer client;
  auto channel = grpc::CreateChannel(BuildAddress("0.0.0.0", "37893"),
                                     grpc::InsecureChannelCredentials());

  // Should fail to connect with mismatched token
  client.Connect(channel);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify connection fails - no connected nodes
  ASSERT_EQ(client.syncer->GetAllConnectedNodeIDs().size(), 0);
}

TEST_F(SyncerAuthenticationTest, ServerHasTokenClientDoesNot) {
  // Test that connections fail when server requires token but client doesn't provide it
  const std::string server_token = "server-token-12345";

  // Client has no token - auth mode is disabled (default from SetUp)
  ray::UnsetEnv("RAY_AUTH_TOKEN");
  ray::rpc::AuthenticationTokenLoader::instance().ResetCache();

  // Create authenticated server
  auto server = CreateAuthenticatedServer("37895", server_token);

  // Create client with separate io_context
  ClientSyncer client;
  auto channel = grpc::CreateChannel(BuildAddress("0.0.0.0", "37895"),
                                     grpc::InsecureChannelCredentials());

  // Should fail to connect without token
  client.Connect(channel);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify connection fails - no connected nodes
  ASSERT_EQ(client.syncer->GetAllConnectedNodeIDs().size(), 0);
}

TEST_F(SyncerAuthenticationTest, ClientHasTokenServerDoesNotRequire) {
  // Test that connections succeed when client has token but server doesn't require it
  const std::string server_token = "";
  const std::string client_token = "different-client-token";

  // Set client token
  ray::SetEnv("RAY_AUTH_TOKEN", client_token);
  // Enable token authentication
  RayConfig::instance().auth_mode() = "token";
  ray::rpc::AuthenticationTokenLoader::instance().ResetCache();

  // Create server without authentication (empty token)
  auto server = CreateAuthenticatedServer("37896", server_token);

  // Create client with separate io_context
  ClientSyncer client;
  auto channel = grpc::CreateChannel(BuildAddress("0.0.0.0", "37896"),
                                     grpc::InsecureChannelCredentials());

  // Should connect successfully - server accepts any client when auth is not required
  client.Connect(channel);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify connection is established
  ASSERT_GT(client.syncer->GetAllConnectedNodeIDs().size(), 0);
}

}  // namespace syncer
}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      ray::RayLog::ShutDownRayLog,
      argv[0],
      ray::RayLogLevel::INFO,
      ray::GetLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::GetErrLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
      ray::RayLog::GetRayLogRotationBackupCountOrDefault());
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ray::RayLog::InstallTerminateHandler();

  ::testing::InitGoogleTest(&argc, argv);
  auto ret = RUN_ALL_TESTS();
  // Sleep for gRPC to gracefully shutdown.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return ret;
}
