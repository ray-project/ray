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
#include "ray/common/ray_syncer/ray_syncer.h"
#include "mock/ray/common/ray_syncer/ray_syncer.h"
// clang-format on

using namespace ray::syncer;
using ::testing::_;
using ::testing::Invoke;
using ::testing::WithArg;

class RaySyncerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    for (size_t cid = 0; cid < reporters_.size(); ++cid) {
      receivers_[cid] = std::make_unique<MockReceiverInterface>();
      auto &reporter = reporters_[cid];
      reporter = std::make_unique<MockReporterInterface>();
      auto take_snapshot =
          [this, cid](uint64_t curr_version) mutable -> std::optional<RaySyncMessage> {
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
  }

  MockReporterInterface *GetReporter(RayComponentId cid) {
    return reporters_[static_cast<size_t>(cid)].get();
  }

  MockReceiverInterface *GetReceiver(RayComponentId cid) {
    return receivers_[static_cast<size_t>(cid)].get();
  }

  uint64_t &LocalVersion(RayComponentId cid) {
    return local_versions_[static_cast<size_t>(cid)];
  }

  void TearDown() override {}

  Array<uint64_t> local_versions_ = {1};
  Array<std::unique_ptr<MockReporterInterface>> reporters_ = {nullptr};
  Array<std::unique_ptr<MockReceiverInterface>> receivers_ = {nullptr};
};

TEST_F(RaySyncerTest, NodeStatusGetSnapshot) {
  auto node_status = std::make_unique<NodeStatus>();
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

TEST_F(RaySyncerTest, NodeStatusConsume) {
  auto node_status = std::make_unique<NodeStatus>();
  node_status->SetComponents(RayComponentId::RESOURCE_MANAGER,
                             nullptr,
                             GetReceiver(RayComponentId::RESOURCE_MANAGER));
  auto msg = RaySyncMessage();
  msg.set_version(0);
  msg.set_component_id(RayComponentId::RESOURCE_MANAGER);
  msg.set_node_id("a");
  // The first time receiver the message
  ASSERT_TRUE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));

  msg.set_version(1);
  ASSERT_TRUE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
  ASSERT_FALSE(node_status->ConsumeMessage(std::make_shared<RaySyncMessage>(msg)));
}
