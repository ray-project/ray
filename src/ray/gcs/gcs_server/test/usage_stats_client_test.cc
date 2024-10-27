// Copyright 2024 The Ray Authors.
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

#include "ray/gcs/gcs_server/usage_stats_client.h"

#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/gcs/gcs_server/gcs_server.h"

using namespace ray;

class UsageStatsClientTest : public ::testing::Test {
 protected:
  void SetUp() override { fake_kv_ = std::make_unique<gcs::FakeInternalKVInterface>(); }
  void TearDown() override { fake_kv_.reset(); }
  std::unique_ptr<gcs::FakeInternalKVInterface> fake_kv_;
};

TEST_F(UsageStatsClientTest, TestRecordExtraUsageTag) {
  gcs::UsageStatsClient usage_stats_client(*fake_kv_);
  usage_stats_client.RecordExtraUsageTag(usage::TagKey::_TEST1, "value1");
  fake_kv_->Get(
      "usage_stats", "extra_usage_tag__test1", [](std::optional<std::string> value) {
        ASSERT_TRUE(value.has_value());
        ASSERT_EQ(value.value(), "value1");
      });
  // Make sure the value is overriden for the same key.
  usage_stats_client.RecordExtraUsageTag(usage::TagKey::_TEST2, "value2");
  fake_kv_->Get(
      "usage_stats", "extra_usage_tag__test2", [](std::optional<std::string> value) {
        ASSERT_TRUE(value.has_value());
        ASSERT_EQ(value.value(), "value2");
      });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
