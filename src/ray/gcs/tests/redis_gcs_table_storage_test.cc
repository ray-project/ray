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
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/store_client/observable_store_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/tests/gcs_table_storage_test_base.h"
#include "ray/observability/fake_metric.h"
#include "ray/util/clock.h"

namespace ray {

class RedisGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    auto &io_service = *io_service_pool_->Get();
    gcs::RedisClientOptions options{"127.0.0.1", TEST_REDIS_SERVER_PORTS.front()};
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(
        std::make_unique<gcs::RedisStoreClient>(io_service, options, clock_));
  }

  void TearDown() override {}

  Clock clock_;
  boost::asio::io_context metric_context_;
};

TEST_F(RedisGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApi) { TestGcsTableWithJobIdApi(); }

/// Exercises gcs::MaybeObserve against a real Redis, with metrics enabled and
/// disabled.
class RedisObservableGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  // The storage is built per test rather than in SetUp, because the two tests
  // differ only in the flag MaybeObserve is called with. io_service_pool_ comes
  // from the base's constructor and is already running.
  void SetUp() override {}

  void TearDown() override {}

 protected:
  void BuildStorage(bool metrics_enabled) {
    auto &io_service = *io_service_pool_->Get();
    gcs::RedisClientOptions options{"127.0.0.1", TEST_REDIS_SERVER_PORTS.front()};
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(gcs::MaybeObserve(
        std::make_shared<gcs::RedisStoreClient>(io_service, options, clock_),
        metrics_enabled,
        latency_,
        count_,
        clock_,
        metric_context_));
  }

  using Recorded =
      absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>;

  static std::optional<double> ValueFor(const Recorded &recorded,
                                        const std::string &operation,
                                        const std::string &table) {
    for (const auto &[tags, value] : recorded) {
      auto op = tags.find("Operation");
      auto tbl = tags.find("TableName");
      if (op != tags.end() && op->second == operation && tbl != tags.end() &&
          tbl->second == table) {
        return value;
      }
    }
    return std::nullopt;
  }

  Clock clock_;
  ray::observability::FakeHistogram latency_;
  ray::observability::FakeCounter count_;
  boost::asio::io_context metric_context_;
};

TEST_F(RedisObservableGcsTableStorageTest, RecordsOnTheRedisBackend) {
  BuildStorage(/*metrics_enabled=*/true);

  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);
  Put(gcs_table_storage_->JobTable(), job_id, *job_table_data);
  metric_context_.restart();
  metric_context_.run();

  EXPECT_EQ(ValueFor(count_.GetTagToValue(), "Put", "JOB").value_or(0), 1);
  // FakeHistogram keeps the last observation, so only its presence is stable.
  EXPECT_TRUE(ValueFor(latency_.GetTagToValue(), "Put", "JOB").has_value());
}

TEST_F(RedisObservableGcsTableStorageTest, RecordsNothingWhenTheKillSwitchIsOff) {
  BuildStorage(/*metrics_enabled=*/false);

  JobID job_id = JobID::FromInt(2);
  auto job_table_data = GenJobTableData(job_id);
  Put(gcs_table_storage_->JobTable(), job_id, *job_table_data);

  EXPECT_TRUE(count_.GetTagToValue().empty());
  EXPECT_TRUE(latency_.GetTagToValue().empty());
}

}  // namespace ray
