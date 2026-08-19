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
#include <string>

#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/store_client/observable_store_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/table_name_label.h"
#include "ray/gcs/tests/gcs_table_storage_test_base.h"
#include "ray/observability/fake_metric.h"
#include "ray/util/clock.h"
#include "src/ray/protobuf/gcs.pb.h"

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
};

TEST_F(RedisGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApi) { TestGcsTableWithJobIdApi(); }

/// gcs_storage_operation_count and gcs_storage_operation_latency_ms do not exist
/// on the external-Redis backend unless GcsServer routes the store client
/// through gcs::MaybeObserve, so that branch is the whole of the Redis support
/// added with the TableName label. This exercises it against a real Redis, in
/// both configurations, with table names that come from GcsTableStorage rather
/// than from a test constant.
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
        clock_));
  }

  double CountFor(const std::string &operation, const std::string &table) const {
    for (const auto &[tags, value] : count_.GetTagToValue()) {
      auto op = tags.find("Operation");
      auto tbl = tags.find("TableName");
      if (op != tags.end() && op->second == operation && tbl != tags.end() &&
          tbl->second == table) {
        return value;
      }
    }
    return 0;
  }

  Clock clock_;
  ray::observability::FakeHistogram latency_;
  ray::observability::FakeCounter count_;
};

TEST_F(RedisObservableGcsTableStorageTest, RecordsTableNameOnTheRedisBackend) {
  BuildStorage(/*metrics_enabled=*/true);

  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);
  Put(gcs_table_storage_->JobTable(), job_id, *job_table_data);

  const std::string job_table = rpc::TablePrefix_Name(rpc::TablePrefix::JOB);
  EXPECT_EQ(CountFor("Put", job_table), 1)
      << "the Redis backend must attribute the operation to the table it wrote";

  // Nothing user supplied, and nothing outside the bounded domain.
  for (const auto &[tags, unused_value] : count_.GetTagToValue()) {
    auto table = tags.find("TableName");
    ASSERT_TRUE(table != tags.end());
    EXPECT_NE(table->second, gcs::kUnknownTable);
    EXPECT_EQ(table->second.find(job_id.Hex()), std::string::npos);
  }
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
