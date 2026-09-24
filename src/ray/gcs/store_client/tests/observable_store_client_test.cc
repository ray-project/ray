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

#include "ray/gcs/store_client/observable_store_client.h"

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/asio/io_service_pool.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/table_name_label.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"
#include "ray/util/clock.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

class ObservableStoreClientTest : public StoreClientTestBase {
 public:
  void InitStoreClient() override {
    store_client_ = std::make_shared<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(),
        fake_storage_operation_latency_in_ms_histogram_,
        fake_storage_operation_count_counter_,
        clock_);
  }

  void TestMetrics() override {
    auto counter_tag_to_value = fake_storage_operation_count_counter_.GetTagToValue();
    // 3 operations: Put, Get, Delete
    // Get operations include both Get() and GetEmpty() calls, so they're grouped together
    ASSERT_EQ(counter_tag_to_value.size(), 3);

    // Check each operation type individually
    for (const auto &[key, value] : counter_tag_to_value) {
      // Find the operation type
      std::string operation_type;
      for (const auto &[k, v] : key) {
        if (k == "Operation") {
          operation_type = v;
          break;
        }
      }

      if (operation_type == "Put" || operation_type == "Delete") {
        ASSERT_EQ(value, 5000) << "Expected 5000 for " << operation_type << " operation";
      } else if (operation_type == "Get") {
        ASSERT_EQ(value, 10000) << "Expected 10000 for Get operation (5000 from Get() + "
                                   "5000 from GetEmpty())";
      }
    }

    auto latency_tag_to_value =
        fake_storage_operation_latency_in_ms_histogram_.GetTagToValue();
    // 3 operations: Put, Get, Delete
    ASSERT_EQ(latency_tag_to_value.size(), 3);
  }

  ray::FakeClock clock_;
  ray::observability::FakeHistogram fake_storage_operation_latency_in_ms_histogram_;
  ray::observability::FakeCounter fake_storage_operation_count_counter_;
};

TEST_F(ObservableStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(ObservableStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

// Drives ObservableStoreClient directly with real GCS table names, which
// StoreClientTestBase cannot do -- it shares a single "test_table" with the
// in-memory, Redis and RocksDB store client tests, and that name is deliberately
// outside the label domain.
class ObservableStoreClientTableLabelTest : public ::testing::Test {
 public:
  void SetUp() override {
    io_service_pool_ = std::make_shared<IOServicePool>(1);
    io_service_pool_->Run();
    store_client_ = std::make_shared<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(), latency_, count_, clock_);
  }

  void TearDown() override { io_service_pool_->Stop(); }

 protected:
  using TagMap = absl::flat_hash_map<std::string, std::string>;
  using Recorded = absl::flat_hash_map<TagMap, double>;

  static double ValueFor(const Recorded &recorded,
                         std::string_view operation,
                         std::string_view table) {
    for (const auto &[tags, value] : recorded) {
      auto op = tags.find("Operation");
      auto tbl = tags.find("TableName");
      if (op != tags.end() && op->second == operation && tbl != tags.end() &&
          tbl->second == table) {
        return value;
      }
    }
    return 0;
  }

  static bool HasLabelPair(const Recorded &recorded,
                           std::string_view operation,
                           std::string_view table) {
    for (const auto &[tags, unused_value] : recorded) {
      auto op = tags.find("Operation");
      auto tbl = tags.find("TableName");
      if (op != tags.end() && op->second == operation && tbl != tags.end() &&
          tbl->second == table) {
        return true;
      }
    }
    return false;
  }

  // find-with-default rather than at(): a tag missing from a recorded set
  // should fail the assertion with a readable message, not throw
  // std::out_of_range out of the test body.
  static std::string TagOr(const TagMap &tags,
                           std::string_view key,
                           std::string_view fallback) {
    auto it = tags.find(key);
    return it == tags.end() ? std::string(fallback) : it->second;
  }

  static std::string Describe(const TagMap &tags) {
    std::string described;
    for (const auto &[tag_key, tag_value] : tags) {
      if (!described.empty()) {
        described += ", ";
      }
      described += tag_key;
      described += "=";
      described += tag_value;
    }
    return described;
  }

  instrumented_io_context &io() { return *io_service_pool_->Get(); }

  void WaitPending() {
    ASSERT_TRUE(WaitForCondition([this] { return pending_ == 0; }, 5000));
  }

  // Runs every instrumented operation once against `table`, waiting for each to
  // complete so the latency observer has fired by the time the test asserts.
  void RunAllOperations(const std::string &table) {
    const std::string key = "key";

    pending_ = 1;
    store_client_->AsyncPut(
        table, key, "value", /*overwrite=*/true, {[this](bool) { --pending_; }, io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncGet(
        table,
        key,
        {[this](const Status &, const std::optional<std::string> &) { --pending_; },
         io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncGetAll(
        table,
        {[this](const absl::flat_hash_map<std::string, std::string> &) { --pending_; },
         io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncMultiGet(
        table,
        {key},
        {[this](const absl::flat_hash_map<std::string, std::string> &) { --pending_; },
         io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncGetKeys(
        table, "k", {[this](const std::vector<std::string> &) { --pending_; }, io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncExists(table, key, {[this](bool) { --pending_; }, io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncDelete(table, key, {[this](bool) { --pending_; }, io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncBatchDelete(
        table, {key}, {[this](int64_t) { --pending_; }, io()});
    WaitPending();

    pending_ = 1;
    store_client_->AsyncGetNextJobID({[this](int) { --pending_; }, io()});
    WaitPending();
  }

  std::shared_ptr<IOServicePool> io_service_pool_;
  std::shared_ptr<StoreClient> store_client_;
  ray::FakeClock clock_;
  ray::observability::FakeHistogram latency_;
  ray::observability::FakeCounter count_;
  std::atomic<int> pending_{0};
};

// The issue's reproduction: the same Operation on two tables must be
// distinguishable rather than collapsed into one series.
TEST_F(ObservableStoreClientTableLabelTest, SameOperationDistinctTables) {
  const std::string actor_table = rpc::TablePrefix_Name(rpc::TablePrefix::ACTOR);
  const std::string job_table = rpc::TablePrefix_Name(rpc::TablePrefix::JOB);

  pending_ = 2;
  store_client_->AsyncGetAll(
      actor_table,
      {[this](const absl::flat_hash_map<std::string, std::string> &) { --pending_; },
       io()});
  store_client_->AsyncGetAll(
      job_table,
      {[this](const absl::flat_hash_map<std::string, std::string> &) { --pending_; },
       io()});
  WaitPending();

  const auto recorded = count_.GetTagToValue();
  EXPECT_EQ(ValueFor(recorded, "GetAll", actor_table), 1);
  EXPECT_EQ(ValueFor(recorded, "GetAll", job_table), 1);
  EXPECT_EQ(recorded.size(), 2u) << "one GetAll series per table, not one in total";
}

TEST_F(ObservableStoreClientTableLabelTest, LatencyCarriesTheSameLabelPair) {
  const std::string node_table = rpc::TablePrefix_Name(rpc::TablePrefix::NODE);
  RunAllOperations(node_table);

  const auto counted = count_.GetTagToValue();
  const auto timed = latency_.GetTagToValue();
  for (const auto &[tags, unused_value] : counted) {
    EXPECT_TRUE(timed.contains(tags))
        << "counter and histogram disagree on the label set: " << Describe(tags);
  }
  EXPECT_EQ(counted.size(), timed.size());
}

TEST_F(ObservableStoreClientTableLabelTest, AllOperationsAreLabeled) {
  const std::string worker_table = rpc::TablePrefix_Name(rpc::TablePrefix::WORKERS);
  RunAllOperations(worker_table);

  const std::unordered_set<std::string> expected_operations = {"Put",
                                                               "Get",
                                                               "GetAll",
                                                               "MultiGet",
                                                               "GetKeys",
                                                               "Exists",
                                                               "Delete",
                                                               "BatchDelete",
                                                               "GetNextJobID"};
  std::unordered_set<std::string> seen_operations;
  for (const auto &[tags, unused_value] : count_.GetTagToValue()) {
    EXPECT_TRUE(tags.contains("TableName"))
        << "every site must record TableName -- both metric stacks export a "
           "declared tag with an empty value rather than omitting it: "
        << Describe(tags);
    EXPECT_FALSE(TagOr(tags, "TableName", "").empty()) << Describe(tags);
    seen_operations.insert(TagOr(tags, "Operation", "<missing>"));
  }
  EXPECT_EQ(seen_operations, expected_operations);

  // AsyncGetNextJobID addresses a counter key, not a table.
  EXPECT_EQ(ValueFor(count_.GetTagToValue(), "GetNextJobID", kJobCounterTable), 1);
  // Everything else is attributed to the table it was called with.
  EXPECT_TRUE(HasLabelPair(count_.GetTagToValue(), "Put", worker_table));
}

// The cardinality guarantee, asserted at the layer that makes it.
TEST_F(ObservableStoreClientTableLabelTest, UnknownTableCollapsesInTheWrapper) {
  pending_ = 1;
  store_client_->AsyncPut("test_table",
                          "key",
                          "value",
                          /*overwrite=*/true,
                          {[this](bool) { --pending_; }, io()});
  WaitPending();

  const auto recorded = count_.GetTagToValue();
  EXPECT_EQ(ValueFor(recorded, "Put", kUnknownTable), 1);
  for (const auto &[tags, unused_value] : recorded) {
    EXPECT_NE(TagOr(tags, "TableName", "<missing>"), "test_table")
        << "the raw table name reached a label: " << Describe(tags);
  }
}

// The internal KV folds its user-controlled namespace into the key, and the
// wrapper only ever labels the table, so nothing a user supplies can become a
// label value.
TEST_F(ObservableStoreClientTableLabelTest, NoUserDataInLabels) {
  const std::string kv_table = rpc::TablePrefix_Name(rpc::TablePrefix::KV);
  const std::string user_key = "@namespace_secret_ns:secret_key";

  pending_ = 1;
  store_client_->AsyncPut(kv_table,
                          user_key,
                          "secret_value",
                          /*overwrite=*/true,
                          {[this](bool) { --pending_; }, io()});
  WaitPending();

  for (const auto &recorded : {count_.GetTagToValue(), latency_.GetTagToValue()}) {
    ASSERT_FALSE(recorded.empty());
    for (const auto &[tags, unused_value] : recorded) {
      for (const auto &[tag_key, tag_value] : tags) {
        EXPECT_EQ(tag_value.find("secret"), std::string::npos)
            << "user data leaked into label " << tag_key << "=" << tag_value;
      }
    }
  }
  EXPECT_EQ(ValueFor(count_.GetTagToValue(), "Put", kv_table), 1);
}

// GcsServer routes the Redis backend through MaybeObserve, which is the only
// thing that makes gcs_storage_operation_* exist on that backend at all. The
// delegate type is irrelevant to the branch, so it is exercised here over the
// in-memory client; RedisObservableGcsTableStorageTest covers the same branch
// against a real Redis.
TEST_F(ObservableStoreClientTableLabelTest, MaybeObserveRecordsWhenEnabled) {
  ray::observability::FakeHistogram latency;
  ray::observability::FakeCounter count;
  auto client = MaybeObserve(std::make_shared<InMemoryStoreClient>(),
                             /*enabled=*/true,
                             latency,
                             count,
                             clock_);
  const std::string node_table = rpc::TablePrefix_Name(rpc::TablePrefix::NODE);

  pending_ = 1;
  client->AsyncPut(node_table,
                   "key",
                   "value",
                   /*overwrite=*/true,
                   {[this](bool) { --pending_; }, io()});
  WaitPending();

  EXPECT_EQ(ValueFor(count.GetTagToValue(), "Put", node_table), 1);
  EXPECT_TRUE(HasLabelPair(latency.GetTagToValue(), "Put", node_table));
}

TEST_F(ObservableStoreClientTableLabelTest, MaybeObserveRecordsNothingWhenDisabled) {
  ray::observability::FakeHistogram latency;
  ray::observability::FakeCounter count;
  auto delegate = std::make_shared<InMemoryStoreClient>();
  auto client = MaybeObserve(delegate, /*enabled=*/false, latency, count, clock_);
  // The kill switch must hand back the delegate itself, not a silent wrapper.
  EXPECT_EQ(client.get(), static_cast<StoreClient *>(delegate.get()));

  pending_ = 1;
  client->AsyncPut(rpc::TablePrefix_Name(rpc::TablePrefix::NODE),
                   "key",
                   "value",
                   /*overwrite=*/true,
                   {[this](bool) { --pending_; }, io()});
  WaitPending();

  EXPECT_TRUE(count.GetTagToValue().empty());
  EXPECT_TRUE(latency.GetTagToValue().empty());
}

}  // namespace gcs

}  // namespace ray
