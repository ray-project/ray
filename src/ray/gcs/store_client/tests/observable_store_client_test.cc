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

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/asio/io_service_pool.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"
#include "ray/util/clock.h"

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

// GcsServer routes the Redis backend through MaybeObserve, which is the only
// thing that makes gcs_storage_operation_* exist on that backend at all. The
// delegate type is irrelevant to the branch, so it is exercised here over the
// in-memory client; RedisObservableGcsTableStorageTest covers the same branch
// against a real Redis.
class MaybeObserveTest : public ::testing::Test {
 public:
  void SetUp() override {
    io_service_pool_ = std::make_shared<IOServicePool>(1);
    io_service_pool_->Run();
  }

  void TearDown() override { io_service_pool_->Stop(); }

 protected:
  // Issues one Put and waits for it to complete, so the latency observer has
  // fired by the time the test asserts.
  void PutAndWait(StoreClient &client) {
    std::atomic<bool> done{false};
    client.AsyncPut("table",
                    "key",
                    "value",
                    /*overwrite=*/true,
                    {[&done](bool) { done = true; }, *io_service_pool_->Get()});
    ASSERT_TRUE(WaitForCondition([&done] { return done.load(); }, 5000));
  }

  using Recorded =
      absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>;

  // Matches on Operation alone rather than on the whole tag set, so the test
  // does not depend on which other tags the wrapper records.
  static std::optional<double> ValueFor(const Recorded &recorded,
                                        const std::string &operation) {
    for (const auto &[tags, value] : recorded) {
      auto op = tags.find("Operation");
      if (op != tags.end() && op->second == operation) {
        return value;
      }
    }
    return std::nullopt;
  }

  std::shared_ptr<IOServicePool> io_service_pool_;
  ray::FakeClock clock_;
  ray::observability::FakeHistogram latency_;
  ray::observability::FakeCounter count_;
};

TEST_F(MaybeObserveTest, RecordsWhenEnabled) {
  auto client = MaybeObserve(std::make_shared<InMemoryStoreClient>(),
                             /*enabled=*/true,
                             latency_,
                             count_,
                             clock_);
  PutAndWait(*client);

  EXPECT_EQ(ValueFor(count_.GetTagToValue(), "Put").value_or(0), 1);
  // FakeHistogram keeps the last observation, so only its presence is stable.
  EXPECT_TRUE(ValueFor(latency_.GetTagToValue(), "Put").has_value());
}

TEST_F(MaybeObserveTest, RecordsNothingWhenDisabled) {
  auto delegate = std::make_shared<InMemoryStoreClient>();
  auto client = MaybeObserve(delegate, /*enabled=*/false, latency_, count_, clock_);
  // The kill switch must hand back the delegate itself, not a silent wrapper.
  EXPECT_EQ(client.get(), static_cast<StoreClient *>(delegate.get()));

  PutAndWait(*client);

  EXPECT_TRUE(count_.GetTagToValue().empty());
  EXPECT_TRUE(latency_.GetTagToValue().empty());
}

}  // namespace gcs

}  // namespace ray
