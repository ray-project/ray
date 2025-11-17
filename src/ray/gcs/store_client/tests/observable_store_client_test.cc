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

#include <memory>

#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"

namespace ray {

namespace gcs {

class ObservableStoreClientTest : public StoreClientTestBase {
 public:
  void InitStoreClient() override {
    store_client_ = std::make_shared<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(),
        fake_storage_operation_latency_in_ms_histogram_,
        fake_storage_operation_count_counter_);
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

  ray::observability::FakeHistogram fake_storage_operation_latency_in_ms_histogram_;
  ray::observability::FakeCounter fake_storage_operation_count_counter_;
};

TEST_F(ObservableStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(ObservableStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
