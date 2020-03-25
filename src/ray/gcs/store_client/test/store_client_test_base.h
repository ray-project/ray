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

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ray/common/test_util.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class StoreClientTestBase : public RedisServiceManagerForTest {
 public:
  StoreClientTestBase() {}

  virtual ~StoreClientTestBase() {}

  void SetUp() override {
    io_service_pool_ = std::make_shared<IOServicePool>(io_service_num_);
    io_service_pool_->Run();
    InitStoreClient();
    GenTestData();
  }

  void TearDown() override {
    DisconnectStoreClient();

    io_service_pool_->Stop();

    key_to_value_.clear();
    key_to_index_.clear();
    index_to_keys_.clear();
  }

  virtual void InitStoreClient() = 0;

  virtual void DisconnectStoreClient() = 0;

 protected:
  void GenTestData() {
    for (size_t i = 0; i < key_count_; i++) {
      std::string key = std::to_string(i);
      key_to_value_[key] = key;

      std::string index = std::to_string(i % index_count_);
      key_to_index_[key] = index;

      auto it = index_to_keys_.find(index);
      if (it != index_to_keys_.end()) {
        it->second.emplace(key);
      } else {
        std::unordered_set<std::string> key_set;
        key_set.emplace(key);
        index_to_keys_.emplace(index, std::move(key_set));
      }
    }
  }

  void WaitPendingDone() { WaitPendingDone(pending_count_); }

  void WaitPendingDone(std::atomic<int> &pending_count) {
    auto condition = [&pending_count]() { return pending_count == 0; };
    EXPECT_TRUE(WaitForCondition(condition, wait_pending_timeout_.count()));
  }

 protected:
  size_t io_service_num_{2};
  std::shared_ptr<IOServicePool> io_service_pool_;

  std::string table_name_{"test_table"};
  size_t key_count_{5000};
  size_t index_count_{100};
  std::unordered_map<std::string, std::string> key_to_value_;
  std::unordered_map<std::string, std::string> key_to_index_;
  std::unordered_map<std::string, std::unordered_set<std::string>> index_to_keys_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{5000};
};

}  // namespace gcs

}  // namespace ray
