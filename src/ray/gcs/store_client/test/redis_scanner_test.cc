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

#include "ray/gcs/store_client/redis_scanner.h"
#include <unistd.h>
#include <chrono>
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisScannerTest : public StoreClientTestBase {
 public:
  typedef std::pair<std::string, std::string> Row;

  RedisScannerTest() {}

  void InitStoreClient() override {
    RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    store_client_ =
        std::make_shared<RedisStoreClient<ActorID, rpc::ActorTableData, JobID>>(
            redis_client_);

    // Generate table name by random number so that each test writes
    // different keys to the storage.
    table_name_ += std::to_string(GenRandomNum());
    std::string match_pattern = table_name_ + "*";
    scanner_ = std::make_shared<RedisScanner>(redis_client_, match_pattern);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

  size_t GenRandomNum() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> random_gen{1, 10000};
    return random_gen(gen);
  }

  void OnScanRowCallback(Status status, bool has_more, const std::vector<Row> &result) {
    RAY_CHECK_OK(status);
    for (const auto &row : result) {
      std::string actor_id_str = row.first.substr(table_name_.length());
      ActorID actor_id = ActorID::FromBinary(actor_id_str);
      rpc::ActorTableData actor_data;
      ASSERT_TRUE(actor_data.ParseFromString(row.second));
      ASSERT_EQ(actor_id_str, actor_data.actor_id());

      auto it = key_to_value_.find(actor_id);
      ASSERT_TRUE(it != key_to_value_.end()) << key_to_value_.size();
      received_rows_.emplace_back(row);
    }

    if (!has_more) {
      ASSERT_EQ(received_rows_.size(), key_to_value_.size());
      --pending_count_;
    } else {
      DoScanPartialRows();
    }
  }

  void DoScanPartialRows() {
    auto scan_callback = [this](Status status, bool has_more,
                                const std::vector<Row> &result) {
      OnScanRowCallback(status, has_more, result);
    };

    Status status = scanner_->ScanPartialRows(scan_callback);
    RAY_CHECK_OK(status);
  }

  void OnScanKeyCallback(Status status, bool has_more,
                         const std::vector<std::string> &result) {
    RAY_CHECK_OK(status);
    for (const auto &key : result) {
      std::string actor_id_str = key.substr(table_name_.length());
      ActorID actor_id = ActorID::FromBinary(actor_id_str);
      auto it = key_to_value_.find(actor_id);
      ASSERT_TRUE(it != key_to_value_.end());
      received_keys_.emplace_back(key);
    }

    if (!has_more) {
      ASSERT_EQ(received_keys_.size(), key_to_value_.size());
      --pending_count_;
    } else {
      DoScanPartialKeys();
    }
  }

  void DoScanPartialKeys() {
    auto scan_callback = [this](Status status, bool has_more,
                                const std::vector<std::string> &result) {
      OnScanKeyCallback(status, has_more, result);
    };

    Status status = scanner_->ScanPartialKeys(scan_callback);
    RAY_CHECK_OK(status);
  }

 protected:
  std::shared_ptr<RedisClient> redis_client_;
  std::shared_ptr<RedisScanner> scanner_;

  std::vector<Row> received_rows_;

  std::vector<std::string> received_keys_;
};

TEST_F(RedisScannerTest, ScanPartialRowsTest) {
  WriteDataToStore();

  ++pending_count_;
  DoScanPartialRows();

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanRowsTest) {
  WriteDataToStore();

  auto scan_callback = [this](Status status, const std::vector<Row> &result) {
    OnScanRowCallback(status, false, result);
  };

  ++pending_count_;
  Status status = scanner_->ScanRows(scan_callback);
  RAY_CHECK_OK(status);

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanPartialKeysTest) {
  WriteDataToStore();

  ++pending_count_;
  DoScanPartialKeys();

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanKeysTest) {
  WriteDataToStore();

  auto scan_callback = [this](Status status, const std::vector<std::string> &result) {
    OnScanKeyCallback(status, false, result);
  };

  ++pending_count_;
  Status status = scanner_->ScanKeys(scan_callback);
  RAY_CHECK_OK(status);

  WaitPendingDone();
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
