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

#include "ray/gcs/store_client/redis_multi_reader.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisMultiReaderTest : public StoreClientTestBase {
 public:
  typedef std::pair<std::string, std::string> Row;

  RedisMultiReaderTest() {}

  void InitStoreClient() override {
    RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    store_client_ =
        std::make_shared<RedisStoreClient<ActorID, rpc::ActorTableData, JobID>>(
            redis_client_);

    auto keys = GetStringKeys();
    multi_reader_ = std::make_shared<RedisMultiReader>(redis_client_, keys);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

  std::vector<std::string> GetStringKeys() {
    std::vector<std::string> string_keys;
    for (const auto &item : key_to_value_) {
      std::string full_key = table_name_ + item.first.Binary();
      string_keys.emplace_back(full_key);
    }
    return string_keys;
  }

  void OnReadCallback(Status status, const std::vector<Row> &result) {
    RAY_CHECK_OK(status);
    for (const auto &row : result) {
      std::string key = row.first.substr(table_name_.length());
      ActorID actor_id = ActorID::FromBinary(key);
      rpc::ActorTableData actor_data;
      ASSERT_TRUE(actor_data.ParseFromString(row.second));
      ASSERT_EQ(key, actor_data.actor_id());

      auto it = key_to_value_.find(actor_id);
      ASSERT_TRUE(it != key_to_value_.end());
    }
    ASSERT_EQ(result.size(), key_to_value_.size());
    --pending_count_;
  }

 protected:
  std::shared_ptr<RedisClient> redis_client_;
  std::shared_ptr<RedisMultiReader> multi_reader_;
};

TEST_F(RedisMultiReaderTest, ReadTest) {
  WriteDataToStore();

  auto read_callback = [this](Status status, const std::vector<Row> &result) {
    OnReadCallback(status, result);
  };

  ++pending_count_;
  Status status = multi_reader_->Read(read_callback);
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
