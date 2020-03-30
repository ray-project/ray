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

#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisStoreClientTest : public StoreClientTestBase {
 public:
  RedisStoreClientTest() {}

  virtual ~RedisStoreClientTest() {}

  void InitStoreClient() override {
    RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    store_client_ = std::make_shared<RedisStoreClient>(options);
    Status status = store_client_->Connect(io_service_pool_);
    RAY_CHECK_OK(status);
  }

  void DisconnectStoreClient() override { store_client_->Disconnect(); }
};

TEST_F(RedisStoreClientTest, AsyncPutAndAsyncGetTest) {
  // AsyncPut without index.
  auto put_calllback = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status = store_client_->AsyncPut(table_name_, elem.first.Binary(), elem.second,
                                            put_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncGet
  auto get_callback = [this](Status status, const boost::optional<std::string> &result) {
    RAY_CHECK_OK(status);
    RAY_CHECK(result);
    rpc::ActorTableData actor_data;
    RAY_CHECK(actor_data.ParseFromString(result.value()));
    ActorID actor_id = ActorID::FromBinary(actor_data.actor_id());
    auto it = key_to_value_.find(actor_id);
    RAY_CHECK(it != key_to_value_.end());
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status =
        store_client_->AsyncGet(table_name_, elem.first.Binary(), get_callback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();
}

TEST_F(RedisStoreClientTest, AsyncDeleteTest) {
  // AsyncPut
  auto put_calllback = [this](Status status) { --pending_count_; };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status = store_client_->AsyncPut(table_name_, elem.first.Binary(), elem.second,
                                            put_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncDelete
  auto delete_calllback = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status =
        store_client_->AsyncDelete(table_name_, elem.first.Binary(), delete_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncGet
  auto get_callback = [this](Status status, const boost::optional<std::string> &result) {
    RAY_CHECK_OK(status);
    RAY_CHECK(!result);
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status =
        store_client_->AsyncGet(table_name_, elem.first.Binary(), get_callback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();
}

TEST_F(RedisStoreClientTest, DISABLED_AsyncGetAllTest) {
  // AsyncPut
  auto put_calllback = [this](Status status) { --pending_count_; };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    // Get index
    auto it = key_to_index_.find(elem.first);
    const JobID &index = it->second;
    Status status = store_client_->AsyncPutWithIndex(
        table_name_, elem.first.Binary(), index.Binary(), elem.second, put_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncGetAll
  auto get_all_callback =
      [this](Status status, bool has_more,
             const std::vector<std::pair<std::string, std::string>> &result) {
        RAY_CHECK_OK(status);
        static std::unordered_set<ActorID> received_keys;
        for (const auto &item : result) {
          ActorID actor_id = ActorID::FromBinary(item.first);
          auto it = received_keys.find(actor_id);
          RAY_CHECK(it == received_keys.end());
          received_keys.emplace(actor_id);

          auto map_it = key_to_value_.find(actor_id);
          RAY_CHECK(map_it != key_to_value_.end());
        }
        if (!has_more) {
          RAY_CHECK(received_keys.size() == key_to_value_.size());
        }
        pending_count_ -= result.size();
      };

  pending_count_ += key_to_value_.size();
  Status status = store_client_->AsyncGetAll(table_name_, get_all_callback);
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
