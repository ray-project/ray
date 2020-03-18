#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisStoreClientTest : public StoreClientTestBase {
 public:
  RedisStoreClientTest() {}

  virtual ~RedisStoreClientTest() {}

  void InitStoreClient() override {
    StoreClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    store_client_ = std::make_shared<RedisStoreClient>(options);
  }
};

TEST_F(RedisStoreClientTest, AsyncPutAndAsyncGetTest) {
  // AsyncPut without index.
  auto put_calllback = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status =
        store_client_->AsyncPut(table_name_, elem.first, elem.second, put_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncGet
  auto get_callback = [this](Status status, const boost::optional<std::string> &result) {
    RAY_CHECK_OK(status);
    RAY_CHECK(result);
    auto it = key_to_value_.find(*result);
    RAY_CHECK(it != key_to_value_.end());
    --pending_count_;
  };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status = store_client_->AsyncGet(table_name_, elem.first, get_callback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();
}

TEST_F(RedisStoreClientTest, AsyncDeleteTest) {
  // AsyncPut
  auto put_calllback = [this](Status status) { --pending_count_; };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    Status status =
        store_client_->AsyncPut(table_name_, elem.first, elem.second, put_calllback);
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
    Status status = store_client_->AsyncDelete(table_name_, elem.first, delete_calllback);
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
    Status status = store_client_->AsyncGet(table_name_, elem.first, get_callback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();
}

TEST_F(RedisStoreClientTest, AsyncGetAllTest) {
  // AsyncPut
  auto put_calllback = [this](Status status) { --pending_count_; };
  for (const auto &elem : key_to_value_) {
    ++pending_count_;
    // Get index
    auto it = key_to_index_.find(elem.first);
    const std::string &index = it->second;
    Status status = store_client_->AsyncPut(table_name_, elem.first, index, elem.second,
                                            put_calllback);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone();

  // AsyncGetAll
  auto get_all_callback =
      [this](Status status, bool has_more,
             const std::vector<std::pair<std::string, std::string>> &result) {
        RAY_CHECK_OK(status);
        static std::unordered_set<std::string> received_keys;
        for (const auto &item : result) {
          auto it = received_keys.find(item.first);
          RAY_CHECK(it == received_keys.end());
          received_keys.emplace(item.first);

          auto map_it = key_to_value_.find(item.first);
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
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
