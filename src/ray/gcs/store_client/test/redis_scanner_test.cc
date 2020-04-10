#include "ray/gcs/store_client/redis_scanner.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisScannerTest : public RedisStoreClientTest {
 public:
  typedef std::pair<std::string, std::string> Row;

  RedisScannerTest() {}

  void Init() override {
    RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    // TODO(micafan) Write data to Redis.

    std::string match_pattern = table_name_ + "*";
    scanner_ = std::shared_ptr<RedisScanner>(redis_client_, match_pattern);
  }

  void OnScanRowCallback(Status status, bool has_more, const std::vector<Row> &result) {
    RAY_CHECK_OK(status);
    for (const &row : result) {
      ActorID actor_id = ActorID::FromBinary(row.first);
      rpc::ActorTableData actor_data;
      ASSERT_TRUE(actor_data.ParseFromString(row.second));
      ASSERT_EQ(row.first, actor_data.actor_id());

      auto it = key_to_value_.find(actor_id);
      ASSERT_TRUE(it != key_to_value_.end());
      received_rows_.emplace(row);
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
      ActorID actor_id = ActorID::FromBinary(key);
      auto it = key_to_value_.find(actor_id);
      ASSERT_TRUE(it != key_to_value_.end());
      received_keys_.emplace(key);
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
  ++pending_count_;
  DoScanPartialRows();

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanRowsTest) {
  auto scan_callback = [this](Status status, const std::vector<Row> &result) {
    OnScanRowCallback(status, false, result);
  };

  ++pending_count_;
  Status status = scanner_->ScanRows(scan_callback);
  RAY_CHECK_OK(status);

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanPartialKeysTest) {
  ++pending_count_;
  DoScanPartialKeys();

  WaitPendingDone();
}

TEST_F(RedisScannerTest, ScanKeysTest) {
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
