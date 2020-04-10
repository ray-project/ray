#include "ray/gcs/store_client/redis_multi_reader.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisMultiReaderTest : public RedisStoreClientTest {
 public:
  typedef std::pair<std::string, std::string> Row;

  RedisMultiReaderTest() {}

  void Init() {
    RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    // TODO(micafan) Write data to Redis.

    // TODO(micafan) Get keys.
    auto keys = GetKeys();
    multi_reader_ = std::make_shared<RedisMultiReader>(keys);
  }

  void OnReadCallback(Status status, const std::vector<Row> &result) {
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
    ASSERT_EQ(result.size(), key_to_value_.size());
  }

 protected:
  std::shared_ptr<RedisClient> redis_client_;
  std::shared_ptr<RedisMultiReader> multi_reader_;
};

TEST_F(RedisMultiReaderTest, ReadTest) {
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
