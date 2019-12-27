#include <iostream>

#include "gtest/gtest.h"
#include "ray/gcs/asio.h"
#include "ray/util/logging.h"
#include "ray/util/test_util.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

boost::asio::io_service io_service;

void ConnectCallback(const redisAsyncContext *c, int status) {
  ASSERT_EQ(status, REDIS_OK);
}

void DisconnectCallback(const redisAsyncContext *c, int status) {
  ASSERT_EQ(status, REDIS_OK);
}

void GetCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  ASSERT_TRUE(reply != nullptr);
  ASSERT_TRUE(std::string(reinterpret_cast<char *>(reply->str)) == "test");
  io_service.stop();
}

class RedisAsioTest : public RedisServiceManagerForTest {};

TEST_F(RedisAsioTest, TestRedisCommands) {
  redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", REDIS_SERVER_PORT);
  ASSERT_TRUE(ac->err == 0);
  ray::gcs::RedisAsyncContext redis_async_context(ac);

  RedisAsioClient client(io_service, redis_async_context);

  redisAsyncSetConnectCallback(ac, ConnectCallback);
  redisAsyncSetDisconnectCallback(ac, DisconnectCallback);

  redisAsyncCommand(ac, NULL, NULL, "SET key test");
  redisAsyncCommand(ac, GetCallback, nullptr, "GET key");

  io_service.run();
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
