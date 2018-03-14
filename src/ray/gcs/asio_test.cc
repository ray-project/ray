#include <iostream>

#include "asio.h"
#include "gtest/gtest.h"
#include "ray/util/logging.h"

boost::asio::io_service io_service;

// For this test to work, you need to have a redis-server in your PATH

class TestRedisAsioClient : public ::testing::Test {
 public:
  TestRedisAsioClient() {
    int r = system("redis-server > /dev/null & sleep 1");
    RAY_LOG(INFO) << "TestRedisAsioClient: redis-server status code was " << r;
  }
  ~TestRedisAsioClient() {
    int r = system("redis-cli -c shutdown");
    RAY_LOG(INFO) << "TestRedisAsioClient: redis-cli status code was " << r;
  }
};

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
  redisAsyncDisconnect(c);
  io_service.stop();
}

TEST_F(TestRedisAsioClient, TestRedisCommands) {
  redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", 6379);
  ASSERT_TRUE(ac->err == 0);

  RedisAsioClient client(io_service, ac);

  redisAsyncSetConnectCallback(ac, ConnectCallback);
  redisAsyncSetDisconnectCallback(ac, DisconnectCallback);

  redisAsyncCommand(ac, NULL, NULL, "SET key test");
  redisAsyncCommand(ac, GetCallback, nullptr, "GET key");

  io_service.run();
}
