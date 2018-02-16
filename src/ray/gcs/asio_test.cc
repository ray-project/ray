#include <iostream>

#include "gtest/gtest.h"

#include "asio.h"

boost::asio::io_service io_service;

class TestRedisAsioClient : public ::testing::Test {
 public:
  TestRedisAsioClient() {
    int r = system("redis-server > /dev/null &");
    std::cout << "TestRedisAsioClient: redis-server status code was" << r
              << std::endl;
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
  char c[] = "end-1";
  redisAsyncCommand(ac, GetCallback, &c[0], "GET key");

  io_service.run();
}
