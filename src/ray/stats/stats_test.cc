#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <iostream>
#include <vector>
#include <chrono>
#include <thread>

#include "ray/stats/stats.h"

namespace ray {

class StatsTest : public ::testing::Test {

public:
  void SetUp() {
    ray::stats::Init("127.0.0.1:8888");
  }

  void Shutdown() {

  }

};

TEST_F(StatsTest, F) {

  for (size_t i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    stats::RedisLatency().Record(i % 10,
        {{stats::CustomKey, "AAAAA"}});

    stats::TaskElapse().Record(i * 10,
        {{stats::NodeAddressKey, "localhost"}, {stats::CustomKey, "BBBBB"}});

    stats::TaskCount().Record(1);

    stats::WorkerCount().Record(i * 1000, {{stats::CustomKey, "DDDDD"}});
  }

}

} // namespace ray


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
