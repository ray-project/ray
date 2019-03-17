#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/metrics/stats.h"

#include <chrono>
#include <thread>

namespace ray {


class StatsTest : public ::testing::Test {

public:
  void SetUp() {
    stats::Init("127.0.0.1:8888");
  }

  void Shutdown() {

  }

};

TEST_F(StatsTest, F) {

  for (size_t i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    stats::RedisLatency().Record(i % 10, {});

    stats::TaskElapse().Record(i * 10,
        {{stats::NodeAddressKey, "localhost"}});

    stats::TaskCount().Record(i);

    stats::WorkerCount().Record(i*1000);
  }

}

} // namespace ray


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
