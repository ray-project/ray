#include "gmock/gmock.h"
#include "gtest/gtest.h"

//#include "ray/metrics/metric.h"
#include "ray/metrics/perf_counter.h"

#include <chrono>
#include <thread>

namespace ray {


class PerfCounterTest : public ::testing::Test {

public:
  void SetUp() {
    perf_counter::Init("127.0.0.1:8888");
  }

  void Shutdown() {

  }

};

TEST_F(PerfCounterTest, F) {

  for (size_t i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    perf_counter::RedisLatency().Record(i % 40, {});

    perf_counter::TaskElapse().Record(i * 10,
        {{perf_counter::NodeAddressKey, "localhost"}});

    perf_counter::TaskCount().Record(i);

  }

}

} // namespace ray


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
