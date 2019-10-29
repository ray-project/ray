#include <thread>

#include <boost/asio.hpp>

#include "gtest/gtest.h"

#include "ray/util/util.h"

namespace ray {

// Posting 1e6 events directly took 1720 ms.
// Executing 1e6 events directly took 1720 ms.
// Posting 1e6 events with combiner took 71 ms.
// Executing 1e6 events with combiner took 71 ms.
TEST(EventCombinerTest, TestThroughput) {
  std::atomic<int> count;
  boost::asio::thread_pool pool(4);
  EventCombiner combiner(pool);

  auto start = current_time_ms();
  for (int i=0; i < 1000000; i++) {
    boost::asio::post(pool, [&count]() { count++; });
  }
  RAY_LOG(INFO) << "Posting 1e6 events directly took " << (current_time_ms() - start) << " ms.";
  while (count < 1000000) {}
  RAY_LOG(INFO) << "Executing 1e6 events directly took " << (current_time_ms() - start) << " ms.";

  count = 0;
  start = current_time_ms();
  for (int i=0; i < 1000000; i++) {
    combiner.post([&count]() { count++; });
  }
  RAY_LOG(INFO) << "Posting 1e6 events with combiner took " << (current_time_ms() - start) << " ms.";
  while (count < 1000000) {}
  RAY_LOG(INFO) << "Executing 1e6 events with combiner took " << (current_time_ms() - start) << " ms.";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
