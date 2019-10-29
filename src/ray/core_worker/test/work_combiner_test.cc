#include <thread>

#include <boost/asio.hpp>

#include "gtest/gtest.h"
#include "ray/util/work_combiner.h"
#include "ray/util/util.h"

namespace ray {

// event_combiner_test.cc:25] Posting 1e6 events directly took 1716 ms.
// event_combiner_test.cc:27] Executing 1e6 events directly took 1716 ms.
// event_combiner_test.cc:34] Posting 1e6 events with combiner1 took 49 ms.
// event_combiner_test.cc:36] Executing 1e6 events with combiner1 took 1855 ms.
// event_combiner_test.cc:43] Posting 1e6 events with combiner64 took 60 ms.
// event_combiner_test.cc:45] Executing 1e6 events with combiner64 took 64 ms.
TEST(WorkCombinerTest, TestThroughput) {
  std::atomic<int> count;
  boost::asio::thread_pool pool(4);
  WorkCombiner combiner1(pool, 1);
  WorkCombiner combiner64(pool, 64);

  auto start = current_time_ms();
  for (int i = 0; i < 1000000; i++) {
    boost::asio::post(pool, [&count]() { count++; });
  }
  RAY_LOG(INFO) << "Posting 1e6 events directly took " << (current_time_ms() - start)
                << " ms.";
  while (count < 1000000) {
  }
  RAY_LOG(INFO) << "Executing 1e6 events directly took " << (current_time_ms() - start)
                << " ms.";

  count = 0;
  start = current_time_ms();
  for (int i = 0; i < 1000000; i++) {
    combiner1.post([&count]() { count++; });
  }
  RAY_LOG(INFO) << "Posting 1e6 events with combiner1 took "
                << (current_time_ms() - start) << " ms.";
  while (count < 1000000) {
  }
  RAY_LOG(INFO) << "Executing 1e6 events with combiner1 took "
                << (current_time_ms() - start) << " ms.";

  count = 0;
  start = current_time_ms();
  for (int i = 0; i < 1000000; i++) {
    combiner64.post([&count]() { count++; });
  }
  RAY_LOG(INFO) << "Posting 1e6 events with combiner64 took "
                << (current_time_ms() - start) << " ms.";
  while (count < 1000000) {
  }
  RAY_LOG(INFO) << "Executing 1e6 events with combiner64 took "
                << (current_time_ms() - start) << " ms.";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
