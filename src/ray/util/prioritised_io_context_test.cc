#include "ray/util/prioritised_io_context.h"
#include "gtest/gtest.h"

namespace ray {

class PrioritisedIoContextTest : public ::testing::Test {
 protected:
  bool IsOrdered(const std::vector<int> &queue) const {
    int current = -1;
    int next = -1;
    for (auto value : queue) {
      current = next;
      next = value;
      if (next <= current) {
        return false;
      }
    }
    return true;
  }
};

TEST_F(PrioritisedIoContextTest, TestFIFO) {
  ray::IOContext io_context;
  std::thread([&] { io_context.run(); }).detach();

  int count = 1000;
  std::vector<int> queue;
  for (int i = 0; i < count; ++i) {
    io_context.post([&queue, i] { queue.emplace_back(i); });
  }

  while (queue.size() != count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_TRUE(IsOrdered(queue));

  io_context.stop();
}

TEST_F(PrioritisedIoContextTest, TestPriority) {
  ray::IOContext io_context;
  std::thread([&] { io_context.run(); }).detach();

  int count = 1000;
  std::vector<int> queue;
  for (int i = 0; i < count; ++i) {
    io_context.post([&queue, i] { queue.emplace_back(i); }, Priority(i % 2));
  }

  while (queue.size() != count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_FALSE(IsOrdered(queue));

  io_context.stop();
}

TEST_F(PrioritisedIoContextTest, TestTimerFIFO) {
  ray::IOContext io_context;
  std::thread([&] { io_context.run(); }).detach();

  int count = 100;
  std::vector<int> queue;
  for (int i = 0; i < count; ++i) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_context.io_context());
    auto interval = boost::posix_time::milliseconds(100);
    timer->expires_from_now(interval);
    io_context.async_wait_timer(
        *timer, [timer, &queue, i](const boost::system::error_code & /*ec*/) {
          queue.emplace_back(i);
        });
    std::this_thread::sleep_for(std::chrono::nanoseconds(1));
  }

  while (queue.size() != count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_TRUE(IsOrdered(queue));

  io_context.stop();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
