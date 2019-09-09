#include <thread>
#include "gtest/gtest.h"

#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

TEST(SchedulingQueueTest, TestInOrder) {
  boost::asio::io_service io_service;
  SchedulingQueue queue(io_service, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej);
  queue.Add(1, -1, fn_ok, fn_rej);
  queue.Add(2, -1, fn_ok, fn_rej);
  queue.Add(3, -1, fn_ok, fn_rej);
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);
}

TEST(SchedulingQueueTest, TestOutOfOrder) {
  boost::asio::io_service io_service;
  SchedulingQueue queue(io_service, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(2, -1, fn_ok, fn_rej);
  queue.Add(0, -1, fn_ok, fn_rej);
  queue.Add(3, -1, fn_ok, fn_rej);
  queue.Add(1, -1, fn_ok, fn_rej);
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);
}

TEST(SchedulingQueueTest, TestDepWaitTimeout) {
  boost::asio::io_service io_service;
  SchedulingQueue queue(io_service, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(2, -1, fn_ok, fn_rej);
  queue.Add(0, -1, fn_ok, fn_rej);
  queue.Add(3, -1, fn_ok, fn_rej);
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 0);
  io_service.run();  // immediately triggers timeout
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);
  queue.Add(4, -1, fn_ok, fn_rej);
  queue.Add(5, -1, fn_ok, fn_rej);
  ASSERT_EQ(n_ok, 3);
  ASSERT_EQ(n_rej, 2);
}

TEST(SchedulingQueueTest, TestSkipAlreadyProcessedByClient) {
  boost::asio::io_service io_service;
  SchedulingQueue queue(io_service, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(2, 2, fn_ok, fn_rej);
  queue.Add(3, 2, fn_ok, fn_rej);
  queue.Add(1, 2, fn_ok, fn_rej);
  io_service.run();
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
