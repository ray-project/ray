#include <thread>
#include "gtest/gtest.h"

#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

class MockWaiter : public DependencyWaiter {
 public:
  MockWaiter() {}

  void Wait(const std::vector<ObjectID> &dependencies,
            std::function<void()> on_dependencies_available) override {
    callbacks_.push_back([on_dependencies_available]() { on_dependencies_available(); });
  }

  void Complete(int index) { callbacks_[index](); }

 private:
  std::vector<std::function<void()>> callbacks_;
};

TEST(SchedulingQueueTest, TestInOrder) {
  boost::asio::io_service io_service;
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
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

TEST(SchedulingQueueTest, TestWaitForObjects) {
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  boost::asio::io_service io_service;
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej);
  queue.Add(1, -1, fn_ok, fn_rej, {obj1});
  queue.Add(2, -1, fn_ok, fn_rej, {obj2});
  queue.Add(3, -1, fn_ok, fn_rej, {obj3});
  ASSERT_EQ(n_ok, 1);

  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(2);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(1);
  ASSERT_EQ(n_ok, 4);
}

TEST(SchedulingQueueTest, TestWaitForObjectsNotSubjectToSeqTimeout) {
  ObjectID obj1 = ObjectID::FromRandom();
  boost::asio::io_service io_service;
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok]() { n_ok++; };
  auto fn_rej = [&n_rej]() { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej);
  queue.Add(1, -1, fn_ok, fn_rej, {obj1});
  ASSERT_EQ(n_ok, 1);
  io_service.run();
  ASSERT_EQ(n_rej, 0);
  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);
}

TEST(SchedulingQueueTest, TestOutOfOrder) {
  boost::asio::io_service io_service;
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
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

TEST(SchedulingQueueTest, TestSeqWaitTimeout) {
  boost::asio::io_service io_service;
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
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
  MockWaiter waiter;
  SchedulingQueue queue(io_service, waiter, nullptr, 0);
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
