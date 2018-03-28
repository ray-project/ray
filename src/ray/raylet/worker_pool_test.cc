#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/node_manager.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

class MockClientManager : public ClientManager<boost::asio::local::stream_protocol> {
 public:
  MOCK_METHOD3(ProcessClientMessage,
               void(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *));
  MOCK_METHOD1(ProcessNewClient, void(std::shared_ptr<LocalClientConnection>));
};

class WorkerPoolTest : public ::testing::Test {
 public:
  WorkerPoolTest() : worker_pool_(0), client_manager_(), io_service_() {}

  std::shared_ptr<Worker> CreateWorker(pid_t pid) {
    boost::asio::local::stream_protocol::socket socket(io_service_);
    auto client = LocalClientConnection::Create(client_manager_, std::move(socket));
    return std::shared_ptr<Worker>(new Worker(pid, client));
  }

 protected:
  WorkerPool worker_pool_;
  MockClientManager client_manager_;
  boost::asio::io_service io_service_;
};

TEST_F(WorkerPoolTest, HandleWorkerRegistration) {
  auto worker = CreateWorker(1234);
  // Check that we cannot lookup the worker before it's registered.
  ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
  worker_pool_.RegisterWorker(worker);
  // Check that we can lookup the worker after it's registered.
  ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), worker);
  worker_pool_.DisconnectWorker(worker);
  // Check that we cannot lookup the worker after it's disconnected.
  ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
}

TEST_F(WorkerPoolTest, HandleWorkerPushPop) {
  // Try to pop a worker from the empty pool and make sure we don't get one.
  std::shared_ptr<Worker> popped_worker;
  popped_worker = worker_pool_.PopWorker();
  ASSERT_EQ(popped_worker, nullptr);

  // Create some workers.
  std::unordered_set<std::shared_ptr<Worker>> workers;
  workers.insert(CreateWorker(1234));
  workers.insert(CreateWorker(5678));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_.PushWorker(worker);
  }
  ASSERT_EQ(worker_pool_.PoolSize(), workers.size());

  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_.PopWorker();
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker();
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
