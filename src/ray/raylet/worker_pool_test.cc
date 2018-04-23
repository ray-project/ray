#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/node_manager.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

class WorkerPoolMock : public WorkerPool {
 public:
  WorkerPoolMock(const std::vector<std::string> &worker_command)
      : WorkerPool(worker_command) {}

  void StartWorker(pid_t pid, bool force_start = false) {
    if (NumStartedWorkers() > 0 && !force_start) {
      // Workers have been started, but not registered. Force start disabled -- returning.
      RAY_LOG(DEBUG) << NumStartedWorkers() << " workers pending registration";
      return;
    }
    // Either no workers are pending registration or the worker start is being forced.
    RAY_LOG(DEBUG) << "starting worker, worker pool size " << Size();
    AddStartedWorker(pid);
  }
};

class WorkerPoolTest : public ::testing::Test {
 public:
  WorkerPoolTest() : worker_pool_({}), io_service_() {}

  std::shared_ptr<Worker> CreateWorker(pid_t pid) {
    std::function<void(std::shared_ptr<LocalClientConnection>)> client_handler = [this](
        std::shared_ptr<LocalClientConnection> client) { HandleNewClient(client); };
    std::function<void(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *)>
        message_handler = [this](std::shared_ptr<LocalClientConnection> client,
                                 int64_t message_type, const uint8_t *message) {
          HandleMessage(client, message_type, message);
        };
    boost::asio::local::stream_protocol::socket socket(io_service_);
    auto client =
        LocalClientConnection::Create(client_handler, message_handler, std::move(socket));
    worker_pool_.StartWorker(pid);
    return std::shared_ptr<Worker>(new Worker(pid, client));
  }

 protected:
  WorkerPoolMock worker_pool_;
  boost::asio::io_service io_service_;

 private:
  void HandleNewClient(std::shared_ptr<LocalClientConnection>){};
  void HandleMessage(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *){};
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
  popped_worker = worker_pool_.PopWorker(ActorID::nil());
  ASSERT_EQ(popped_worker, nullptr);

  // Create some workers.
  std::unordered_set<std::shared_ptr<Worker>> workers;
  workers.insert(CreateWorker(1234));
  workers.insert(CreateWorker(5678));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_.PushWorker(worker);
  }

  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_.PopWorker(ActorID::nil());
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(ActorID::nil());
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(ActorID::nil());
  ASSERT_EQ(popped_worker, nullptr);
}

TEST_F(WorkerPoolTest, PopActorWorker) {
  // Create a worker.
  auto worker = CreateWorker(1234);
  // Add the worker to the pool.
  worker_pool_.PushWorker(worker);

  // Assign an actor ID to the worker.
  auto actor = worker_pool_.PopWorker(ActorID::nil());
  auto actor_id = ActorID::from_random();
  actor->AssignActorId(actor_id);
  worker_pool_.PushWorker(actor);

  // Check that there are no more non-actor workers.
  ASSERT_EQ(worker_pool_.PopWorker(ActorID::nil()), nullptr);
  // Check that we can pop the actor worker.
  actor = worker_pool_.PopWorker(actor_id);
  ASSERT_EQ(actor, worker);
  ASSERT_EQ(actor->GetActorId(), actor_id);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
