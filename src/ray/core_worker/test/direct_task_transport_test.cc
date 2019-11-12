#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/util/test_util.h"

namespace ray {

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  ray::Status PushNormalTask(
      std::unique_ptr<rpc::PushTaskRequest> request,
      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    callbacks.push_back(callback);
    return Status::OK();
  }

  std::vector<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
};

class MockRayletClient : public WorkerLeaseInterface {
 public:
  ray::Status ReturnWorker(int worker_port) {
    num_workers_returned += 1;
    return Status::OK();
  }

  ray::Status RequestWorkerLease(const ray::TaskSpecification &resource_spec) {
    num_workers_requested += 1;
    return Status::OK();
  }

  int num_workers_requested = 0;
  int num_workers_returned = 0;
};

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(LocalDependencyResolverTest, TestIgnorePlasmaDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::RAYLET);
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  // We ignore and don't block on plasma dependencies.
  ASSERT_TRUE(ok);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestInlineLocalDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  // Ensure the data is already present in the local store.
  ASSERT_TRUE(store.Put(*data, obj1).ok());
  ASSERT_TRUE(store.Put(*data, obj2).ok());
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  task.GetMutableMessage().add_args()->add_object_ids(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  // Tests that the task proto was rewritten to have inline argument values.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestInlinePendingDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  task.GetMutableMessage().add_args()->add_object_ids(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_EQ(resolver.NumPendingTasks(), 1);
  ASSERT_TRUE(!ok);
  ASSERT_TRUE(store.Put(*data, obj1).ok());
  ASSERT_TRUE(store.Put(*data, obj2).ok());
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(DirectTaskTranportTest, TestSubmitOneTask) {
  MockRayletClient raylet_client;
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto store = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, store);
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client.num_workers_requested, 1);
  ASSERT_EQ(raylet_client.num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1234));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  worker_client->callbacks[0](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client.num_workers_returned, 1);
}

TEST(DirectTaskTranportTest, TestHandleTaskFailure) {
  MockRayletClient raylet_client;
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto store = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, store);
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1234));
  // Simulate a system failure, i.e., worker died unexpectedly.
  worker_client->callbacks[0](Status::IOError("oops"), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client.num_workers_returned, 1);
}

TEST(DirectTaskTranportTest, TestConcurrentWorkerLeases) {
  MockRayletClient raylet_client;
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto store = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, store);
  TaskSpecification task1;
  TaskSpecification task2;
  TaskSpecification task3;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task3.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client.num_workers_requested, 1);

  // Task 1 is pushed; worker 2 is requested.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1000));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client.num_workers_requested, 2);

  // Task 2 is pushed; worker 3 is requested.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1001));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client.num_workers_requested, 3);

  // Task 3 is pushed; no more workers requested.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1002));
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(raylet_client.num_workers_requested, 3);

  // All workers returned.
  for (const auto &cb : worker_client->callbacks) {
    cb(Status::OK(), rpc::PushTaskReply());
  }
  ASSERT_EQ(raylet_client.num_workers_returned, 3);
}

TEST(DirectTaskTranportTest, TestReuseWorkerLease) {
  MockRayletClient raylet_client;
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto store = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, store);
  TaskSpecification task1;
  TaskSpecification task2;
  TaskSpecification task3;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task3.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client.num_workers_requested, 1);

  // Task 1 is pushed.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1000));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client.num_workers_requested, 2);

  // Task 1 finishes, Task 2 is scheduled on the same worker.
  worker_client->callbacks[0](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client.num_workers_returned, 0);

  // Task 2 finishes, Task 3 is scheduled on the same worker.
  worker_client->callbacks[1](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(raylet_client.num_workers_returned, 0);

  // Task 3 finishes, the worker is returned.
  worker_client->callbacks[2](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client.num_workers_returned, 1);

  // The second lease request is returned immediately.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1001));
  ASSERT_EQ(raylet_client.num_workers_returned, 2);
}

TEST(DirectTaskTranportTest, TestWorkerNotReusedOnError) {
  MockRayletClient raylet_client;
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto store = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, store);
  TaskSpecification task1;
  TaskSpecification task2;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_EQ(raylet_client.num_workers_requested, 1);

  // Task 1 is pushed.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1000));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client.num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned.
  worker_client->callbacks[0](Status::IOError("worker dead"), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client.num_workers_returned, 1);

  // Task 2 runs successfully on the second worker.
  submitter.HandleWorkerLeaseGranted(std::make_pair("localhost", 1001));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  worker_client->callbacks[1](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client.num_workers_returned, 2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
