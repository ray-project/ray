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
  ray::Status ReturnWorker(int worker_port) override {
    num_workers_returned += 1;
    return Status::OK();
  }

  ray::Status RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const rpc::ClientCallback<rpc::WorkerLeaseReply> &callback) override {
    num_workers_requested += 1;
    callbacks.push_back(callback);
    return Status::OK();
  }

  // Trigger reply to RequestWorkerLease.
  bool GrantWorkerLease(const std::string &address, int port,
                        const ClientID &retry_at_raylet_id) {
    rpc::WorkerLeaseReply reply;
    if (!retry_at_raylet_id.IsNil()) {
      reply.mutable_retry_at_raylet_address()->set_ip_address(address);
      reply.mutable_retry_at_raylet_address()->set_port(port);
      reply.mutable_retry_at_raylet_address()->set_raylet_id(retry_at_raylet_id.Binary());
    } else {
      reply.mutable_worker_address()->set_ip_address(address);
      reply.mutable_worker_address()->set_port(port);
      reply.mutable_worker_address()->set_raylet_id(retry_at_raylet_id.Binary());
    }
    if (callbacks.size() == 0) {
      return false;
    } else {
      auto callback = callbacks.front();
      callback(Status::OK(), reply);
      callbacks.pop_front();
      return true;
    }
  }

  ~MockRayletClient() {}

  int num_workers_requested = 0;
  int num_workers_returned = 0;
  std::list<rpc::ClientCallback<rpc::WorkerLeaseReply>> callbacks = {};
};

TEST(TestMemoryStore, TestPromoteToPlasma) {
  bool num_plasma_puts = 0;
  auto mem = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore(
      [&](const RayObject &obj, const ObjectID &obj_id) { num_plasma_puts += 1; }));
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  ASSERT_TRUE(mem->Put(obj1, *data).ok());

  // Test getting an already existing object.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj1) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);

  // Testing getting an object that doesn't exist yet causes promotion.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) == nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);
  ASSERT_TRUE(mem->Put(obj2, *data).ok());
  ASSERT_TRUE(num_plasma_puts == 1);

  // The next time you get it, it's already there so no need to promote.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 1);
}

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  LocalDependencyResolver resolver(store);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(LocalDependencyResolverTest, TestIgnorePlasmaDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
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

TEST(LocalDependencyResolverTest, TestHandlePlasmaPromotion) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer);
  ASSERT_TRUE(store->Put(data, obj1).ok());
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  ASSERT_TRUE(task.ArgId(0, 0).IsDirectCallType());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_TRUE(task.ArgByRef(0));
  // Checks that the object id was promoted to a plasma type id.
  ASSERT_FALSE(task.ArgId(0, 0).IsDirectCallType());
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestInlineLocalDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  // Ensure the data is already present in the local store.
  ASSERT_TRUE(store->Put(*data, obj1).ok());
  ASSERT_TRUE(store->Put(*data, obj2).ok());
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
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
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
  ASSERT_TRUE(store->Put(*data, obj1).ok());
  ASSERT_TRUE(store->Put(*data, obj2).ok());
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(DirectTaskTransportTest, TestSubmitOneTask) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store);
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  worker_client->callbacks[0](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
}

TEST(DirectTaskTransportTest, TestHandleTaskFailure) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store);
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, ClientID::Nil()));
  // Simulate a system failure, i.e., worker died unexpectedly.
  worker_client->callbacks[0](Status::IOError("oops"), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
}

TEST(DirectTaskTransportTest, TestConcurrentWorkerLeases) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store);
  TaskSpecification task1;
  TaskSpecification task2;
  TaskSpecification task3;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task3.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed; worker 2 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 2 is pushed; worker 3 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // Task 3 is pushed; no more workers requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // All workers returned.
  for (const auto &cb : worker_client->callbacks) {
    cb(Status::OK(), rpc::PushTaskReply());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 3);
}

TEST(DirectTaskTransportTest, TestReuseWorkerLease) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store);
  TaskSpecification task1;
  TaskSpecification task2;
  TaskSpecification task3;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task3.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes, Task 2 is scheduled on the same worker.
  worker_client->callbacks[0](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);

  // Task 2 finishes, Task 3 is scheduled on the same worker.
  worker_client->callbacks[1](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);

  // Task 3 finishes, the worker is returned.
  worker_client->callbacks[2](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // The second lease request is returned immediately.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
}

TEST(DirectTaskTransportTest, TestWorkerNotReusedOnError) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store);
  TaskSpecification task1;
  TaskSpecification task2;
  task1.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task2.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned.
  worker_client->callbacks[0](Status::IOError("worker dead"), rpc::PushTaskReply());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // Task 2 runs successfully on the second worker.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  worker_client->callbacks[1](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
}

TEST(DirectTaskTransportTest, TestSpillback) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::shared_ptr<MockWorkerClient>(new MockWorkerClient());
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  auto store = std::make_shared<CoreWorkerMemoryStoreProvider>(ptr);
  auto factory = [&](WorkerAddress addr) { return worker_client; };

  std::unordered_map<ClientID, std::shared_ptr<MockRayletClient>> remote_lease_clients;
  auto lease_client_factory = [&](const rpc::Address &addr) {
    ClientID raylet_id = ClientID::FromBinary(addr.raylet_id());
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_lease_clients.count(raylet_id) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_lease_clients[raylet_id] = client;
    return client;
  };
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, lease_client_factory,
                                          store);
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = ClientID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(remote_raylet_id), 1);
  // There should be no more callbacks on the local client.
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, ClientID::Nil()));
  // Trigger retry at the remote node.
  ASSERT_TRUE(remote_lease_clients[remote_raylet_id]->GrantWorkerLease("remote", 1234,
                                                                       ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  // The worker is returned to the remote node, not the local one.
  worker_client->callbacks[0](Status::OK(), rpc::PushTaskReply());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(remote_lease_clients[remote_raylet_id]->num_workers_returned, 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
