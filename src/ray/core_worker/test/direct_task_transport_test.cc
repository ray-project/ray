#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/util/test_util.h"

namespace ray {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  ray::Status PushNormalTask(
      std::unique_ptr<rpc::PushTaskRequest> request,
      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    callbacks.push_back(callback);
    return Status::OK();
  }

  bool ReplyPushTask(Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    callback(status, rpc::PushTaskReply());
    callbacks.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
};

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  void CompletePendingTask(const TaskID &, const rpc::PushTaskReply &,
                           const rpc::Address *actor_addr) override {
    num_tasks_complete++;
  }

  void PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type,
                         Status *status) override {
    num_tasks_failed++;
  }

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
};

class MockRayletClient : public WorkerLeaseInterface {
 public:
  ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                           bool disconnect_worker) override {
    if (disconnect_worker) {
      num_workers_disconnected++;
    } else {
      num_workers_returned++;
    }
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
  int num_workers_disconnected = 0;
  std::list<rpc::ClientCallback<rpc::WorkerLeaseReply>> callbacks = {};
};

TEST(TestMemoryStore, TestPromoteToPlasma) {
  bool num_plasma_puts = 0;
  auto mem = std::make_shared<CoreWorkerMemoryStore>(
      [&](const RayObject &obj, const ObjectID &obj_id) { num_plasma_puts += 1; });
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  ASSERT_TRUE(mem->Put(*data, obj1).ok());

  // Test getting an already existing object.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj1) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);

  // Testing getting an object that doesn't exist yet causes promotion.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) == nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);
  ASSERT_TRUE(mem->Put(*data, obj2).ok());
  ASSERT_TRUE(num_plasma_puts == 1);

  // The next time you get it, it's already there so no need to promote.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 1);
}

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  LocalDependencyResolver resolver(store);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(LocalDependencyResolverTest, TestIgnorePlasmaDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
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
  auto store = std::make_shared<CoreWorkerMemoryStore>();
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
  auto store = std::make_shared<CoreWorkerMemoryStore>();
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
  auto store = std::make_shared<CoreWorkerMemoryStore>();
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

TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const std::vector<std::string> &function_descriptor) {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  builder.SetCommonTaskSpec(TaskID::Nil(), Language::PYTHON, function_descriptor,
                            JobID::Nil(), TaskID::Nil(), 0, TaskID::Nil(), empty_address,
                            1, true, resources, resources);
  return builder.Build();
}

TEST(DirectTaskTransportTest, TestSubmitOneTask) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);

  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);

  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
}

TEST(DirectTaskTransportTest, TestHandleTaskFailure) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, ClientID::Nil()));
  // Simulate a system failure, i.e., worker died unexpectedly.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("oops")));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);
}

TEST(DirectTaskTransportTest, TestConcurrentWorkerLeases) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

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
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 3);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
}

TEST(DirectTaskTransportTest, TestReuseWorkerLease) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);

  // Task 2 finishes, Task 3 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);

  // Task 3 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // The second lease request is returned immediately.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
}

TEST(DirectTaskTransportTest, TestWorkerNotReusedOnError) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);
}

TEST(DirectTaskTransportTest, TestSpillback) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };

  std::unordered_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
  auto lease_client_factory = [&](const std::string &ip, int port) {
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_lease_clients.count(port) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_lease_clients[port] = client;
    return client;
  };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, lease_client_factory,
                                          store, task_finisher, ClientID::Nil(),
                                          kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = ClientID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 7777, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(7777), 1);
  // There should be no more callbacks on the local client.
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, ClientID::Nil()));
  // Trigger retry at the remote node.
  ASSERT_TRUE(
      remote_lease_clients[7777]->GrantWorkerLease("remote", 1234, ClientID::Nil()));

  // The worker is returned to the remote node, not the local one.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
}

TEST(DirectTaskTransportTest, TestSpillbackRoundTrip) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };

  std::unordered_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
  auto lease_client_factory = [&](const std::string &ip, int port) {
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_lease_clients.count(port) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_lease_clients[port] = client;
    return client;
  };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto local_raylet_id = ClientID::FromRandom();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, lease_client_factory,
                                          store, task_finisher, local_raylet_id,
                                          kLongTimeout);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = ClientID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 7777, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(7777), 1);
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, ClientID::Nil()));
  // Trigger a spillback back to the local node.
  ASSERT_TRUE(
      remote_lease_clients[7777]->GrantWorkerLease("local", 1234, local_raylet_id));
  // We should not have created another lease client to the local raylet.
  ASSERT_EQ(remote_lease_clients.size(), 1);
  // There should be no more callbacks on the remote node.
  ASSERT_FALSE(
      remote_lease_clients[7777]->GrantWorkerLease("remote", 1234, ClientID::Nil()));

  // The worker is returned to the local node.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("local", 1234, ClientID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
}

// Helper to run a test that checks that 'same1' and 'same2' are treated as the same
// resource shape, while 'different' is treated as a separate shape.
void TestSchedulingKey(const std::shared_ptr<CoreWorkerMemoryStore> store,
                       const TaskSpecification &same1, const TaskSpecification &same2,
                       const TaskSpecification &different) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(), kLongTimeout);

  ASSERT_TRUE(submitter.SubmitTask(same1).ok());
  ASSERT_TRUE(submitter.SubmitTask(same2).ok());
  ASSERT_TRUE(submitter.SubmitTask(different).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // same1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  // Another worker is requested because same2 is pending.
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // same1 runs successfully. Worker isn't returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  // taske1_2 is pushed.
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  // different is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // same2 runs successfully. Worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);

  // different runs successfully. Worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
}

TEST(DirectTaskTransportTest, TestSchedulingKeys) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();

  std::unordered_map<std::string, double> resources1({{"a", 1.0}});
  std::unordered_map<std::string, double> resources2({{"b", 2.0}});
  std::vector<std::string> descriptor1({"a"});
  std::vector<std::string> descriptor2({"b"});

  // Tasks with different resources should request different worker leases.
  RAY_LOG(INFO) << "Test different resources";
  TestSchedulingKey(store, BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources2, descriptor1));

  // Tasks with different function descriptors should request different worker leases.
  RAY_LOG(INFO) << "Test different descriptors";
  TestSchedulingKey(store, BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor2));

  ObjectID direct1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID direct2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID plasma1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID plasma2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  // Ensure the data is already present in the local store for direct call objects.
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store->Put(*data, direct1).ok());
  ASSERT_TRUE(store->Put(*data, direct2).ok());

  // Force plasma objects to be promoted.
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto plasma_data = RayObject(nullptr, meta_buffer);
  ASSERT_TRUE(store->Put(plasma_data, plasma1).ok());
  ASSERT_TRUE(store->Put(plasma_data, plasma2).ok());

  TaskSpecification same_deps_1 = BuildTaskSpec(resources1, descriptor1);
  same_deps_1.GetMutableMessage().add_args()->add_object_ids(direct1.Binary());
  same_deps_1.GetMutableMessage().add_args()->add_object_ids(plasma1.Binary());
  TaskSpecification same_deps_2 = BuildTaskSpec(resources1, descriptor1);
  same_deps_2.GetMutableMessage().add_args()->add_object_ids(direct1.Binary());
  same_deps_2.GetMutableMessage().add_args()->add_object_ids(direct2.Binary());
  same_deps_2.GetMutableMessage().add_args()->add_object_ids(plasma1.Binary());

  TaskSpecification different_deps = BuildTaskSpec(resources1, descriptor1);
  different_deps.GetMutableMessage().add_args()->add_object_ids(direct1.Binary());
  different_deps.GetMutableMessage().add_args()->add_object_ids(direct2.Binary());
  different_deps.GetMutableMessage().add_args()->add_object_ids(plasma2.Binary());

  // Tasks with different plasma dependencies should request different worker leases,
  // but direct call dependencies shouldn't be considered.
  RAY_LOG(INFO) << "Test different dependencies";
  TestSchedulingKey(store, same_deps_1, same_deps_2, different_deps);
}

TEST(DirectTaskTransportTest, TestWorkerLeaseTimeout) {
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto factory = [&](const std::string &addr, int port) { return worker_client; };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  CoreWorkerDirectTaskSubmitter submitter(raylet_client, factory, nullptr, store,
                                          task_finisher, ClientID::Nil(),
                                          /*lease_timeout_ms=*/5);
  std::unordered_map<std::string, double> empty_resources;
  std::vector<std::string> empty_descriptor;
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, ClientID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned due to the error even though
  // it hasn't timed out.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker; the worker is returned due to the
  // timeout.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, ClientID::Nil()));
  usleep(10 * 1000);  // Sleep for 10ms, causing the lease to time out.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 3 runs successfully on the third worker; the worker is returned even though it
  // hasn't timed out.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, ClientID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
