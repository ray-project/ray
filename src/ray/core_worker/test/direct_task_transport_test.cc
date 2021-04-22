// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/transport/direct_task_transport.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;
TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const ray::FunctionDescriptor &function_descriptor);

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
                      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplyPushTask(Status status = Status::OK(), bool exit = false) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::PushTaskReply();
    if (exit) {
      reply.set_worker_exiting(true);
    }
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  void CancelTask(const rpc::CancelTaskRequest &request,
                  const rpc::ClientCallback<rpc::CancelTaskReply> &callback) override {
    kill_requests.push_front(request);
  }

  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  std::list<rpc::CancelTaskRequest> kill_requests;
};

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  void CompletePendingTask(const TaskID &, const rpc::PushTaskReply &,
                           const rpc::Address &actor_addr) override {
    num_tasks_complete++;
  }

  bool PendingTaskFailed(
      const TaskID &task_id, rpc::ErrorType error_type, Status *status,
      const std::shared_ptr<rpc::RayException> &creation_task_exception = nullptr,
      bool immediately_mark_object_fail = true) override {
    num_tasks_failed++;
    return true;
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {
    num_inlined_dependencies += inlined_dependency_ids.size();
    num_contained_ids += contained_ids.size();
  }

  void MarkPendingTaskFailed(const TaskID &task_id, const TaskSpecification &spec,
                             rpc::ErrorType error_type,
                             const std::shared_ptr<rpc::RayException>
                                 &creation_task_exception = nullptr) override {}

  bool MarkTaskCanceled(const TaskID &task_id) override { return true; }

  absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    std::unordered_map<std::string, double> empty_resources;
    ray::FunctionDescriptor empty_descriptor =
        ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
    TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);
    return task;
  }

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
  int num_inlined_dependencies = 0;
  int num_contained_ids = 0;
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

  void RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size) override {
    num_workers_requested += 1;
    callbacks.push_back(callback);
  }

  void ReleaseUnusedWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) override {}

  void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
    num_leases_canceled += 1;
    cancel_callbacks.push_back(callback);
  }

  // Trigger reply to RequestWorkerLease.
  bool GrantWorkerLease(const std::string &address, int port,
                        const NodeID &retry_at_raylet_id, bool cancel = false) {
    rpc::RequestWorkerLeaseReply reply;
    if (cancel) {
      reply.set_canceled(true);
    } else if (!retry_at_raylet_id.IsNil()) {
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

  bool ReplyCancelWorkerLease(bool success = true) {
    rpc::CancelWorkerLeaseReply reply;
    reply.set_success(success);
    if (cancel_callbacks.size() == 0) {
      return false;
    } else {
      auto callback = cancel_callbacks.front();
      callback(Status::OK(), reply);
      cancel_callbacks.pop_front();
      return true;
    }
  }

  ~MockRayletClient() {}

  int num_workers_requested = 0;
  int num_workers_returned = 0;
  int num_workers_disconnected = 0;
  int num_leases_canceled = 0;
  std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
  std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
};

class MockActorCreator : public ActorCreatorInterface {
 public:
  MockActorCreator() {}

  Status RegisterActor(const TaskSpecification &task_spec) override {
    return Status::OK();
  };

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const gcs::StatusCallback &callback) override {
    return Status::OK();
  }

  ~MockActorCreator() {}
};

class MockLeasePolicy : public LeasePolicyInterface {
 public:
  MockLeasePolicy(const NodeID &node_id = NodeID::Nil()) {
    fallback_rpc_address_ = rpc::Address();
    fallback_rpc_address_.set_raylet_id(node_id.Binary());
  }

  rpc::Address GetBestNodeForTask(const TaskSpecification &spec) {
    num_lease_policy_consults++;
    return fallback_rpc_address_;
  };

  ~MockLeasePolicy() {}

  rpc::Address fallback_rpc_address_;

  int num_lease_policy_consults = 0;
};

TEST(TestMemoryStore, TestPromoteToPlasma) {
  size_t num_plasma_puts = 0;
  auto mem = std::make_shared<CoreWorkerMemoryStore>(
      [&](const RayObject &obj, const ObjectID &obj_id) { num_plasma_puts += 1; });
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  ASSERT_TRUE(mem->Put(*data, obj1));

  // Test getting an already existing object.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj1) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);

  // Testing getting an object that doesn't exist yet causes promotion.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) == nullptr);
  ASSERT_TRUE(num_plasma_puts == 0);
  ASSERT_FALSE(mem->Put(*data, obj2));
  ASSERT_TRUE(num_plasma_puts == 1);

  // The next time you get it, it's already there so no need to promote.
  ASSERT_TRUE(mem->GetOrPromoteToPlasma(obj2) != nullptr);
  ASSERT_TRUE(num_plasma_puts == 1);
}

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  LocalDependencyResolver resolver(store, task_finisher);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestHandlePlasmaPromotion) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  LocalDependencyResolver resolver(store, task_finisher);
  ObjectID obj1 = ObjectID::FromRandom();
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<ObjectID>());
  ASSERT_TRUE(store->Put(data, obj1));
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_TRUE(task.ArgByRef(0));
  // Checks that the object id is still a direct call id.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestInlineLocalDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  LocalDependencyResolver resolver(store, task_finisher);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  // Ensure the data is already present in the local store.
  ASSERT_TRUE(store->Put(*data, obj1));
  ASSERT_TRUE(store->Put(*data, obj2));
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  // Tests that the task proto was rewritten to have inline argument values.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 2);
}

TEST(LocalDependencyResolverTest, TestInlinePendingDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  LocalDependencyResolver resolver(store, task_finisher);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_EQ(resolver.NumPendingTasks(), 1);
  ASSERT_TRUE(!ok);
  ASSERT_TRUE(store->Put(*data, obj1));
  ASSERT_TRUE(store->Put(*data, obj2));
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 2);
  ASSERT_EQ(task_finisher->num_contained_ids, 0);
}

TEST(LocalDependencyResolverTest, TestInlinedObjectIds) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  LocalDependencyResolver resolver(store, task_finisher);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  auto data = GenerateRandomObject({obj3});
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_EQ(resolver.NumPendingTasks(), 1);
  ASSERT_TRUE(!ok);
  ASSERT_TRUE(store->Put(*data, obj1));
  ASSERT_TRUE(store->Put(*data, obj2));
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 2);
  ASSERT_EQ(task_finisher->num_contained_ids, 2);
}

TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const ray::FunctionDescriptor &function_descriptor) {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  builder.SetCommonTaskSpec(TaskID::Nil(), "dummy_task", Language::PYTHON,
                            function_descriptor, JobID::Nil(), TaskID::Nil(), 0,
                            TaskID::Nil(), empty_address, 1, resources, resources,
                            std::make_pair(PlacementGroupID::Nil(), -1), true, "");
  return builder.Build();
}

TEST(DirectTaskTransportTest, TestSubmitOneTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);

  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);

  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestHandleTaskFailure) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));
  // Simulate a system failure, i.e., worker died unexpectedly.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("oops")));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestConcurrentWorkerLeases) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed; worker 2 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 2 is pushed; worker 3 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // Task 3 is pushed; no more workers requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 3);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestReuseWorkerLease) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 2 finishes, Task 3 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  // Task 3 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // The second lease request is returned immediately.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestRetryLeaseCancellation) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  // Task 2 finishes, Task 3 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  // Task 3 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // Simulate the lease cancellation request failing because it arrives at the
  // raylet before the last worker lease request has been received.
  int i = 1;
  for (; i <= 3; i++) {
    ASSERT_EQ(raylet_client->num_leases_canceled, i);
    ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease(false));
  }

  // Simulate the lease cancellation request succeeding.
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, i);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, i);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("", 0, NodeID::Nil(), /*cancel=*/true));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  // The canceled lease is not returned.
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestConcurrentCancellationAndSubmission) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());

  // Task 2's lease request gets canceled.
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Task 2 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // Another task is submitted while task 2's lease request is being canceled.
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 2's lease request is canceled, a new worker is requested for task 3.
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("", 0, NodeID::Nil(), /*cancel=*/true));
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // Task 3 finishes, all workers returned.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestWorkerNotReusedOnError) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestWorkerNotReturnedOnExit) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  // Task 1 finishes with exit status; the worker is not returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), /*exit=*/true));
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestSpillback) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });

  std::unordered_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
  auto lease_client_factory = [&](const std::string &ip, int port) {
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_lease_clients.count(port) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_lease_clients[port] = client;
    return client;
  };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(
      address, raylet_client, client_pool, lease_client_factory, lease_policy, store,
      task_finisher, NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = NodeID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 7777, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(7777), 1);
  // Confirm that lease policy is not consulted on spillback.
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  // There should be no more callbacks on the local client.
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, NodeID::Nil()));
  // Trigger retry at the remote node.
  ASSERT_TRUE(
      remote_lease_clients[7777]->GrantWorkerLease("remote", 1234, NodeID::Nil()));

  // The worker is returned to the remote node, not the local one.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  for (const auto &remote_client : remote_lease_clients) {
    ASSERT_EQ(remote_client.second->num_leases_canceled, 0);
    ASSERT_FALSE(remote_client.second->ReplyCancelWorkerLease());
  }

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestSpillbackRoundTrip) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });

  std::unordered_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
  auto lease_client_factory = [&](const std::string &ip, int port) {
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_lease_clients.count(port) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_lease_clients[port] = client;
    return client;
  };
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto local_raylet_id = NodeID::FromRandom();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>(local_raylet_id);
  CoreWorkerDirectTaskSubmitter submitter(
      address, raylet_client, client_pool, lease_client_factory, lease_policy, store,
      task_finisher, local_raylet_id, kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = NodeID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 7777, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(7777), 1);
  // Confirm that lease policy is not consulted on spillback.
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, NodeID::Nil()));
  // Trigger a spillback back to the local node.
  ASSERT_TRUE(
      remote_lease_clients[7777]->GrantWorkerLease("local", 1234, local_raylet_id));
  // We should not have created another lease client to the local raylet.
  ASSERT_EQ(remote_lease_clients.size(), 1);
  // There should be no more callbacks on the remote node.
  ASSERT_FALSE(
      remote_lease_clients[7777]->GrantWorkerLease("remote", 1234, NodeID::Nil()));

  // The worker is returned to the local node.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("local", 1234, NodeID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  for (const auto &remote_client : remote_lease_clients) {
    ASSERT_EQ(remote_client.second->num_leases_canceled, 0);
    ASSERT_FALSE(remote_client.second->ReplyCancelWorkerLease());
  }

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

// Helper to run a test that checks that 'same1' and 'same2' are treated as the same
// resource shape, while 'different' is treated as a separate shape.
void TestSchedulingKey(const std::shared_ptr<CoreWorkerMemoryStore> store,
                       const TaskSpecification &same1, const TaskSpecification &same2,
                       const TaskSpecification &different) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);

  ASSERT_TRUE(submitter.SubmitTask(same1).ok());
  ASSERT_TRUE(submitter.SubmitTask(same2).ok());
  ASSERT_TRUE(submitter.SubmitTask(different).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // same1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  // Another worker is requested because same2 is pending.
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // same1 runs successfully. Worker isn't returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  // same2 is pushed.
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());

  // different is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
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

  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Trigger reply to RequestWorkerLease to remove the canceled pending lease request
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, NodeID::Nil(), true));
  ASSERT_EQ(raylet_client->num_workers_returned, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestSchedulingKeys) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();

  std::unordered_map<std::string, double> resources1({{"a", 1.0}});
  std::unordered_map<std::string, double> resources2({{"b", 2.0}});
  ray::FunctionDescriptor descriptor1 =
      ray::FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  ray::FunctionDescriptor descriptor2 =
      ray::FunctionDescriptorBuilder::BuildPython("b", "", "", "");

  // Tasks with different resources should request different worker leases.
  RAY_LOG(INFO) << "Test different resources";
  TestSchedulingKey(store, BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources2, descriptor1));

  // Tasks with different function descriptors do not request different worker leases.
  RAY_LOG(INFO) << "Test different descriptors";
  TestSchedulingKey(store, BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor2),
                    BuildTaskSpec(resources2, descriptor1));

  ObjectID direct1 = ObjectID::FromRandom();
  ObjectID direct2 = ObjectID::FromRandom();
  ObjectID plasma1 = ObjectID::FromRandom();
  ObjectID plasma2 = ObjectID::FromRandom();
  // Ensure the data is already present in the local store for direct call objects.
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store->Put(*data, direct1));
  ASSERT_TRUE(store->Put(*data, direct2));

  // Force plasma objects to be promoted.
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto plasma_data = RayObject(nullptr, meta_buffer, std::vector<ObjectID>());
  ASSERT_TRUE(store->Put(plasma_data, plasma1));
  ASSERT_TRUE(store->Put(plasma_data, plasma2));

  TaskSpecification same_deps_1 = BuildTaskSpec(resources1, descriptor1);
  same_deps_1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  same_deps_1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());
  TaskSpecification same_deps_2 = BuildTaskSpec(resources1, descriptor1);
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct2.Binary());
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());

  TaskSpecification different_deps = BuildTaskSpec(resources1, descriptor1);
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct2.Binary());
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma2.Binary());

  // Tasks with different plasma dependencies should request different worker leases,
  // but direct call dependencies shouldn't be considered.
  RAY_LOG(INFO) << "Test different dependencies";
  TestSchedulingKey(store, same_deps_1, same_deps_2, different_deps);
}

TEST(DirectTaskTransportTest, TestWorkerLeaseTimeout) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(),
                                          /*lease_timeout_ms=*/5, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task1 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task2 = BuildTaskSpec(empty_resources, empty_descriptor);
  TaskSpecification task3 = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned due to the error even though
  // it hasn't timed out.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker; the worker is returned due to the
  // timeout.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  std::this_thread::sleep_for(
      std::chrono::milliseconds(10));  // Sleep for 10ms, causing the lease to time out.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 3 runs successfully on the third worker; the worker is returned even though it
  // hasn't timed out.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, NodeID::Nil()));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestKillExecutingTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });

  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));

  // Try force kill, exiting the worker
  ASSERT_TRUE(submitter.CancelTask(task, true, false).ok());
  ASSERT_EQ(worker_client->kill_requests.front().intended_task_id(),
            task.TaskId().Binary());
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("workerdying"), true));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);

  task.GetMutableMessage().set_task_id(
      TaskID::ForNormalTask(JobID::Nil(), TaskID::Nil(), 1).Binary());
  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));

  // Try non-force kill, worker returns normally
  ASSERT_TRUE(submitter.CancelTask(task, false, false).ok());
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->kill_requests.front().intended_task_id(),
            task.TaskId().Binary());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestKillPendingTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(submitter.CancelTask(task, true, false).ok());
  ASSERT_EQ(worker_client->kill_requests.size(), 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());

  // Trigger reply to RequestWorkerLease to remove the canceled pending lease request
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil(), true));

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestKillResolvingTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address, raylet_client, client_pool, nullptr,
                                          lease_policy, store, task_finisher,
                                          NodeID::Nil(), kLongTimeout, actor_creator);
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  TaskSpecification task = BuildTaskSpec(empty_resources, empty_descriptor);
  ObjectID obj1 = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
  ASSERT_TRUE(submitter.CancelTask(task, true, false).ok());
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store->Put(*data, obj1));
  ASSERT_EQ(worker_client->kill_requests.size(), 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestPipeliningConcurrentWorkerLeases) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();

  // Set max_tasks_in_flight_per_worker to a value larger than 1 to enable the
  // pipelining of task submissions. This is done by passing a
  // max_tasks_in_flight_per_worker parameter to the CoreWorkerDirectTaskSubmitter.
  uint32_t max_tasks_in_flight_per_worker = 10;
  CoreWorkerDirectTaskSubmitter submitter(
      address, raylet_client, client_pool, nullptr, lease_policy, store, task_finisher,
      NodeID::Nil(), kLongTimeout, actor_creator, max_tasks_in_flight_per_worker);

  // Prepare 20 tasks and save them in a vector.
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  std::vector<TaskSpecification> tasks;
  for (int i = 1; i <= 20; i++) {
    tasks.push_back(BuildTaskSpec(empty_resources, empty_descriptor));
  }
  ASSERT_EQ(tasks.size(), 20);

  // Submit the 20 tasks and check that one worker is requested.
  for (auto task : tasks) {
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
  }
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // First 10 tasks are pushed; worker 2 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 10);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Last 10 tasks are pushed; no more workers are requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 20);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  for (int i = 1; i <= 20; i++) {
    ASSERT_FALSE(worker_client->callbacks.empty());
    ASSERT_TRUE(worker_client->ReplyPushTask());
    // No worker should be returned until all the tasks that were submitted to it have
    // been completed. In our case, the first worker should only be returned after the
    // 10th task has been executed. The second worker should only be returned at the
    // end, or after the 20th task has been executed.
    if (i < 10) {
      ASSERT_EQ(raylet_client->num_workers_returned, 0);
    } else if (i >= 10 && i < 20) {
      ASSERT_EQ(raylet_client->num_workers_returned, 1);
    } else if (i == 20) {
      ASSERT_EQ(raylet_client->num_workers_returned, 2);
    }
  }

  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 20);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestPipeliningReuseWorkerLease) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();

  // Set max_tasks_in_flight_per_worker to a value larger than 1 to enable the
  // pipelining of task submissions. This is done by passing a
  // max_tasks_in_flight_per_worker parameter to the CoreWorkerDirectTaskSubmitter.
  uint32_t max_tasks_in_flight_per_worker = 10;
  CoreWorkerDirectTaskSubmitter submitter(
      address, raylet_client, client_pool, nullptr, lease_policy, store, task_finisher,
      NodeID::Nil(), kLongTimeout, actor_creator, max_tasks_in_flight_per_worker);

  // prepare 30 tasks and save them in a vector
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  std::vector<TaskSpecification> tasks;
  for (int i = 0; i < 30; i++) {
    tasks.push_back(BuildTaskSpec(empty_resources, empty_descriptor));
  }
  ASSERT_EQ(tasks.size(), 30);

  // Submit the 30 tasks and check that one worker is requested
  for (auto task : tasks) {
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
  }
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1-10 are pushed, and a new worker is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 10);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  // The lease is not cancelled, as there is more work to do
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 1-10 finish, Tasks 11-20 are scheduled on the same worker.
  for (int i = 1; i <= 10; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(worker_client->callbacks.size(), 10);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 11-20 finish, Tasks 21-30 are scheduled on the same worker.
  for (int i = 11; i <= 20; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(worker_client->callbacks.size(), 10);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());

  // Tasks 21-30 finish, and the worker is finally returned.
  for (int i = 21; i <= 30; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // The second lease request is returned immediately.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 30);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestPipeliningNumberOfWorkersRequested) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();

  // Set max_tasks_in_flight_per_worker to a value larger than 1 to enable the
  // pipelining of task submissions. This is done by passing a
  // max_tasks_in_flight_per_worker parameter to the CoreWorkerDirectTaskSubmitter.
  uint32_t max_tasks_in_flight_per_worker = 10;
  CoreWorkerDirectTaskSubmitter submitter(
      address, raylet_client, client_pool, nullptr, lease_policy, store, task_finisher,
      NodeID::Nil(), kLongTimeout, actor_creator, max_tasks_in_flight_per_worker);

  // prepare 30 tasks and save them in a vector
  std::unordered_map<std::string, double> empty_resources;
  ray::FunctionDescriptor empty_descriptor =
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
  std::vector<TaskSpecification> tasks;
  for (int i = 0; i < 30; i++) {
    tasks.push_back(BuildTaskSpec(empty_resources, empty_descriptor));
  }
  ASSERT_EQ(tasks.size(), 30);

  // Submit 4 tasks, and check that 1 worker is requested.
  for (int i = 1; i <= 4; i++) {
    auto task = tasks.front();
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
    tasks.erase(tasks.begin());
  }
  ASSERT_EQ(tasks.size(), 26);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Grant a worker lease, and check that still only 1 worker was requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 4);

  // Submit 6 more tasks, and check that still only 1 worker was requested.
  for (int i = 1; i <= 6; i++) {
    auto task = tasks.front();
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
    tasks.erase(tasks.begin());
  }
  ASSERT_EQ(tasks.size(), 20);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 10);

  // Submit 1 more task, and check that one more worker is requested, for a total of 2.
  auto task = tasks.front();
  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  tasks.erase(tasks.begin());
  ASSERT_EQ(tasks.size(), 19);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 10);

  // Grant a worker lease, and check that still only 2 workers were requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 11);

  // Submit 9 more tasks, and check that the total number of workers requested is
  // still 2.
  for (int i = 1; i <= 9; i++) {
    auto task = tasks.front();
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
    tasks.erase(tasks.begin());
  }
  ASSERT_EQ(tasks.size(), 10);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 0);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 20);

  // Call ReplyPushTask on a quarter of the submitted tasks (5), and check that the
  // total number of workers requested remains equal to 2.
  for (int i = 1; i <= 5; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 5);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 15);

  // Submit 5 new tasks, and check that we still have requested only 2 workers.
  for (int i = 1; i <= 5; i++) {
    auto task = tasks.front();
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
    tasks.erase(tasks.begin());
  }
  ASSERT_EQ(tasks.size(), 5);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 5);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 20);

  // Call ReplyPushTask on a quarter of the submitted tasks (5), and check that the
  // total number of workers requested remains equal to 2.
  for (int i = 1; i <= 5; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 10);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 15);

  // Submit last 5 tasks, and check that the total number of workers requested is still
  // 2
  for (int i = 1; i <= 5; i++) {
    auto task = tasks.front();
    ASSERT_TRUE(submitter.SubmitTask(task).ok());
    tasks.erase(tasks.begin());
  }
  ASSERT_EQ(tasks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 10);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 20);

  // Execute all the resulting 20 tasks, and check that the total number of workers
  // requested is 2.
  for (int i = 1; i <= 20; i++) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 30);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
