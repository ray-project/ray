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
namespace core {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const FunctionDescriptor &function_descriptor,
                                int64_t depth = 0,
                                std::string serialized_runtime_env = "") {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  builder.SetCommonTaskSpec(TaskID::Nil(),
                            "dummy_task",
                            Language::PYTHON,
                            function_descriptor,
                            JobID::Nil(),
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            empty_address,
                            1,
                            resources,
                            resources,
                            serialized_runtime_env,
                            depth);
  return builder.Build();
}
// Calls BuildTaskSpec with empty resources map and empty function descriptor
TaskSpecification BuildEmptyTaskSpec();

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
                      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplyPushTask(Status status = Status::OK(),
                     bool exit = false,
                     bool is_application_level_error = false) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::PushTaskReply();
    if (exit) {
      reply.set_worker_exiting(true);
    }
    if (is_application_level_error) {
      reply.set_is_application_level_error(true);
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

  void CompletePendingTask(const TaskID &,
                           const rpc::PushTaskReply &,
                           const rpc::Address &actor_addr) override {
    num_tasks_complete++;
  }

  bool RetryTaskIfPossible(const TaskID &task_id) override {
    num_task_retries_attempted++;
    return false;
  }

  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status,
                       const rpc::RayErrorInfo *ray_error_info = nullptr,
                       bool mark_task_object_failed = true) override {
    num_fail_pending_task_calls++;
  }

  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true) override {
    num_tasks_failed++;
    return true;
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {
    num_inlined_dependencies += inlined_dependency_ids.size();
    num_contained_ids += contained_ids.size();
  }

  void MarkTaskReturnObjectsFailed(
      const TaskSpecification &spec,
      rpc::ErrorType error_type,
      const rpc::RayErrorInfo *ray_error_info = nullptr) override {}

  bool MarkTaskCanceled(const TaskID &task_id) override { return true; }

  absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    TaskSpecification task = BuildEmptyTaskSpec();
    return task;
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {}

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
  int num_inlined_dependencies = 0;
  int num_contained_ids = 0;
  int num_task_retries_attempted = 0;
  int num_fail_pending_task_calls = 0;
};

class MockRayletClient : public WorkerLeaseInterface {
 public:
  Status ReturnWorker(int worker_port,
                      const WorkerID &worker_id,
                      bool disconnect_worker,
                      bool worker_exiting) override {
    if (disconnect_worker) {
      num_workers_disconnected++;
    } else {
      num_workers_returned++;
      if (worker_exiting) {
        num_workers_returned_exiting++;
      }
    }
    return Status::OK();
  }

  void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override {
    reported_backlog_size = 0;
    reported_backlogs.clear();
    for (const auto &backlog_report : backlog_reports) {
      reported_backlog_size += backlog_report.backlog_size();
      const TaskSpecification resource_spec(backlog_report.resource_spec());
      const SchedulingClass scheduling_class = resource_spec.GetSchedulingClass();
      reported_backlogs[scheduling_class] = backlog_report.backlog_size();
    }
  }

  void RequestWorkerLease(
      const rpc::TaskSpec &task_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size,
      const bool is_selected_based_on_locality) override {
    num_workers_requested += 1;
    if (grant_or_reject) {
      num_grant_or_reject_leases_requested += 1;
    }
    if (is_selected_based_on_locality) {
      num_is_selected_based_on_locality_leases_requested += 1;
    }
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
  bool GrantWorkerLease(
      const std::string &address,
      int port,
      const NodeID &retry_at_raylet_id,
      bool cancel = false,
      std::string worker_id = std::string(),
      bool reject = false,
      const rpc::RequestWorkerLeaseReply::SchedulingFailureType &failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED) {
    rpc::RequestWorkerLeaseReply reply;
    if (cancel) {
      reply.set_canceled(true);
      reply.set_failure_type(failure_type);
    } else if (reject) {
      reply.set_rejected(true);
    } else if (!retry_at_raylet_id.IsNil()) {
      reply.mutable_retry_at_raylet_address()->set_ip_address(address);
      reply.mutable_retry_at_raylet_address()->set_port(port);
      reply.mutable_retry_at_raylet_address()->set_raylet_id(retry_at_raylet_id.Binary());
    } else {
      reply.mutable_worker_address()->set_ip_address(address);
      reply.mutable_worker_address()->set_port(port);
      reply.mutable_worker_address()->set_raylet_id(retry_at_raylet_id.Binary());
      // Set the worker ID if the worker_id string is a valid, non-empty argument. A
      // worker ID can only be set using a 28-characters string.
      if (worker_id.length() == 28) {
        reply.mutable_worker_address()->set_worker_id(worker_id);
      }
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

  bool FailWorkerLeaseDueToGrpcUnavailable() {
    rpc::RequestWorkerLeaseReply reply;
    if (callbacks.size() == 0) {
      return false;
    } else {
      auto callback = callbacks.front();
      callback(Status::GrpcUnavailable("unavailable"), reply);
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

  int num_grant_or_reject_leases_requested = 0;
  int num_is_selected_based_on_locality_leases_requested = 0;
  int num_workers_requested = 0;
  int num_workers_returned = 0;
  int num_workers_returned_exiting = 0;
  int num_workers_disconnected = 0;
  int num_leases_canceled = 0;
  int reported_backlog_size = 0;
  std::map<SchedulingClass, int64_t> reported_backlogs;
  std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
  std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
};

class MockActorCreator : public ActorCreatorInterface {
 public:
  MockActorCreator() {}

  Status RegisterActor(const TaskSpecification &task_spec) const override {
    return Status::OK();
  };

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            gcs::StatusCallback callback) override {
    return Status::OK();
  }

  Status AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override {
    return Status::OK();
  }

  void AsyncWaitForActorRegisterFinish(const ActorID &,
                                       gcs::StatusCallback callback) override {
    callbacks.push_back(callback);
  }

  [[nodiscard]] bool IsActorInRegistering(const ActorID &actor_id) const override {
    return actor_pending;
  }

  ~MockActorCreator() {}

  std::list<gcs::StatusCallback> callbacks;
  bool actor_pending = false;
};

class MockLeasePolicy : public LeasePolicyInterface {
 public:
  MockLeasePolicy(const NodeID &node_id = NodeID::Nil()) {
    fallback_rpc_address_ = rpc::Address();
    fallback_rpc_address_.set_raylet_id(node_id.Binary());
  }

  std::pair<rpc::Address, bool> GetBestNodeForTask(const TaskSpecification &spec) {
    num_lease_policy_consults++;
    return std::make_pair(fallback_rpc_address_, is_locality_aware);
  };

  ~MockLeasePolicy() {}

  rpc::Address fallback_rpc_address_;

  int num_lease_policy_consults = 0;

  bool is_locality_aware = false;
};

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestActorAndObjectDependencies1) {
  // Actor dependency resolved first.
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  TaskSpecification task;
  ObjectID obj = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());

  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  task.GetMutableMessage().add_args()->add_nested_inlined_refs()->set_object_id(
      actor_handle_id.Binary());

  int num_resolved = 0;
  actor_creator.actor_pending = true;
  resolver.ResolveDependencies(task, [&](const Status &) { num_resolved++; });
  ASSERT_EQ(num_resolved, 0);
  ASSERT_EQ(resolver.NumPendingTasks(), 1);

  for (const auto &cb : actor_creator.callbacks) {
    cb(Status());
  }
  ASSERT_EQ(num_resolved, 0);

  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  ASSERT_TRUE(store->Put(data, obj));
  ASSERT_EQ(num_resolved, 1);

  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestActorAndObjectDependencies2) {
  // Object dependency resolved first.
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  TaskSpecification task;
  ObjectID obj = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());

  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  task.GetMutableMessage().add_args()->add_nested_inlined_refs()->set_object_id(
      actor_handle_id.Binary());

  int num_resolved = 0;
  actor_creator.actor_pending = true;
  resolver.ResolveDependencies(task, [&](const Status &) { num_resolved++; });
  ASSERT_EQ(num_resolved, 0);
  ASSERT_EQ(resolver.NumPendingTasks(), 1);

  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  ASSERT_EQ(num_resolved, 0);
  ASSERT_TRUE(store->Put(data, obj));

  for (const auto &cb : actor_creator.callbacks) {
    cb(Status());
  }
  ASSERT_EQ(num_resolved, 1);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestHandlePlasmaPromotion) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  ObjectID obj1 = ObjectID::FromRandom();
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  ASSERT_TRUE(store->Put(data, obj1));
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_TRUE(task.ArgByRef(0));
  // Checks that the object id is still a direct call id.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestInlineLocalDependencies) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
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
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
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
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
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
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  auto data = GenerateRandomObject({obj3});
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
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

TaskSpecification BuildEmptyTaskSpec() {
  std::unordered_map<std::string, double> empty_resources;
  FunctionDescriptor empty_descriptor =
      FunctionDescriptorBuilder::BuildPython("", "", "", "");
  return BuildTaskSpec(empty_resources, empty_descriptor);
}

TEST(DirectTaskTransportTest, TestLocalityAwareSubmitOneTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  lease_policy->is_locality_aware = true;
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());

  TaskSpecification task = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_is_selected_based_on_locality_leases_requested, 1);
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
  ASSERT_EQ(task_finisher->num_task_retries_attempted, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());

  TaskSpecification task = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_is_selected_based_on_locality_leases_requested, 0);
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
  ASSERT_EQ(task_finisher->num_task_retries_attempted, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestRetryTaskApplicationLevelError) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();
  task.GetMutableMessage().set_retry_exceptions(true);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));
  // Simulate an application-level error.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), false, true));
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 1);
  ASSERT_EQ(task_finisher->num_task_retries_attempted, 1);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  task.GetMutableMessage().set_retry_exceptions(false);

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));
  // Simulate an application-level error.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), false, true));
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 2);
  ASSERT_EQ(task_finisher->num_task_retries_attempted, 1);
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();

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

TEST(DirectTaskTransportTest, TestHandleUnschedulableTask) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          2);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestHandleRuntimeEnvSetupFailed) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          2);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestWorkerHandleLocalRayletDied) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          2);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_DEATH(raylet_client->FailWorkerLeaseDueToGrpcUnavailable(), "");
}

TEST(DirectTaskTransportTest, TestDriverHandleLocalRayletDied) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::DRIVER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          2);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->FailWorkerLeaseDueToGrpcUnavailable());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->FailWorkerLeaseDueToGrpcUnavailable());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_finisher->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          2);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Trigger the periodic backlog report
  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlog_size, 1);

  // Task 1 is pushed; worker 3 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // Task 2 is pushed; no more workers requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // Task 3 is pushed; no more workers requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 3);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_finisher->num_tasks_complete, 3);
  ASSERT_EQ(task_finisher->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(DirectTaskTransportTest, TestSubmitMultipleTasks) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // Task 1 is pushed; worker 2 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->reported_backlog_size, 1);

  // Task 2 is pushed; worker 3 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

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
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task1 = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, NodeID::Nil()));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  // Task 1 finishes with exit status; the worker is not returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), /*exit=*/true));
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 1);
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

  absl::flat_hash_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          lease_client_factory,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();

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

  absl::flat_hash_map<int, std::shared_ptr<MockRayletClient>> remote_lease_clients;
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          lease_client_factory,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          local_raylet_id,
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_EQ(raylet_client->num_grant_or_reject_leases_requested, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_lease_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_raylet_id = NodeID::FromRandom();
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 7777, remote_raylet_id));
  ASSERT_EQ(remote_lease_clients.count(7777), 1);
  ASSERT_EQ(remote_lease_clients[7777]->num_workers_requested, 1);
  // Confirm that the spillback lease request has grant_or_reject set to true.
  ASSERT_EQ(remote_lease_clients[7777]->num_grant_or_reject_leases_requested, 1);
  // Confirm that lease policy is not consulted on spillback.
  ASSERT_EQ(lease_policy->num_lease_policy_consults, 1);
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, NodeID::Nil()));
  // Trigger a rejection back to the local node.
  ASSERT_TRUE(remote_lease_clients[7777]->GrantWorkerLease(
      "local", 1234, local_raylet_id, false, "", /*reject=*/true));
  // We should not have created another lease client to the local raylet.
  ASSERT_EQ(remote_lease_clients.size(), 1);
  // There should be no more callbacks on the remote node.
  ASSERT_FALSE(
      remote_lease_clients[7777]->GrantWorkerLease("remote", 1234, NodeID::Nil()));

  // The worker is returned to the local node.
  ASSERT_EQ(raylet_client->num_grant_or_reject_leases_requested, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
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
                       const TaskSpecification &same1,
                       const TaskSpecification &same2,
                       const TaskSpecification &different) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);

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
  FunctionDescriptor descriptor1 =
      FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  FunctionDescriptor descriptor2 =
      FunctionDescriptorBuilder::BuildPython("b", "", "", "");

  // Tasks with different resources should request different worker leases.
  RAY_LOG(INFO) << "Test different resources";
  TestSchedulingKey(store,
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources2, descriptor1));

  // Tasks with different functions should request different worker leases.
  RAY_LOG(INFO) << "Test different functions";
  TestSchedulingKey(store,
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor2));

  // Tasks with different depths should request different worker leases.
  RAY_LOG(INFO) << "Test different depths";
  TestSchedulingKey(store,
                    BuildTaskSpec(resources1, descriptor1, 0),
                    BuildTaskSpec(resources1, descriptor1, 0),
                    BuildTaskSpec(resources1, descriptor1, 1));

  // Tasks with different runtime envs do not request different workers.
  RAY_LOG(INFO) << "Test different runtimes";
  TestSchedulingKey(store,
                    BuildTaskSpec(resources1, descriptor1, 0, "a"),
                    BuildTaskSpec(resources1, descriptor1, 0, "b"),
                    BuildTaskSpec(resources1, descriptor1, 1, "a"));

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
  auto plasma_data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
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

TEST(DirectTaskTransportTest, TestBacklogReport) {
  rpc::Address address;
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  auto actor_creator = std::make_shared<MockActorCreator>();
  auto lease_policy = std::make_shared<MockLeasePolicy>();
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);

  TaskSpecification task1 = BuildEmptyTaskSpec();

  std::unordered_map<std::string, double> resources1({{"a", 1.0}});
  std::unordered_map<std::string, double> resources2({{"b", 2.0}});
  FunctionDescriptor descriptor1 =
      FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  FunctionDescriptor descriptor2 =
      FunctionDescriptorBuilder::BuildPython("b", "", "", "");
  ObjectID plasma1 = ObjectID::FromRandom();
  ObjectID plasma2 = ObjectID::FromRandom();
  // Force plasma objects to be promoted.
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto plasma_data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  ASSERT_TRUE(store->Put(plasma_data, plasma1));
  ASSERT_TRUE(store->Put(plasma_data, plasma2));

  // Same SchedulingClass, different SchedulingKey
  TaskSpecification task2 = BuildTaskSpec(resources1, descriptor1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());
  TaskSpecification task3 = BuildTaskSpec(resources1, descriptor1);
  task3.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma2.Binary());
  TestSchedulingKey(store, task2, task2, task3);

  TaskSpecification task4 = BuildTaskSpec(resources2, descriptor2);

  ASSERT_TRUE(submitter.SubmitTask(task1).ok());
  // One is requested and one is in the backlog for each SchedulingKey
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_TRUE(submitter.SubmitTask(task3).ok());
  ASSERT_TRUE(submitter.SubmitTask(task4).ok());
  ASSERT_TRUE(submitter.SubmitTask(task4).ok());

  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlogs.size(), 3);
  ASSERT_EQ(raylet_client->reported_backlogs[task1.GetSchedulingClass()], 0);
  ASSERT_EQ(raylet_client->reported_backlogs[task2.GetSchedulingClass()], 2);
  ASSERT_EQ(raylet_client->reported_backlogs[task4.GetSchedulingClass()], 1);
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          /*lease_timeout_ms=*/5,
                                          actor_creator,
                                          JobID::Nil(),
                                          absl::nullopt,
                                          1);
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();

  ASSERT_TRUE(submitter.SubmitTask(task).ok());
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, NodeID::Nil()));

  // Try force kill, exiting the worker
  ASSERT_TRUE(submitter.CancelTask(task, true, false).ok());
  ASSERT_EQ(worker_client->kill_requests.front().intended_task_id(),
            task.TaskId().Binary());
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("workerdying"), true));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
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
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();

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
  CoreWorkerDirectTaskSubmitter submitter(address,
                                          raylet_client,
                                          client_pool,
                                          nullptr,
                                          lease_policy,
                                          store,
                                          task_finisher,
                                          NodeID::Nil(),
                                          WorkerType::WORKER,
                                          kLongTimeout,
                                          actor_creator,
                                          JobID::Nil());
  TaskSpecification task = BuildEmptyTaskSpec();
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

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
