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

#include "ray/core_worker/transport/dependency_resolver.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace core {

TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const FunctionDescriptor &function_descriptor,
                                int64_t depth = 0,
                                std::string serialized_runtime_env = "") {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  rpc::JobConfig job_config;
  builder.SetCommonTaskSpec(TaskID::Nil(),
                            "dummy_task",
                            Language::PYTHON,
                            function_descriptor,
                            JobID::Nil(),
                            job_config,
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            empty_address,
                            1,
                            false,
                            resources,
                            resources,
                            serialized_runtime_env,
                            depth);
  return builder.Build();
}
TaskSpecification BuildEmptyTaskSpec() {
  std::unordered_map<std::string, double> empty_resources;
  FunctionDescriptor empty_descriptor =
      FunctionDescriptorBuilder::BuildPython("", "", "", "");
  return BuildTaskSpec(empty_resources, empty_descriptor);
}

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  void CompletePendingTask(const TaskID &,
                           const rpc::PushTaskReply &,
                           const rpc::Address &actor_addr,
                           bool is_application_error) override {
    num_tasks_complete++;
  }

  bool RetryTaskIfPossible(const TaskID &task_id, bool task_failed_due_to_oom) override {
    num_task_retries_attempted++;
    return false;
  }

  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status,
                       const rpc::RayErrorInfo *ray_error_info = nullptr) override {
    num_fail_pending_task_calls++;
  }

  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true,
                              bool fail_immediately = false) override {
    num_tasks_failed++;
    return true;
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {
    num_inlined_dependencies += inlined_dependency_ids.size();
    num_contained_ids += contained_ids.size();
  }

  bool MarkTaskCanceled(const TaskID &task_id) override { return true; }

  absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    TaskSpecification task = BuildEmptyTaskSpec();
    return task;
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {}

  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id) override {}

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
  int num_inlined_dependencies = 0;
  int num_contained_ids = 0;
  int num_task_retries_attempted = 0;
  int num_fail_pending_task_calls = 0;
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

TEST(LocalDependencyResolverTest, TestCancelDependencyResolution) {
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

  resolver.CancelDependencyResolution(task.TaskId());
  // Callback is not called.
  ASSERT_FALSE(ok);
  // Should not have inlined any dependencies.
  ASSERT_TRUE(task.ArgByRef(0));
  ASSERT_TRUE(task.ArgByRef(1));
  ASSERT_EQ(task_finisher->num_inlined_dependencies, 0);
  // Check for leaks.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestDependenciesAlreadyLocal) {
  auto store = std::make_shared<CoreWorkerMemoryStore>();
  auto task_finisher = std::make_shared<MockTaskFinisher>();
  MockActorCreator actor_creator;
  LocalDependencyResolver resolver(*store, *task_finisher, actor_creator);

  ObjectID obj = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store->Put(*data, obj));

  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
  ASSERT_TRUE(ok);
  // Check for leaks.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
