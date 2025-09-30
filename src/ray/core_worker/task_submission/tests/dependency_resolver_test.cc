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

#include "ray/core_worker/task_submission/dependency_resolver.h"

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mock/ray/core_worker/memory_store.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/fake_actor_creator.h"

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
                            false,
                            -1,
                            resources,
                            resources,
                            serialized_runtime_env,
                            depth,
                            TaskID::Nil(),
                            "");
  return std::move(builder).ConsumeAndBuild();
}
TaskSpecification BuildEmptyTaskSpec() {
  std::unordered_map<std::string, double> empty_resources;
  FunctionDescriptor empty_descriptor =
      FunctionDescriptorBuilder::BuildPython("", "", "", "");
  return BuildTaskSpec(empty_resources, empty_descriptor);
}

class MockTaskManager : public MockTaskManagerInterface {
 public:
  MockTaskManager() {}

  void CompletePendingTask(const TaskID &,
                           const rpc::PushTaskReply &,
                           const rpc::Address &actor_addr,
                           bool is_application_error) override {
    num_tasks_complete++;
  }

  bool RetryTaskIfPossible(const TaskID &task_id,
                           const rpc::RayErrorInfo &error_info) override {
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

  void MarkTaskCanceled(const TaskID &task_id) override {}

  void MarkTaskNoRetry(const TaskID &task_id) override {}

  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    TaskSpecification task = BuildEmptyTaskSpec();
    return task;
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {}

  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id,
                                   const WorkerID &worker_id) override {}

  bool IsTaskPending(const TaskID &task_id) const override { return true; }

  void MarkGeneratorFailedAndResubmit(const TaskID &task_id) override {}

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
  int num_inlined_dependencies = 0;
  int num_contained_ids = 0;
  int num_task_retries_attempted = 0;
  int num_fail_pending_task_calls = 0;
};

TEST(LocalDependencyResolverTest, TestNoDependencies) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok](Status) { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestActorAndObjectDependencies1) {
  // Actor dependency resolved first.
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  TaskSpecification task;
  ObjectID obj = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());

  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  task.GetMutableMessage().add_args()->add_nested_inlined_refs()->set_object_id(
      actor_handle_id.Binary());

  int num_resolved = 0;
  std::promise<bool> dependencies_resolved;
  actor_creator.actor_pending = true;
  resolver.ResolveDependencies(task, [&](const Status &) {
    num_resolved++;
    dependencies_resolved.set_value(true);
  });
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
  store->Put(data, obj);
  // Wait for the async callback to call
  ASSERT_TRUE(dependencies_resolved.get_future().get());
  ASSERT_EQ(num_resolved, 1);

  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestActorAndObjectDependencies2) {
  // Object dependency resolved first.
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  TaskSpecification task;
  ObjectID obj = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());

  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  task.GetMutableMessage().add_args()->add_nested_inlined_refs()->set_object_id(
      actor_handle_id.Binary());

  int num_resolved = 0;
  std::promise<bool> dependencies_resolved;
  actor_creator.actor_pending = true;
  resolver.ResolveDependencies(task, [&](const Status &) {
    num_resolved++;
    dependencies_resolved.set_value(true);
  });
  ASSERT_EQ(num_resolved, 0);
  ASSERT_EQ(resolver.NumPendingTasks(), 1);

  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  ASSERT_EQ(num_resolved, 0);
  store->Put(data, obj);

  for (const auto &cb : actor_creator.callbacks) {
    cb(Status());
  }
  // Wait for the async callback to call
  ASSERT_TRUE(dependencies_resolved.get_future().get());

  ASSERT_EQ(num_resolved, 1);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestHandlePlasmaPromotion) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  ObjectID obj1 = ObjectID::FromRandom();
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  store->Put(data, obj1);
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  bool ok = false;
  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task, [&](Status) {
    ok = true;
    dependencies_resolved.set_value(true);
  });
  ASSERT_TRUE(dependencies_resolved.get_future().get());
  ASSERT_TRUE(ok);
  ASSERT_TRUE(task.ArgByRef(0));
  // Checks that the object id is still a direct call id.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 0);
}

TEST(LocalDependencyResolverTest, TestInlineLocalDependencies) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  // Ensure the data is already present in the local store.
  store->Put(*data, obj1);
  store->Put(*data, obj2);
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task, [&](Status) {
    ok = true;
    dependencies_resolved.set_value(true);
  });
  ASSERT_TRUE(dependencies_resolved.get_future().get());
  // Tests that the task proto was rewritten to have inline argument values.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 2);
}

TEST(LocalDependencyResolverTest, TestInlinePendingDependencies) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task, [&](Status) {
    ok = true;
    dependencies_resolved.set_value(true);
  });
  ASSERT_EQ(resolver.NumPendingTasks(), 1);
  ASSERT_TRUE(!ok);
  store->Put(*data, obj1);
  store->Put(*data, obj2);

  ASSERT_TRUE(dependencies_resolved.get_future().get());
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 2);
  ASSERT_EQ(task_manager->num_contained_ids, 0);
}

TEST(LocalDependencyResolverTest, TestInlinedObjectIds) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  auto data = GenerateRandomObject({obj3});
  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());
  bool ok = false;
  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task, [&](Status) {
    ok = true;
    dependencies_resolved.set_value(true);
  });
  ASSERT_EQ(resolver.NumPendingTasks(), 1);
  ASSERT_TRUE(!ok);
  store->Put(*data, obj1);
  store->Put(*data, obj2);

  ASSERT_TRUE(dependencies_resolved.get_future().get());
  // Tests that the task proto was rewritten to have inline argument values after
  // resolution completes.
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 2);
  ASSERT_EQ(task_manager->num_contained_ids, 2);
}

TEST(LocalDependencyResolverTest, TestCancelDependencyResolution) {
  InstrumentedIOContextWithThread io_context("TestCancelDependencyResolution");
  auto store = std::make_shared<CoreWorkerMemoryStore>(io_context.GetIoService());
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });
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
  store->Put(*data, obj1);

  ASSERT_TRUE(resolver.CancelDependencyResolution(task.TaskId()));
  // Callback is not called.
  ASSERT_FALSE(ok);
  // Should not have inlined any dependencies.
  ASSERT_TRUE(task.ArgByRef(0));
  ASSERT_TRUE(task.ArgByRef(1));
  ASSERT_EQ(task_manager->num_inlined_dependencies, 0);
  // Check for leaks.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);

  io_context.Stop();
}

// Even if dependencies are already local, the ResolveDependencies callbacks are still
// called asynchronously in the event loop as a different task.
TEST(LocalDependencyResolverTest, TestDependenciesAlreadyLocal) {
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;
  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [](const ObjectID &object_id) {
        return rpc::TensorTransport::OBJECT_STORE;
      });

  ObjectID obj = ObjectID::FromRandom();
  auto data = GenerateRandomObject();
  store->Put(*data, obj);

  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());
  bool ok = false;
  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task, [&](Status) {
    ok = true;
    dependencies_resolved.set_value(true);
  });
  ASSERT_TRUE(dependencies_resolved.get_future().get());
  ASSERT_TRUE(ok);
  // Check for leaks.
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

TEST(LocalDependencyResolverTest, TestMixedTensorTransport) {
  // There are two arguments of the task, and the first argument is a GPU object
  // with tensor transport NCCL, and the second argument is a normal object with
  // tensor transport OBJECT_STORE.
  //
  // Both objects are small enough to be inlined. The first argument should be inlined
  // and the `object_ref` field should not be cleared so that this actor can use the
  // object ID as a key to retrieve the tensor from the GPU store. The second argument
  // should be inlined and the `object_ref` field should be cleared. If it is not cleared,
  // there will be performance regression in some edge cases.
  auto store = DefaultCoreWorkerMemoryStoreWithThread::Create();
  auto task_manager = std::make_shared<MockTaskManager>();
  FakeActorCreator actor_creator;

  // `obj1` is a GPU object, and `obj2` is a normal object.
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();

  LocalDependencyResolver resolver(
      *store, *task_manager, actor_creator, [&](const ObjectID &object_id) {
        if (object_id == obj1) {
          return rpc::TensorTransport::NCCL;
        }
        return rpc::TensorTransport::OBJECT_STORE;
      });

  auto data = GenerateRandomObject();
  store->Put(*data, obj1);
  store->Put(*data, obj2);

  TaskSpecification task;
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj2.Binary());

  std::promise<bool> dependencies_resolved;
  resolver.ResolveDependencies(task,
                               [&](Status) { dependencies_resolved.set_value(true); });
  ASSERT_TRUE(dependencies_resolved.get_future().get());

  // First arg (NCCL) should not be cleared
  ASSERT_TRUE(task.GetMutableMessage().args(0).is_inlined());
  ASSERT_TRUE(task.GetMutableMessage().args(0).has_object_ref());
  // Second arg (OBJECT_STORE) should be cleared
  ASSERT_TRUE(task.GetMutableMessage().args(1).is_inlined());
  ASSERT_FALSE(task.GetMutableMessage().args(1).has_object_ref());

  // The first argument is inlined but will not be passed into
  // `OnTaskDependenciesInlined` because it is a GPU object reference.
  // Please see https://github.com/ray-project/ray/pull/53911 for more details.
  ASSERT_EQ(task_manager->num_inlined_dependencies, 1);
  ASSERT_EQ(resolver.NumPendingTasks(), 0);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
