#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "src/ray/util/test_util.h"

namespace ray {

TEST(DirectTaskTransportTest, TestNoDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  TaskSpecification task;
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(DirectTaskTransportTest, TestIgnorePlasmaDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::RAYLET);
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(DirectTaskTransportTest, TestInlineLocalDependencies) {
  auto ptr = std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore());
  CoreWorkerMemoryStoreProvider store(ptr);
  LocalDependencyResolver resolver(store);
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto data = GenerateRandomObject();
  store.Put(*data, obj1);
  store.Put(*data, obj2);
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  task.GetMutableMessage().add_args()->add_object_ids(obj2.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
}

TEST(DirectTaskTransportTest, TestInlinePendingDependencies) {
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
  ASSERT_TRUE(!ok);
  store.Put(*data, obj1);
  store.Put(*data, obj2);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(task.ArgByRef(0));
  ASSERT_FALSE(task.ArgByRef(1));
  ASSERT_NE(task.ArgData(0), nullptr);
  ASSERT_NE(task.ArgData(1), nullptr);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
