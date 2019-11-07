#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/transport/direct_task_transport.h"

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
  TaskSpecification task;
  task.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  bool ok = false;
  resolver.ResolveDependencies(task, [&ok]() { ok = true; });
  ASSERT_TRUE(ok);
}

TEST(DirectTaskTransportTest, TestInlinePendingDependencies) {
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
