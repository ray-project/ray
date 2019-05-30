#include <thread>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "core_worker.h"
#include "context.h"
#include "ray/common/buffer.h"

namespace ray {

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest()
    : core_worker_(WorkerType::WORKER, Language::PYTHON, "", "") {}

 protected:
  CoreWorker core_worker_;
};

TEST_F(CoreWorkerTest, TestTaskArg) {
  // Test by-reference argument.
  ObjectID id = ObjectID::from_random();
  TaskArg by_ref = TaskArg::PassByReference(id);
  ASSERT_TRUE(by_ref.IsPassedByReference());
  ASSERT_EQ(by_ref.GetReference(), id);
  // Test by-value argument.
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(static_cast<uint8_t *>(0), 0);
  TaskArg by_value = TaskArg::PassByValue(buffer);
  ASSERT_FALSE(by_value.IsPassedByReference());
  auto data = by_value.GetValue();
  ASSERT_TRUE(data != nullptr);
  ASSERT_EQ(*data, *buffer);
}

TEST_F(CoreWorkerTest, TestAttributeGetters) {
  ASSERT_EQ(core_worker_.WorkerType(), WorkerType::WORKER);
  ASSERT_EQ(core_worker_.Language(), Language::PYTHON);
}

TEST_F(CoreWorkerTest, TestWorkerContext) {
  auto driver_id = DriverID::from_random();
  WorkerContext context(WorkerType::WORKER, driver_id);
  ASSERT_EQ(context.worker_type, WorkerType::WORKER);

  ASSERT_EQ(context.task_index, 0);
  ASSERT_EQ(context.put_index, 0);

  auto thread_func = [&context]() {
    // TODO
    ASSERT_EQ(context.task_index, 0);
  };
  std::thread async_thread(thread_func);

  async_thread.join();
}

}  // namespace ray
