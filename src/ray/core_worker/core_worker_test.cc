#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "core_worker.h"
#include "ray/common/buffer.h"

namespace ray {

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest() : core_worker_(WorkerType::WORKER, Language::PYTHON) {}

 protected:
  CoreWorker core_worker_;
};

TEST_F(CoreWorkerTest, TestTaskArg) {
  // Test by-reference argument.
  ObjectID id = ObjectID::FromRandom();
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

}  // namespace ray
