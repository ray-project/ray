#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "core_worker.h"

namespace ray {

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest() : core_worker_(WorkerType::WORKER, Language::PYTHON) {}

 protected:
  CoreWorker core_worker_;
};

TEST_F(CoreWorkerTest, TestAttributeGetters) {
  ASSERT_EQ(core_worker_.WorkerType(), WorkerType::WORKER);
  ASSERT_EQ(core_worker_.Language(), Language::PYTHON);
}

}  // namespace ray
