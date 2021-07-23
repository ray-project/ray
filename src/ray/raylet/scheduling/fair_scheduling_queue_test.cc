#include "ray/raylet/scheduling/fair_scheduling_queue.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

namespace raylet {

using ::testing::_;

class SchedulingQueueTest : public ::testing::Test {
  FairSchedulingQueue queue_;
};

TEST_F(SchedulingQueueTest, TestBasic) {}
}  // namespace raylet
}  // namespace ray
