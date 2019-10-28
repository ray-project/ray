#include "gtest/gtest.h"

#include "format/streaming_generated.h"
#include "streaming.h"
#include "streaming_dynamic_step.h"

using namespace ray;
using namespace ray::streaming;

class StreamingDynamicStepTest : public ::testing::Test {
  virtual void SetUp() {
    set_streaming_log_config("streaming_dynamic_step", StreamingLogLevel::WARNING);
  }
};

TEST_F(StreamingDynamicStepTest, streaming_default_step_test) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingDefaultStep());
  step_updater->InitStep(10);
  step_updater->UpdateStep(20);
  EXPECT_TRUE(step_updater->GetStep() == 10);
}

TEST_F(StreamingDynamicStepTest, streaming_dynamic_step_test) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingDynamicStep());
  step_updater->InitStep(10);
  step_updater->UpdateStep(20);
  EXPECT_TRUE(step_updater->GetStep() == 20);
}

TEST_F(StreamingDynamicStepTest, streaming_average_step_test) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingAverageStep());
  step_updater->InitStep(10);
  step_updater->UpdateStep(20);
  EXPECT_TRUE(step_updater->GetStep() == 15);
  step_updater->UpdateStep(5);
  EXPECT_TRUE(step_updater->GetStep() == 10);
}

TEST_F(StreamingDynamicStepTest, streaming_average_step_inc_test) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingAverageStep());
  step_updater->InitStep(10);
  step_updater->UpdateStep(10);
  EXPECT_TRUE(step_updater->GetStep() == 10);
  step_updater->UpdateStep(10);
  EXPECT_TRUE(step_updater->GetStep() == 20);
  step_updater->UpdateStep(10);
  STREAMING_LOG(WARNING) << step_updater->GetStep();
  EXPECT_TRUE(step_updater->GetStep() == 15);
}

TEST_F(StreamingDynamicStepTest, streaming_average_step_inc_test2) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingAverageStep());
  step_updater->InitStep(10);
  uint32_t step_vec[10] = {2, 4, 6, 7, 4, 5, 1, 5, 5, 5};
  uint32_t final_step[10] = {6, 5, 5, 6, 5, 5, 4, 4, 8, 6};
  for (int i = 0; i < 10; ++i) {
    step_updater->UpdateStep(step_vec[i]);
    EXPECT_TRUE(step_updater->GetStep() == final_step[i]);
  }
}

TEST_F(StreamingDynamicStepTest, streaming_average_step_inc_test3) {
  std::shared_ptr<StreamingDynamicStep> step_updater(new StreamingAverageStep());
  step_updater->SetMaxStep(20);
  step_updater->InitStep(10);
  uint32_t step_vec[10] = {2, 30, 40, 10, 20, 25, 5, 5, 5, 5};
  uint32_t final_step[10] = {6, 18, 20, 15, 17, 20, 12, 8, 6, 5};
  for (int i = 0; i < 10; ++i) {
    step_updater->UpdateStep(step_vec[i]);
    EXPECT_TRUE(step_updater->GetStep() == final_step[i]);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
