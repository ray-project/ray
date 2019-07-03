#include "gtest/gtest.h"

#include "ray/common/id.h"

#include <iostream>

namespace ray {

TEST(IdTest, TestJobId) {
  const int32_t value = 1;
  auto job_id = JobID::FromInt(value);

  std::cout << ">>>>>>" << job_id.Hex() << std::endl;
  ASSERT_EQ(1, 1);

  WorkerID driver_id = job_id.DriverId();
  std::cout << ">>>>>>" << driver_id.Hex() << std::endl;
}

} // namespace ray


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
