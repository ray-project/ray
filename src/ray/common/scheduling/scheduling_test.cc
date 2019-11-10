#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <stdlib.h>


#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {

class SchedulingTest : public ::testing::Test {
 public:
  void SetUp() {}

  void Shutdown() {}
};

TEST_F(SchedulingTest, SchedulingIdTest) {
  ScheduleIds ids;
  hash<string> hasher;
  int num = 10;  // should be greater than 10.

  for (int i = 0; i < num; i++) {
    ids.insertIdByString(to_string(i));
  }
  ASSERT_EQ(ids.count(), num);

  ids.removeIdByString(to_string(1));
  ASSERT_EQ(ids.count(), num - 1);

  ids.removeIdByInt(hasher(to_string(2)));
  ASSERT_EQ(ids.count(), num - 2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
