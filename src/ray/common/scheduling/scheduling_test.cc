#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <string>
#include <thread>

#include "ray/common/scheduling/scheduling_ids.h"
using namespace std;

namespace ray {

class SchedulingTest : public ::testing::Test {
 public:
  void SetUp() {}

  void Shutdown() {}
};

TEST_F(SchedulingTest, SchedulingIdTest) {
  StringIdMap ids;
  hash<string> hasher;
  int num = 10;  // should be greater than 10.

  for (int i = 0; i < num; i++) {
    ids.insert(to_string(i));
  }
  ASSERT_EQ(ids.count(), num);

  ids.remove(to_string(1));
  ASSERT_EQ(ids.count(), num - 1);

  ids.remove(hasher(to_string(2)));
  ASSERT_EQ(ids.count(), num - 2);

  ASSERT_TRUE(ids.get(to_string(3)) == static_cast<int64_t>(hasher(to_string(3))));

  ASSERT_TRUE(ids.get(to_string(100)) == -1);

  /// Test for handling collision.
  StringIdMap short_ids;
  for (int i = 0; i < 10; i++) {
    /// "true" reduces the range of IDs to [0..9]
    int64_t id = short_ids.insert(to_string(i), true);
    ASSERT_TRUE(id < 10);
  }
  ASSERT_EQ(short_ids.count(), 10);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
