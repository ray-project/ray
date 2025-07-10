#include "example.h"

#include <gtest/gtest.h>

#include <unordered_map>

TEST(SumMapTest, HandlesEmptyMap) {
  std::unordered_map<int, int> empty_map;
  EXPECT_EQ(SumMapValues(empty_map), 0);
}
