#include "src/ray/util/size_literals.h"

#include <gtest/gtest.h>

namespace {

TEST(SizeLiteralsTest, BasicTest) {
  EXPECT_EQ(2_MiB, 2 * 1024 * 1024);
  EXPECT_EQ(2.5_KB, 2500);
}

}  // namespace
