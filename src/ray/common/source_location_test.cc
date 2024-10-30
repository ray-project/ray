#include "ray/common/source_location.h"

#include <gtest/gtest.h>

#include <sstream>

namespace ray {

namespace {

TEST(SourceLocationTest, StringifyTest) {
  // Default source location.
  {
    std::stringstream ss{};
    ss << SourceLocation();
    EXPECT_EQ(ss.str(), "");
  }

  // Initialized source location.
  {
    auto loc = RAY_LOC();
    std::stringstream ss{};
    ss << loc;
    EXPECT_EQ(ss.str(), "src/ray/common/source_location_test.cc:21");
  }
}

}  // namespace

}  // namespace ray
