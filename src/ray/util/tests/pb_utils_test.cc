#include "ray/util/pb_utils.h"

#include <gtest/gtest.h>

#include "src/ray/util/tests/test.pb.h"

namespace {

TEST(PbUtilsTest, MapEqual) {
  ray::InnerMessage inner;
  inner.set_x(10);

  ray::MapMessage msg;
  (*msg.mutable_map_with_pb())["key"] = std::move(inner);

  EXPECT_TRUE(MapEqual(msg.map_with_pb(), msg.map_with_pb()));
}

}  // namespace
