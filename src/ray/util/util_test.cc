#include "gtest/gtest.h"
#include "ray/util/command_line_args.h"
#include "ray/util/logging.h"

#include <iostream>

namespace ray {

TEST(CommandLineArgsTest, TestParse) {
  char *argv[] = {"test", "--k1", "v1", "--k2", "\"my_v2\"", "--k3", "\"\"", nullptr};
  CommandLineArgs command_line_args(7, argv);
  ASSERT_EQ("test", command_line_args.GetProgramName());
  ASSERT_EQ("v1", command_line_args.Get("k1"));
  ASSERT_EQ("my_v2", command_line_args.Get("k2"));
  ASSERT_EQ("", command_line_args.Get("k3"));
  ASSERT_EQ("default_v4", command_line_args.Get("k4", "default_v4"));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
