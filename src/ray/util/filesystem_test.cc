#include "ray/util/filesystem.h"

#include "gtest/gtest.h"

namespace ray {

TEST(FileSystemTest, PathParseTest) {
  ASSERT_EQ(GetFileName("."), ".");
  ASSERT_EQ(GetFileName(".."), "..");
  ASSERT_EQ(GetFileName("foo/bar"), "bar");
  ASSERT_EQ(GetFileName("///bar"), "bar");
  ASSERT_EQ(GetFileName("///bar/"), "");
#ifdef _WIN32
  ASSERT_EQ(GetFileName("C:"), "");
  ASSERT_EQ(GetFileName("C::"), ":");  // just to match Python behavior
  ASSERT_EQ(GetFileName("CC::"), "CC::");
  ASSERT_EQ(GetFileName("C:\\"), "");
#endif
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
