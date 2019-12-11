#include "gtest/gtest.h"

#include "util/streaming_util.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingUtilTest, test_Byte2hex) {
  const uint8_t data[2] = {0x11, 0x07};
  EXPECT_TRUE(Util::Byte2hex(data, 2) == "1107");
  EXPECT_TRUE(Util::Byte2hex(data, 2) != "1108");
}

TEST(StreamingUtilTest, test_Hex2str) {
  const uint8_t data[2] = {0x11, 0x07};
  EXPECT_TRUE(std::memcmp(Util::Hexqid2str("1107").c_str(), data, 2) == 0);
  const uint8_t data2[2] = {0x10, 0x0f};
  EXPECT_TRUE(std::memcmp(Util::Hexqid2str("100f").c_str(), data2, 2) == 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
