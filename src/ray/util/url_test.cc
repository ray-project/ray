#include "ray/util/url.h"

#include <sstream>

#include "gtest/gtest.h"

namespace ray {

template <class T>
static std::string to_str(const T &obj) {
  std::ostringstream ss;
  ss << obj;
  return ss.str();
}

TEST(UrlTest, UrlIpTcpParseTest) {
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://[::1]:1/", 0)) == "[::1]:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://[::1]/", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://[::1]:1", 0)) == "[::1]:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://[::1]", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://::1/", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://::1", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://127.0.0.1:1/", 0)) == "127.0.0.1:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://127.0.0.1/", 0)) == "127.0.0.1:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://127.0.0.1:1", 0)) == "127.0.0.1:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("tcp://127.0.0.1", 0)) == "127.0.0.1:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("[::1]:1/", 0)) == "[::1]:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("[::1]/", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("[::1]:1", 0)) == "[::1]:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("[::1]", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("::1/", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("::1", 0)) == "[::1]:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("127.0.0.1:1/", 0)) == "127.0.0.1:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("127.0.0.1/", 0)) == "127.0.0.1:0");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("127.0.0.1:1", 0)) == "127.0.0.1:1");
  ASSERT_TRUE(to_str(parse_ip_tcp_endpoint("127.0.0.1", 0)) == "127.0.0.1:0");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
