// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/util/network_util.h"

#include <boost/asio/generic/basic_endpoint.hpp>
#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"

namespace ray {

TEST(NetworkUtilTest, TestBuildAddress) {
  // IPv4
  EXPECT_EQ(BuildAddress("192.168.1.1", 8080), "192.168.1.1:8080");
  EXPECT_EQ(BuildAddress("192.168.1.1", "8080"), "192.168.1.1:8080");

  // IPv6
  EXPECT_EQ(BuildAddress("::1", 8080), "[::1]:8080");
  EXPECT_EQ(BuildAddress("::1", "8080"), "[::1]:8080");
  EXPECT_EQ(BuildAddress("2001:db8::1", 8080), "[2001:db8::1]:8080");
  EXPECT_EQ(BuildAddress("2001:db8::1", "8080"), "[2001:db8::1]:8080");

  // Hostname
  EXPECT_EQ(BuildAddress("localhost", 9000), "localhost:9000");
  EXPECT_EQ(BuildAddress("localhost", "9000"), "localhost:9000");
}

TEST(NetworkUtilTest, TestParseAddress) {
  // IPv4
  auto result = ParseAddress("192.168.1.1:8080");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)[0], "192.168.1.1");
  EXPECT_EQ((*result)[1], "8080");

  // IPv6:loopback address
  result = ParseAddress("[::1]:8080");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)[0], "::1");
  EXPECT_EQ((*result)[1], "8080");

  // IPv6
  result = ParseAddress("[2001:db8::1]:8080");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)[0], "2001:db8::1");
  EXPECT_EQ((*result)[1], "8080");

  // Hostname:Port
  result = ParseAddress("localhost:9000");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)[0], "localhost");
  EXPECT_EQ((*result)[1], "9000");

  // bare IP or hostname
  // should return nullopt when no port is found
  result = ParseAddress("::1");
  ASSERT_FALSE(result.has_value());

  result = ParseAddress("2001:db8::1");
  ASSERT_FALSE(result.has_value());

  result = ParseAddress("192.168.1.1");
  ASSERT_FALSE(result.has_value());

  result = ParseAddress("localhost");
  ASSERT_FALSE(result.has_value());
}

TEST(NetworkUtilTest, UrlIpTcpParseTest) {
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://127.0.0.1:1/", 0), false),
            "127.0.0.1:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://127.0.0.1:1", 0), false),
            "127.0.0.1:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("tcp://127.0.0.1", 0), false), "127.0.0.1:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("127.0.0.1", 0), false), "127.0.0.1:0");
#ifndef _WIN32
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("unix:///tmp/sock"), false), "/tmp/sock");
  ASSERT_EQ(EndpointToUrl(ParseUrlEndpoint("/tmp/sock"), false), "/tmp/sock");
#endif
}

TEST(NetworkUtilTest, ParseURLTest) {
  const std::string url = "http://abc?num_objects=9&offset=8388878&size=8388878";
  auto parsed_url = *ParseURL(url);
  ASSERT_EQ(parsed_url["url"], "http://abc");
  ASSERT_EQ(parsed_url["num_objects"], "9");
  ASSERT_EQ(parsed_url["offset"], "8388878");
  ASSERT_EQ(parsed_url["size"], "8388878");
}

TEST(NetworkUtilTest, TestIsIPv6) {
  // IPv4 addresses should return false
  EXPECT_FALSE(IsIPv6("127.0.0.1"));
  EXPECT_FALSE(IsIPv6("192.168.1.1"));

  // IPv6 addresses should return true
  EXPECT_TRUE(IsIPv6("::1"));
  EXPECT_TRUE(IsIPv6("2001:db8::1"));
  EXPECT_TRUE(IsIPv6("::ffff:192.0.2.1"));

  // Invalid input should return false
  EXPECT_FALSE(IsIPv6(""));
  EXPECT_FALSE(IsIPv6("not-an-ip"));
  EXPECT_FALSE(IsIPv6("::1::2"));
}

}  // namespace ray
