// Copyright 2020 The Ray Authors.
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

#include "ray/util/util.h"

#include <boost/asio/generic/basic_endpoint.hpp>
#include <boost/process/child.hpp>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

using namespace std::chrono_literals;  // NOLINT

namespace ray {

template <class T>
static std::string to_str(const T &obj, bool include_scheme) {
  return EndpointToUrl(obj, include_scheme);
}

TEST(UtilTest, UrlIpTcpParseTest) {
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1", 0), false), "127.0.0.1:0");
#ifndef _WIN32
  ASSERT_EQ(to_str(ParseUrlEndpoint("unix:///tmp/sock"), false), "/tmp/sock");
  ASSERT_EQ(to_str(ParseUrlEndpoint("/tmp/sock"), false), "/tmp/sock");
#endif
}

TEST(UtilTest, ParseURLTest) {
  const std::string url = "http://abc?num_objects=9&offset=8388878&size=8388878";
  auto parsed_url = *ParseURL(url);
  ASSERT_EQ(parsed_url["url"], "http://abc");
  ASSERT_EQ(parsed_url["num_objects"], "9");
  ASSERT_EQ(parsed_url["offset"], "8388878");
  ASSERT_EQ(parsed_url["size"], "8388878");
}

TEST(UtilTest, IsProcessAlive) {
  namespace bp = boost::process;
  bp::child c("bash");
  auto pid = c.id();
  c.join();
  for (int i = 0; i < 5; ++i) {
    if (IsProcessAlive(pid)) {
      std::this_thread::sleep_for(1s);
    } else {
      break;
    }
  }
  RAY_CHECK(!IsProcessAlive(pid));
}

TEST(UtilTest, GetAllProcsWithPpid) {
#if defined(__linux__)
  // Verify correctness by spawning several child processes,
  // then asserting that each PID is present in the output.

  namespace bp = boost::process;

  std::vector<bp::child> actual_child_procs;

  for (int i = 0; i < 10; ++i) {
    actual_child_procs.push_back(bp::child("bash"));
  }

  std::optional<std::vector<pid_t>> maybe_child_procs = GetAllProcsWithPpid(GetPID());

  // Assert optional has value.
  ASSERT_EQ(static_cast<bool>(maybe_child_procs), true);

  // Assert each actual process ID is contained in the returned vector.
  auto child_procs = *maybe_child_procs;
  for (auto &child_proc : actual_child_procs) {
    pid_t pid = child_proc.id();
    EXPECT_THAT(child_procs, ::testing::Contains(pid));
  }

  // Clean up each child proc.
  for (auto &child_proc : actual_child_procs) {
    child_proc.join();
  }
#else
  auto result = GetAllProcsWithPpid(1);
  ASSERT_EQ(result, std::nullopt);
#endif
}

}  // namespace ray

int main(int argc, char **argv) {
  int result = 0;
  if (argc > 1 && strcmp(argv[1], "--println") == 0) {
    // If we're given this special command, emit each argument on a new line
    for (int i = 2; i < argc; ++i) {
      fprintf(stdout, "%s\n", argv[i]);
    }
  } else {
    ::testing::InitGoogleTest(&argc, argv);
    result = RUN_ALL_TESTS();
  }
  return result;
}
