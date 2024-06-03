// Copyright 2017 The Ray Authors.
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

#include "ray/util/logging.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <list>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/util/filesystem.h"

using namespace testing;

namespace ray {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

// This is not really test.
// This file just print some information using the logging macro.

void PrintLog() {
  RAY_LOG(DEBUG) << "This is the"
                 << " DEBUG"
                 << " message";
  RAY_LOG(INFO) << "This is the"
                << " INFO message";
  RAY_LOG(WARNING) << "This is the"
                   << " WARNING message";
  RAY_LOG(ERROR) << "This is the"
                 << " ERROR message";
  RAY_CHECK(true) << "This is a RAY_CHECK"
                  << " message but it won't show up";
  // The following 2 lines should not run since it will cause program failure.
  // RAY_LOG(FATAL) << "This is the FATAL message";
  // RAY_CHECK(false) << "This is a RAY_CHECK message but it won't show up";
}

TEST(PrintLogTest, LogTestWithoutInit) {
  // Without RayLog::StartRayLog, this should also work.
  PrintLog();
}

#if GTEST_HAS_STREAM_REDIRECTION
using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;

namespace {
void VerifyOnlyNthOccurenceLogged(bool fallback_to_debug) {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  static int non_fallback_counter = 0;
  static int fallback_counter = 0;
  int &counter = fallback_to_debug ? fallback_counter : non_fallback_counter;
  for (int i = 0; i < 9; i++) {
    counter++;
    if (fallback_to_debug) {
      RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
    } else {
      RAY_LOG_EVERY_N(INFO, 3) << kLogStr;
    }
  }
  std::string output = GetCapturedStderr();
  for (int i = counter - 8; i <= counter; i++) {
    std::string expected_str = absl::StrFormat("[%d] this is a test log", i);
    if (i % 3 == 1) {
      EXPECT_THAT(output, HasSubstr(expected_str));
    } else {
      EXPECT_THAT(output, Not(HasSubstr(expected_str)));
    }
  }

  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 3);
}

void VerifyAllOccurenceLogged() {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  for (int i = 0; i < 10; i++) {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
  }
  std::string output = GetCapturedStderr();
  size_t occurrences = 0;
  std::string::size_type start = 0;
  while ((start = output.find("[0] this is a test log", start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 10);
}

void VerifyNothingLogged(bool fallback_to_debug) {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  for (int i = 0; i < 10; i++) {
    if (fallback_to_debug) {
      RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
    } else {
      RAY_LOG_EVERY_N(INFO, 3) << kLogStr;
    };
  }
  std::string output = GetCapturedStderr();

  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 0);
}
}  // namespace

TEST(PrintLogTest, TestRayLogEveryN) {
  RayLog::severity_threshold_ = RayLogLevel::INFO;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::DEBUG;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::WARNING;
  VerifyNothingLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::INFO;
}

TEST(PrintLogTest, TestRayLogEveryNOrDebug) {
  RayLog::severity_threshold_ = RayLogLevel::INFO;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ true);

  RayLog::severity_threshold_ = RayLogLevel::DEBUG;
  VerifyAllOccurenceLogged();

  RayLog::severity_threshold_ = RayLogLevel::WARNING;
  VerifyNothingLogged(/*fallback_to_debug*/ true);

  RayLog::severity_threshold_ = RayLogLevel::INFO;
}

TEST(PrintLogTest, TestRayLogEveryMs) {
  CaptureStderr();
  const std::string kLogStr = "this is a test log";
  auto start_time = std::chrono::steady_clock::now().time_since_epoch();
  size_t num_iterations = 0;
  while (std::chrono::steady_clock::now().time_since_epoch() - start_time <
         std::chrono::milliseconds(100)) {
    num_iterations++;
    RAY_LOG_EVERY_MS(INFO, 10) << kLogStr;
  }
  std::string output = GetCapturedStderr();
  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_LT(occurrences, num_iterations);
  EXPECT_GT(occurrences, 5);
  EXPECT_LT(occurrences, 15);
}

#endif /* GTEST_HAS_STREAM_REDIRECTION */

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  RayLog::StartRayLog("", RayLogLevel::DEBUG, ray::GetUserTempDir());
  PrintLog();
  RayLog::ShutDownRayLog();
}

// This test will output large amount of logs to stderr, should be disabled in travis.
TEST(LogPerfTest, PerfTest) {
  RayLog::StartRayLog(
      "/fake/path/to/appdire/LogPerfTest", RayLogLevel::ERROR, ray::GetUserTempDir());
  int rounds = 10;

  int64_t start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_LOG(DEBUG) << "This is the "
                   << "RAY_DEBUG message";
  }
  int64_t elapsed = current_time_ms() - start_time;
  std::cout << "Testing DEBUG log for " << rounds << " rounds takes " << elapsed << " ms."
            << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_LOG(ERROR) << "This is the "
                   << "RAY_ERROR message";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing RAY_ERROR log for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_CHECK(i >= 0) << "This is a RAY_CHECK "
                      << "message but it won't show up";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing RAY_CHECK(true) for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;
  RayLog::ShutDownRayLog();
}

TEST(PrintLogTest, TestCheckOp) {
  int i = 1;
  RAY_CHECK_EQ(i, 1);
  ASSERT_DEATH(RAY_CHECK_EQ(i, 2), "1 vs 2");

  RAY_CHECK_NE(i, 0);
  ASSERT_DEATH(RAY_CHECK_NE(i, 1), "1 vs 1");

  RAY_CHECK_LE(i, 1);
  ASSERT_DEATH(RAY_CHECK_LE(i, 0), "1 vs 0");

  RAY_CHECK_LT(i, 2);
  ASSERT_DEATH(RAY_CHECK_LT(i, 1), "1 vs 1");

  RAY_CHECK_GE(i, 1);
  ASSERT_DEATH(RAY_CHECK_GE(i, 2), "1 vs 2");

  RAY_CHECK_GT(i, 0);
  ASSERT_DEATH(RAY_CHECK_GT(i, 1), "1 vs 1");

  int j = 0;
  RAY_CHECK_NE(i, j);
  ASSERT_DEATH(RAY_CHECK_EQ(i, j), "1 vs 0");
}

template <typename T>
std::string ToString(const T &value) {
  std::ostringstream oss;
  oss << value;
  return oss.str();
}

// Abuse RAY_LOG(FATAL) to test the formatted string.
TEST(PrintLogTest, TestPrintContainer) {
  static_assert(sfinae::has_operator_left_shift<ObjectID>, "ObjectID is printable");
  static_assert(sfinae::has_operator_left_shift<int64_t>, "int64_t is printable");
  static_assert(sfinae::has_operator_left_shift<std::string>, "std::string is printable");
  static_assert(sfinae::has_begin_end<std::vector<int>>, "std::vector<int> is iterable");
  static_assert(sfinae::has_begin_end<std::list<int>>, "std::list<int> is iterable");
  static_assert(sfinae::has_begin_end<absl::flat_hash_map<int, std::string>>,
                "absl::flat_hash_map<int, std::string> is iterable");
  static_assert(sfinae::has_first_second<std::pair<int, std::string>>,
                "std::pair<int, std::string> is iterable");

  std::ostringstream oss;

  // Basic std containers.
  std::vector<int> v = {1, 2, 3};
  EXPECT_EQ(ToString(ContainerLog(v)), "{1, 2, 3}");

  std::list<int> l = {1, 2, 3};
  EXPECT_EQ(ToString(ContainerLog(l)), "{1, 2, 3}");

  // Abseil maps and sets. They are unordered.
  absl::flat_hash_map<int, std::string> m = {{1, "one"}, {2, "two"}, {3, "three"}};
  EXPECT_THAT(
      ToString(ContainerLog(m)),
      AllOf(HasSubstr("{1, one}"), HasSubstr("{2, two}"), HasSubstr("{3, three}")));

  absl::flat_hash_set<int> absl_set = {2, 3, 5, 7, 11};
  EXPECT_THAT(ToString(ContainerLog(absl_set)),
              AllOf(HasSubstr("2"),
                    HasSubstr("3"),
                    HasSubstr("5"),
                    HasSubstr("7"),
                    HasSubstr("11")));

  // Ray IDs. ObjectIDs are in Hex.

  std::vector<ObjectID> object_ids = {
      ObjectID::FromRandom(),
      ObjectID::FromRandom(),
  };
  EXPECT_EQ(ToString(ContainerLog(object_ids)),
            "{" + object_ids[0].Hex() + ", " + object_ids[1].Hex() + "}");

  // Pairs.
  std::pair<NodeID, ObjectID> pair = {NodeID::FromRandom(), ObjectID::FromRandom()};
  EXPECT_EQ(ToString(ContainerLog(pair)),
            "{" + pair.first.Hex() + ", " + pair.second.Hex() + "}");

  // Nested containers.
  // sorted_destroyed_actor_list_ in gcs_actor_manager.h
  std::list<std::pair<ActorID, int64_t>> sorted_destroyed_actor_list_ = {
      {ActorID::FromHex("01010101010101010101010101010101"), 1},
      {ActorID::FromHex("02020202020202020202020202020202"), 2}};
  EXPECT_EQ(ToString(ContainerLog(sorted_destroyed_actor_list_)),
            "{{01010101010101010101010101010101, 1}, "
            "{02020202020202020202020202020202, 2}}");
  // Q: do we need to support std::shared_ptr and friends?
}

#ifndef _WIN32
std::string TestFunctionLevel0() {
  std::ostringstream oss;
  oss << ray::StackTrace();
  std::string stack_trace = oss.str();
  RAY_LOG(INFO) << "TestFunctionLevel0\n" << stack_trace;
  return stack_trace;
}

std::string TestFunctionLevel1() {
  RAY_LOG(INFO) << "TestFunctionLevel1:";
  return TestFunctionLevel0();
}

std::string TestFunctionLevel2() {
  RAY_LOG(INFO) << "TestFunctionLevel2:";
  return TestFunctionLevel1();
}

TEST(PrintLogTest, TestStackTrace) {
  auto ret0 = TestFunctionLevel0();
  EXPECT_TRUE(ret0.find("TestFunctionLevel0") != std::string::npos) << ret0;
  auto ret1 = TestFunctionLevel1();
  EXPECT_TRUE(ret1.find("TestFunctionLevel1") != std::string::npos) << ret1;
  auto ret2 = TestFunctionLevel2();
  EXPECT_TRUE(ret2.find("TestFunctionLevel2") != std::string::npos) << ret2;
}

int TerminateHandlerLevel0() {
  RAY_LOG(INFO) << "TerminateHandlerLevel0";
  auto terminate_handler = std::get_terminate();
  (*terminate_handler)();
  return 0;
}

int TerminateHandlerLevel1() {
  RAY_LOG(INFO) << "TerminateHandlerLevel1";
  TerminateHandlerLevel0();
  return 1;
}

TEST(PrintLogTest, TestTerminateHandler) {
  ray::RayLog::InstallTerminateHandler();
  ASSERT_DEATH(TerminateHandlerLevel1(),
               ".*TerminateHandlerLevel0.*TerminateHandlerLevel1.*");
}
#endif

TEST(PrintLogTest, TestFailureSignalHandler) {
  ray::RayLog::InstallFailureSignalHandler(nullptr);
  ASSERT_DEATH(abort(), ".*SIGABRT received.*");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
