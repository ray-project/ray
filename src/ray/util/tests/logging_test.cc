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
#include <fstream>
#include <iostream>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"
#include "ray/util/filesystem.h"
#include "ray/util/util.h"

using namespace testing;
using json = nlohmann::json;

namespace ray {

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
using testing::internal::CaptureStdout;
using testing::internal::GetCapturedStderr;
using testing::internal::GetCapturedStdout;

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

TEST(PrintLogTest, TestTextLogging) {
  setEnv("RAY_BACKEND_LOG_JSON", "0");
  RayLog::StartRayLog("/tmp/gcs", RayLogLevel::INFO, "");
  CaptureStdout();
  RAY_LOG(INFO).WithField("key1", "value1").WithField("key2", "value2")
      << "contextual log";

  std::vector<std::string> log_lines =
      absl::StrSplit(GetCapturedStdout(), '\n', absl::SkipEmpty());
  ASSERT_EQ(1, log_lines.size());
  ASSERT_NE(log_lines[0].find("contextual log key1=value1 key2=value2"),
            std::string::npos);

  RayLog::ShutDownRayLog();
  unsetEnv("RAY_BACKEND_LOG_JSON");
}

TEST(PrintLogTest, TestJSONLogging) {
  setEnv("RAY_BACKEND_LOG_JSON", "1");
  RayLog::StartRayLog("/tmp/raylet", RayLogLevel::INFO, "");
  CaptureStdout();
  RAY_LOG(DEBUG) << "this is not logged";
  RAY_LOG(INFO) << "this is info logged";
  RAY_LOG(WARNING) << "this needs\nescape\"";
  RAY_LOG(INFO).WithField("key1", "value1").WithField("key2", "value\n2")
      << "contextual log";
  // Make sure logging binary (invalid utf-8) won't crash
  // even though it might generate invalid json.
  RAY_LOG(INFO) << "\xC3\x28";

  std::vector<std::string> log_lines =
      absl::StrSplit(GetCapturedStdout(), '\n', absl::SkipEmpty());
  ASSERT_EQ(4, log_lines.size());
  json log1 = json::parse(log_lines[0]);
  json log2 = json::parse(log_lines[1]);
  json log3 = json::parse(log_lines[2]);
  ASSERT_EQ(log1[std::string(kLogKeyMessage)], "this is info logged");
  ASSERT_EQ(log2[std::string(kLogKeyMessage)], "this needs\nescape\"");
  ASSERT_EQ(log3[std::string(kLogKeyMessage)], "contextual log");
  ASSERT_TRUE(log3.contains(kLogKeyAsctime));
  ASSERT_TRUE(log3.contains(kLogKeyFilename));
  ASSERT_TRUE(log3.contains(kLogKeyLineno));
  ASSERT_EQ(log3[std::string(kLogKeyLevelname)], "I");
  ASSERT_EQ(log3[std::string(kLogKeyComponent)], "raylet");
  ASSERT_EQ(log3["key1"], "value1");
  ASSERT_EQ(log3["key2"], "value\n2");
  ASSERT_TRUE(log_lines[3].find("\xC3\x28") != std::string::npos);

  RayLog::ShutDownRayLog();
  unsetEnv("RAY_BACKEND_LOG_JSON");
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
