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

#include "gtest/gtest.h"
#include "ray/util/filesystem.h"

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

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  RayLog::StartRayLog("", RayLogLevel::DEBUG, ray::GetUserTempDir() + ray::GetDirSep());
  PrintLog();
  RayLog::ShutDownRayLog();
}

// This test will output large amount of logs to stderr, should be disabled in travis.
TEST(LogPerfTest, PerfTest) {
  RayLog::StartRayLog("/fake/path/to/appdire/LogPerfTest", RayLogLevel::ERROR,
                      ray::GetUserTempDir() + ray::GetDirSep());
  int rounds = 100000;

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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
