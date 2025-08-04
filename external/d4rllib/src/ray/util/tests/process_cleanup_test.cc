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

#include <gtest/gtest.h>

#if defined(__APPLE__) || defined(__linux__)

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <utility>

#include "ray/common/test/testing.h"
#include "ray/util/filesystem.h"
#include "ray/util/process_cleaner.h"
#include "ray/util/util.h"

namespace ray {

namespace {

TEST(ProcessCleanerTest, BasicTest) {
  const std::string kTestFname =
      absl::StrFormat("/tmp/process_cleanup_%s", GenerateUUIDV4());
  auto test_func = [fname = kTestFname]() {
    std::fstream f{fname, std::ios::app | std::ios::out};
    f << "helloworld";
    f.flush();
    f.close();
  };

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  // For child process, call [SpawnSubprocessAndCleanup] and exit.
  if (pid == 0) {
    SpawnSubprocessAndCleanup(std::move(test_func));
    std::_Exit(0);  // No flush stdout/stderr.
  }

  // For parent process, first wait until child process exits.
  int status = 0;
  ASSERT_NE(waitpid(pid, &status, 0), -1);

  // Wait for a while for grand child to execute cleanup function.
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Check whether expected file has been generated (by child process).
  auto content = ReadEntireFile(kTestFname);
  RAY_ASSERT_OK(content);
  EXPECT_EQ(*content, "helloworld");

  // Remove test file. Here we cannot use `ScopedTemporaryDirectory` because destructor
  // will be invoked in child process, thus double delete and crash.
  std::filesystem::remove(kTestFname);
}

}  // namespace

}  // namespace ray

#endif
