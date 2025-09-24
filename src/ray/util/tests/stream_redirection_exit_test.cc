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

// This test case only checks whether stream redirection process could exit normally.

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "absl/strings/str_format.h"
#include "ray/common/id.h"
#include "ray/common/tests/testing.h"
#include "ray/util/filesystem.h"
#include "ray/util/stream_redirection.h"

namespace ray {

namespace {
constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world";
}  // namespace

TEST(LoggingUtilTest, RedirectStderr) {
  const std::string test_file_path =
      absl::StrFormat("%s.err", UniqueID::FromRandom().Hex());

  // Works via `dup`, so have to execute before we redirect via `dup2` and close stderr.
  testing::internal::CaptureStderr();

  // Redirect stderr for testing, so we could have stdout for debugging.
  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  opts.tee_to_stderr = true;
  RedirectStderrOncePerProcess(opts);

  std::cerr << kLogLine1 << std::flush;
  std::cerr << kLogLine2 << std::flush;

  // TODO(hjiang): Current implementation is flaky intrinsically, sleep for a while to
  // make sure pipe content has been read over to spdlog.
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray
