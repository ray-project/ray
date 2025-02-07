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

#include "ray/util/stream_redirection_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "ray/common/test/testing.h"
#include "ray/util/filesystem.h"
#include "ray/util/util.h"

namespace ray {

namespace {
constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world";
}  // namespace

TEST(LoggingUtilTest, RedirectStderr) {
  const std::string test_file_path = absl::StrFormat("%s.err", GenerateUUIDV4());

  // Redirect stderr for testing, so we could have stdout for debugging.
  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  opts.tee_to_stderr = true;
  opts.rotation_max_size = 5;
  opts.rotation_max_file_count = 2;
  RedirectStderr(opts);

  std::cerr << kLogLine1 << std::flush;
  std::cerr << kLogLine2 << std::flush;

  // TODO(hjiang): Current implementation is flaky intrinsically, sleep for a while to
  // make sure pipe content has been read over to spdlog.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  FlushOnRedirectedStderr();

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray
