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

#ifdef __linux__

#include "ray/util/pipe_logger.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <future>
#include <string_view>

#include "ray/util/tests/linux_test_utils.h"
#include "ray/util/util.h"

namespace ray {

namespace {

constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";

class PipeLoggerTest : public ::testing::TestWithParam<size_t> {};

TEST_P(PipeLoggerTest, LogWriteAndPersistence) {
  const size_t pipe_buffer_size = GetParam();
  setenv(kPipeLogReadBufSizeEnv.data(),
         absl::StrFormat("%d", pipe_buffer_size).data(),
         /*overwrite=*/1);

  // TODO(core): We should have a better test util, which allows us to create a temporary
  // testing directory.
  const std::string test_fname = absl::StrFormat("%s.out", GenerateUUIDV4());

  std::promise<void> promise{};
  auto on_completion = [&promise]() { promise.set_value(); };

  // Take the default option, which doesn't have rotation enabled.
  LogRotationOption logging_option{};
  auto log_token =
      CreatePipeAndStreamOutput(test_fname, logging_option, std::move(on_completion));

  ASSERT_EQ(write(log_token.write_fd, kLogLine1.data(), kLogLine1.length()),
            kLogLine1.length());
  ASSERT_EQ(write(log_token.write_fd, kLogLine2.data(), kLogLine2.length()),
            kLogLine2.length());

  // Write empty line, which is not expected to appear.
  ASSERT_EQ(write(log_token.write_fd, "\n", /*count=*/1), 1);

  log_token.termination_hook();
  promise.get_future().get();

  // Check log content after completion.
  const auto actual_content = CompleteReadFile(test_fname);
  const std::string expected_content = absl::StrFormat("%s%s", kLogLine1, kLogLine2);
  EXPECT_EQ(actual_content, expected_content);

  // Delete temporary file.
  EXPECT_EQ(unlink(test_fname.data()), 0);
}

INSTANTIATE_TEST_SUITE_P(PipeLoggerTest, PipeLoggerTest, testing::Values(1024, 3));

}  // namespace

}  // namespace ray

#endif  // __linux__
