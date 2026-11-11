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

#include "ray/util/process.h"

#include <gtest/gtest.h>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <boost/process/child.hpp>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include "ray/util/logging.h"
#include "ray/util/process_utils.h"

namespace ray {

TEST(UtilTest, IsProcessAlive) {
  namespace bp = boost::process;
  bp::child c("bash");
  auto pid = c.id();
  c.join();
  for (int i = 0; i < 5; ++i) {
    if (IsProcessAlive(pid)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
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
    EXPECT_NE(std::find(child_procs.begin(), child_procs.end(), pid), child_procs.end());
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

TEST(UtilTest, CompareProcessObjects) {
  // Test the std::equal_to<Process> specialization with actual Process objects
  Process null1, null2;
  Process valid1(GetPID()), valid2(GetPID());  // Both reference the current process
  Process other_valid(1);                      // A different valid process (init/systemd)

  // Null process checks
  ASSERT_TRUE(null1.IsNull());
  ASSERT_TRUE(!null1.IsValid());
  ASSERT_TRUE(std::equal_to<Process>()(null1, null1));

  // Valid process checks
  ASSERT_TRUE(!valid1.IsNull());
  ASSERT_TRUE(valid1.IsValid());

  ASSERT_TRUE(std::equal_to<Process>()(null1, null2));
  ASSERT_TRUE(!std::equal_to<Process>()(null1, valid1));

  ASSERT_TRUE(!std::equal_to<Process>()(valid1, null1));
  ASSERT_TRUE(std::equal_to<Process>()(valid1, valid2));
  ASSERT_TRUE(!std::equal_to<Process>()(valid1, other_valid));

  ASSERT_TRUE(std::equal_to<Process>()(valid1, valid1));
}

#ifndef _WIN32
TEST(UtilTest, ProcessLifetimePipeDetectsEofAndDupCloexec) {
  int life_pipe[2] = {-1, -1};
  ASSERT_EQ(pipe(life_pipe), 0);

  // Track getpid() with a lifetime pipe to simulate PID reuse after peer close.
  Process proc(getpid(), life_pipe[0]);
  int flags = fcntl(life_pipe[0], F_GETFD);
  ASSERT_NE(flags, -1);
  EXPECT_NE(flags & FD_CLOEXEC, 0);
  EXPECT_TRUE(proc.IsAlive());

  int dup_fd = proc.DupLifetimeFd();
  ASSERT_GE(dup_fd, 0);
  int dup_flags = fcntl(dup_fd, F_GETFD);
  ASSERT_NE(dup_flags, -1);
  EXPECT_NE(dup_flags & FD_CLOEXEC, 0);
  Process dup_proc(getpid(), dup_fd);
  EXPECT_TRUE(dup_proc.IsAlive());

  close(life_pipe[1]);
  EXPECT_FALSE(proc.IsAlive());
  EXPECT_FALSE(dup_proc.IsAlive());
  // If Kill() failed to honor POLLHUP on fd_, this would SIGKILL our own test process.
  proc.Kill();
  dup_proc.Kill();

  // Process without a lifetime fd returns -1 from DupLifetimeFd().
  Process pid_only(getpid());
  EXPECT_EQ(pid_only.DupLifetimeFd(), -1);
}
#endif

}  // namespace ray
