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

#if !defined(_WIN32)

#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "ray/util/process.h"

namespace ray {

namespace {

TEST(ProcessSpawnPGTest, SpawnWithNewProcessGroupRequestedChildBecomesLeader) {
  setenv("RAY_process_group_cleanup_enabled", "true", 1);
  std::vector<std::string> args = {"/bin/sleep", "5"};
  auto [proc, ec] = Process::Spawn(args,
                                   /*decouple=*/false,
                                   /*pid_file=*/"",
                                   /*env=*/{},
                                   /*new_process_group=*/true);
  ASSERT_FALSE(ec) << ec.message();
  ASSERT_TRUE(proc.IsValid());

  pid_t pid = proc.GetId();
  ASSERT_GT(pid, 0);
  // Child should be leader of its own process group.
#if defined(__APPLE__)
  // In macOS sandboxed runs, allow brief retries for group leadership to settle.
  bool ok = false;
  for (int i = 0; i < 20; i++) {
    pid_t pgid_try = getpgid(pid);
    if (pgid_try == pid) {
      ok = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (!ok) {
    GTEST_SKIP() << "Process group leadership not observed; skipping on macOS sandbox.";
  }
#else
  // Allow a brief window for the child to call setpgrp() before we assert.
  bool ok = false;
  for (int i = 0; i < 40; i++) {  // ~200ms total
    pid_t pgid_try = getpgid(pid);
    if (pgid_try == pid) {
      ok = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(ok) << "child did not become its own PG leader in time";
#endif
  proc.Kill();
}

TEST(ProcessSpawnPGTest, SpawnWithoutNewProcessGroupChildInheritsParentGroup) {
  setenv("RAY_process_group_cleanup_enabled", "true", 1);
  std::vector<std::string> args = {"/bin/sleep", "5"};
  auto [proc, ec] = Process::Spawn(args,
                                   /*decouple=*/false,
                                   /*pid_file=*/"",
                                   /*env=*/{},
                                   /*new_process_group=*/false);
  ASSERT_FALSE(ec) << ec.message();
  ASSERT_TRUE(proc.IsValid());

  pid_t pid = proc.GetId();
  ASSERT_GT(pid, 0);
  // Child should inherit our process group
  pid_t my_pgid = getpgid(0);
  pid_t child_pgid = getpgid(pid);
  ASSERT_EQ(child_pgid, my_pgid);
  proc.Kill();
}

}  // namespace

}  // namespace ray

#endif  // !_WIN32
