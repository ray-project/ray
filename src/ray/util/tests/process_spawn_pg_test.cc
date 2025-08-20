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

TEST(ProcessSpawnPGTest, NewGroupWhenRequested) {
  std::vector<std::string> args = {"/bin/sh", "-c", "sleep 5"};
  auto [proc, ec] = Process::Spawn(args, /*decouple=*/false, /*pid_file=*/"",
                                   /*env=*/{}, /*new_process_group=*/true);
  ASSERT_FALSE(ec) << ec.message();
  ASSERT_TRUE(proc.IsValid());

  pid_t pid = proc.GetId();
  ASSERT_GT(pid, 0);
  // Child should be leader of its own process group
  pid_t pgid = getpgid(pid);
  ASSERT_EQ(pgid, pid);
  proc.Kill();
}

TEST(ProcessSpawnPGTest, SameGroupWhenNotRequested) {
  std::vector<std::string> args = {"/bin/sh", "-c", "sleep 5"};
  auto [proc, ec] = Process::Spawn(args, /*decouple=*/false, /*pid_file=*/"",
                                   /*env=*/{}, /*new_process_group=*/false);
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


