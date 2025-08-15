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

#include "ray/common/cgroup/test/cgroup_test_utils.h"

#include <gtest/gtest.h>

#include <string_view>
#include <unordered_set>

#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "ray/common/test/testing.h"
#include "ray/util/container_util.h"
#include "ray/util/filesystem.h"

namespace ray {

void AssertPidInCgroup(pid_t pid, const std::string &proc_filepath) {
  auto pids = ReadEntireFile(proc_filepath);
  RAY_ASSERT_OK(pids);
  std::string_view pids_sv = *pids;
  absl::ConsumeSuffix(&pids_sv, "\n");

  const std::unordered_set<std::string_view> pid_parts = absl::StrSplit(pids_sv, ' ');
  ASSERT_TRUE(pid_parts.find(std::to_string(pid)) != pid_parts.end())
      << "Couldn't find pid " << pid << "in cgroup proc file " << proc_filepath
      << ", all pids include "
      << DebugStringWrapper<std::unordered_set<std::string_view> >(pid_parts);
}

}  // namespace ray
