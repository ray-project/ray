// Copyright 2024 The Ray Authors.
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

#include "ray/raylet/cgroup_v2_utils.h"

#include <unistd.h>

#include <fstream>
#include <string>

#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "ray/util/logging.h"

namespace ray {

bool CanCurrenUserWriteCgroupV2() { return access("/sys/fs/cgroup", W_OK | X_OK) == 0; }

bool IsCgroupV2MountedAsRw() {
  // Checking all mountpoints directly and parse textually is the easiest way, compared
  // with mounted filesystem attributes.
  std::ifstream mounts("/proc/mounts");
  if (!mounts.is_open()) {
    return false;
  }

  // Mount information is formatted as:
  // <fs_spec> <fs_file> <fs_vfstype> <fs_mntopts> <dump-field> <fsck-field>
  std::string line;
  while (std::getline(mounts, line)) {
    std::vector<std::string_view> mount_info_tokens = absl::StrSplit(line, ' ');
    RAY_CHECK_EQ(mount_info_tokens.size(), 6UL);
    // For cgroupv2, `fs_spec` should be `cgroupv2` and there should be only one mount
    // information item.
    if (mount_info_tokens[0] != "cgroup2") {
      continue;
    }
    const auto &fs_mntopts = mount_info_tokens[3];

    // Mount options are formatted as: <opt1,opt2,...>.
    std::vector<std::string_view> mount_opts = absl::StrSplit(fs_mntopts, ',');

    // CgroupV2 has only one mount item, directly returns.
    return std::any_of(mount_opts.begin(),
                       mount_opts.end(),
                       [](const std::string_view cur_opt) { return cur_opt == "rw"; });
  }

  return false;
}

}  // namespace ray
