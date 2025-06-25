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

#include "ray/common/cgroup/cgroup_manager.h"

namespace ray {

// Here the possible types of cgroup setup classes are small, so we use if-else branch
// instead of registry pattern.
BaseCgroupSetup &GetCgroupSetup(bool enable_resource_isolation) {
  if (enable_resource_isolation) {
    // TODO(hjiang): Enable real cgroup setup after PR:
    // https://github.com/ray-project/ray/pull/49941
    static NoopCgroupSetup noop_cgroup_setup{};
    return noop_cgroup_setup;
  }
  static NoopCgroupSetup noop_cgroup_setup{};
  return noop_cgroup_setup;
}

}  // namespace ray
