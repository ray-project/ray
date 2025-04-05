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

// Util functions for cgroup related operations.

#pragma once

#include <string>

#include "ray/common/cgroup/cgroup_context.h"
#include "ray/common/cgroup/constants.h"
#include "ray/common/cgroup/scoped_cgroup_handle.h"
#include "ray/common/status.h"

namespace ray {

// Kill all processes under the given [cgroup_folder] and wait for all processes
// termination synchronously.
//
// \param cgroup_folder: cgroup folder which contains processes to kill.
Status KillAllProcAndWait(const std::string &cgroup_folder);

// Move all pids from [from] filepath to [to] filepath.
//
// \param from: filepath which contains pids to move from.
// \param to: filepath which should hold pids to move to.
Status MoveProcsBetweenCgroups(const std::string &from, const std::string &to);

// Cleans up cgroup after the raylet exits by killing all dangling processes and
// deleting the node cgroup.
//
// NOTE: This function is expected to be called once for each raylet instance at its
// termination.
//
// \param cgroup_system_proc_filepath: filepath which stores processes for ray system
// processes. \param cgroup_root_procs_filepath: filepath which stores processes for root
// cgroup. \param cgroup_app_directory: directory where application processes are stored
// under.
Status CleanupApplicationCgroup(const std::string &cgroup_system_proc_filepath,
                                const std::string &cgroup_root_procs_filepath,
                                const std::string &cgroup_app_directory);

// Apply cgroup context which addes pid into default cgroup folder.
ScopedCgroupHandler AddCurrentProcessToCgroup(CgroupSetupConfig setup_config,
                                              const AppProcCgroupMetadata &ctx);

}  // namespace ray
