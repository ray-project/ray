// Copyright 2026 The Ray Authors.
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

#include "ray/common/memory_monitor_test_fixture.h"

#include "ray/util/logging.h"

namespace ray {

std::string MemoryMonitorTestFixture::MockProcMemoryUsage(pid_t pid,
                                                          const std::string &usage_kb) {
  auto temp_dir_or = TempDirectory::Create();
  RAY_CHECK(temp_dir_or.ok()) << "Failed to create temp directory: "
                              << temp_dir_or.status().message();
  mock_proc_dirs_.push_back(std::move(temp_dir_or.value()));

  const std::string &proc_dir = mock_proc_dirs_.back()->GetPath();

  auto proc_subdir_or = TempDirectory::Create(proc_dir + "/" + std::to_string(pid));
  RAY_CHECK(proc_subdir_or.ok())
      << "Failed to create temp directory: " << proc_subdir_or.status().message();
  mock_proc_dirs_.push_back(std::move(proc_subdir_or.value()));

  // Create smaps_rollup file.
  std::string usage_filename = proc_dir + "/" + std::to_string(pid) + "/smaps_rollup";
  mock_proc_files_.push_back(std::make_unique<TempFile>(usage_filename));
  mock_proc_files_.back()->AppendLine("SomeHeader\n");
  mock_proc_files_.back()->AppendLine("Private_Clean: " + usage_kb + " kB\n");

  return proc_dir;
}

std::string MemoryMonitorTestFixture::MockCgroupMemoryUsage(int64_t total_bytes,
                                                            int64_t current_bytes,
                                                            int64_t inactive_file_bytes,
                                                            int64_t active_file_bytes) {
  auto temp_dir_or = TempDirectory::Create();
  RAY_CHECK(temp_dir_or.ok()) << "Failed to create temp directory: "
                              << temp_dir_or.status().message();
  mock_cgroup_dirs_.push_back(std::move(temp_dir_or.value()));

  const std::string &cgroup_path = mock_cgroup_dirs_.back()->GetPath();

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(cgroup_path + "/memory.max"));
  mock_cgroup_files_.back()->AppendLine(std::to_string(total_bytes) + "\n");

  mock_cgroup_files_.push_back(
      std::make_unique<TempFile>(cgroup_path + "/memory.current"));
  mock_cgroup_files_.back()->AppendLine(std::to_string(current_bytes) + "\n");

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(cgroup_path + "/memory.stat"));
  mock_cgroup_files_.back()->AppendLine("anon 123456\n");
  mock_cgroup_files_.back()->AppendLine("inactive_file " +
                                        std::to_string(inactive_file_bytes) + "\n");
  mock_cgroup_files_.back()->AppendLine("active_file " +
                                        std::to_string(active_file_bytes) + "\n");
  mock_cgroup_files_.back()->AppendLine("some_other_key 789\n");

  return cgroup_path;
}

}  // namespace ray
