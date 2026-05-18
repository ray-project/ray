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

#include "ray/common/memory_monitor_utils.h"
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

std::string MemoryMonitorTestFixture::MockCgroupv2MemoryUsage(
    int64_t total_bytes,
    int64_t current_bytes,
    std::optional<int64_t> anon_memory_bytes,
    std::optional<int64_t> shmem_memory_bytes,
    int64_t inactive_file_bytes,
    int64_t active_file_bytes) {
  auto temp_dir_or = TempDirectory::Create();
  RAY_CHECK(temp_dir_or.ok()) << "Failed to create temp directory: "
                              << temp_dir_or.status().message();
  mock_cgroup_dirs_.push_back(std::move(temp_dir_or.value()));

  const std::string &cgroup_path = mock_cgroup_dirs_.back()->GetPath();

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV2MemoryMaxPath));
  mock_cgroup_files_.back()->AppendLine(std::to_string(total_bytes) + "\n");

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV2MemoryUsagePath));
  mock_cgroup_files_.back()->AppendLine(std::to_string(current_bytes) + "\n");

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV2MemoryStatPath));
  if (anon_memory_bytes.has_value()) {
    mock_cgroup_files_.back()->AppendLine(
        std::string(MemoryMonitorUtils::kCgroupsV2MemoryAnonKey) + " " +
        std::to_string(*anon_memory_bytes) + "\n");
  }
  if (shmem_memory_bytes.has_value()) {
    mock_cgroup_files_.back()->AppendLine(
        std::string(MemoryMonitorUtils::kCgroupsV2MemoryShmemKey) + " " +
        std::to_string(*shmem_memory_bytes) + "\n");
  }
  mock_cgroup_files_.back()->AppendLine(
      std::string(MemoryMonitorUtils::kCgroupsV2MemoryStatInactiveFileKey) + " " +
      std::to_string(inactive_file_bytes) + "\n");
  mock_cgroup_files_.back()->AppendLine(
      std::string(MemoryMonitorUtils::kCgroupsV2MemoryStatActiveFileKey) + " " +
      std::to_string(active_file_bytes) + "\n");

  return cgroup_path;
}

std::string MemoryMonitorTestFixture::MockCgroupv1MemoryUsage(int64_t total_bytes,
                                                              int64_t current_bytes,
                                                              int64_t inactive_file_bytes,
                                                              int64_t active_file_bytes) {
  auto temp_dir_or = TempDirectory::Create();
  RAY_CHECK(temp_dir_or.ok()) << "Failed to create temp directory: "
                              << temp_dir_or.status().message();
  mock_cgroup_dirs_.push_back(std::move(temp_dir_or.value()));

  const std::string &cgroup_path = mock_cgroup_dirs_.back()->GetPath();

  auto memory_dir_or = TempDirectory::Create(cgroup_path + "/memory");
  RAY_CHECK(memory_dir_or.ok())
      << "Failed to create temp directory: " << memory_dir_or.status().message();
  mock_cgroup_dirs_.push_back(std::move(memory_dir_or.value()));

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV1MemoryMaxPath));
  mock_cgroup_files_.back()->AppendLine(std::to_string(total_bytes) + "\n");

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV1MemoryUsagePath));
  mock_cgroup_files_.back()->AppendLine(std::to_string(current_bytes) + "\n");

  mock_cgroup_files_.push_back(std::make_unique<TempFile>(
      cgroup_path + "/" + MemoryMonitorUtils::kCgroupsV1MemoryStatPath));
  mock_cgroup_files_.back()->AppendLine(
      std::string(MemoryMonitorUtils::kCgroupsV1MemoryStatInactiveFileKey) + " " +
      std::to_string(inactive_file_bytes) + "\n");
  mock_cgroup_files_.back()->AppendLine(
      std::string(MemoryMonitorUtils::kCgroupsV1MemoryStatActiveFileKey) + " " +
      std::to_string(active_file_bytes) + "\n");

  return cgroup_path;
}

}  // namespace ray
