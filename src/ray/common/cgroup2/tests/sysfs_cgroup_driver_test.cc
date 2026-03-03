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

#include "ray/common/cgroup2/sysfs_cgroup_driver.h"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

TEST(SysFsCgroupDriverTest, CheckCgroupv2EnabledFailsIfEmptyMountFile) {
  TempFile temp_mount_file;
  SysFsCgroupDriver driver(temp_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(SysFsCgroupDriverTest, CheckCgroupv2EnabledFailsIfMalformedMountFile) {
  TempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup/unified/ rw 0 0\n");
  temp_mount_file.AppendLine("oopsie");
  SysFsCgroupDriver driver(temp_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(SysFsCgroupDriverTest,
     CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2NotMounted) {
  TempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  SysFsCgroupDriver driver(temp_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  ASSERT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(SysFsCgroupDriverTest,
     CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2Mounted) {
  TempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup/unified/ rw 0 0\n");
  SysFsCgroupDriver driver(temp_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  ASSERT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(SysFsCgroupDriverTest,
     CheckCgroupv2EnabledSucceedsIfMountFileNotFoundButFallbackFileIsCorrect) {
  TempFile temp_fallback_mount_file;
  temp_fallback_mount_file.AppendLine("cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n");
  SysFsCgroupDriver driver("/does/not/exist", temp_fallback_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(SysFsCgroupDriverTest, CheckCgroupv2EnabledSucceedsIfOnlyCgroupv2Mounted) {
  TempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n");
  SysFsCgroupDriver driver(temp_mount_file.GetPath());
  Status s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(SysFsCgroupDriver, CheckCgroupFailsIfNotCgroupv2Path) {
  // This is not a directory on the cgroupv2 vfs.
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(SysFsCgroupDriver, CheckCgroupFailsIfCgroupDoesNotExist) {
  // This is not a directory on the cgroupv2 vfs.
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup("/some/path/that/doesnt/exist");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(SysFsCgroupDriver, DeleteCgroupFailsIfNotCgroup2Path) {
  // This is not a directory on the cgroupv2 vfs.
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DeleteCgroup(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(SysFsCgroupDriver, DeleteCgroupFailsIfCgroupDoesNotExist) {
  // This is not a directory on the cgroupv2 vfs.
  SysFsCgroupDriver driver;
  Status s = driver.DeleteCgroup("/some/path/that/doesnt/exist");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(SysFsCgroupDriver, GetAvailableControllersFailsIfNotCgroup2Path) {
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  std::filesystem::path controller_file_path =
      std::filesystem::path(temp_dir->GetPath()) /
      std::filesystem::path("cgroup.controllers");
  TempFile controller_file(controller_file_path);
  controller_file.AppendLine("cpuset cpu io memory hugetlb pids rdma misc");
  SysFsCgroupDriver driver;
  StatusOr<std::unordered_set<std::string>> s =
      driver.GetAvailableControllers(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(SysFsCgroupDriver, EnableControllerFailsIfNotCgroupv2Path) {
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(temp_dir->GetPath(), "cpu");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(SysFsCgroupDriver, DisableControllerFailsIfNotCgroupv2Path) {
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(temp_dir->GetPath(), "cpu");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(SysFsCgroupDriver, AddConstraintFailsIfNotCgroupv2Path) {
  auto temp_dir_or_status = TempDirectory::Create();
  ASSERT_TRUE(temp_dir_or_status.ok()) << temp_dir_or_status.ToString();
  std::unique_ptr<TempDirectory> temp_dir = std::move(temp_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(temp_dir->GetPath(), "memory.min", "1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

};  // namespace ray
