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
#pragma once

#include <pwd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

static constexpr size_t kCgroupNameLength = 6;

// Generates an ASCII string of the given length.
std::string GenerateRandomFilename(size_t len);

/**
  Scoped CgroupDirectory with a randomly generated name.
*/
class TempCgroupDirectory {
 public:
  /**
    Factory function that creates a cgroup directory with a random name.

    @param base_path the absolute path of the parent cgroup.
    @param mode defaults to rwx for everyone.

    @return
  */
  static ray::StatusOr<std::unique_ptr<TempCgroupDirectory>> Create(
      const std::string &base_path, mode_t mode = 0777);

  TempCgroupDirectory() = default;
  explicit TempCgroupDirectory(std::string &&name, std::string &&path)
      : name_(name), path_(path) {}

  TempCgroupDirectory(const TempCgroupDirectory &) = delete;
  TempCgroupDirectory(TempCgroupDirectory &&) = delete;
  TempCgroupDirectory &operator=(const TempCgroupDirectory &) = delete;
  TempCgroupDirectory &operator=(TempCgroupDirectory &&) = delete;

  const std::string &GetPath() const { return path_; }
  const std::string &GetName() const { return name_; }

  ~TempCgroupDirectory() noexcept(false);

 private:
  std::string name_;
  std::string path_;
};

/**
  RAII style class for creating and destroying temporary directory for testing.
  TODO(irabbani): add full documentation once complete.
  */
class TempDirectory {
 public:
  static ray::StatusOr<std::unique_ptr<TempDirectory>> Create();
  explicit TempDirectory(std::string &&path) : path_(path) {}

  TempDirectory(const TempDirectory &) = delete;
  TempDirectory(TempDirectory &&) = delete;
  TempDirectory &operator=(const TempDirectory &) = delete;
  TempDirectory &operator=(TempDirectory &&) = delete;

  const std::string &GetPath() const { return path_; }

  ~TempDirectory();

 private:
  const std::string path_;
};

/**
  RAII wrapper that creates a file that can be written to.
  TODO(irabbani): Add full documentation once the API is complete.
*/
class TempFile {
 public:
  explicit TempFile(std::string path);
  TempFile();

  TempFile(TempFile &other) = delete;
  TempFile(TempFile &&other) = delete;
  TempFile operator=(TempFile &other) = delete;
  TempFile &operator=(TempFile &&other) = delete;

  ~TempFile();
  void AppendLine(const std::string &line);

  const std::string &GetPath() const { return path_; }

 private:
  std::string path_ = "/tmp/XXXXXX";
  std::ofstream file_output_stream_;
  int fd_;
};

//// Starts a process in the cgroup and returns the pid of the started process.
//// Returns -1 if there's an error.
//// Note: clone3 supports creating a process inside a cgroup instead of creating
//// and then moving. However, clone3 does not have a glibc wrapper and
//// must be called directly using syscall syscall (see man 2 syscall).
//// This function needs linux kernel >= 5.7 to use the CLONE_INTO_CGROUP flag.
//// It is the caller's responsibility to terminate the child process and
//// wait for the pid. Use TerminateChildProcessAndWaitForTimeout.
//// Returns a process_fd. See the CLONE_PIDFD section in "man 2 clone"
ray::StatusOr<std::pair<pid_t, int>> StartChildProcessInCgroup(
    const std::string &cgroup_path);

/**
  @param process_fd can be used as a fd and as a pid. It can be created using
  clone or pidfd_open.
  @param timeout_ms time used to poll for the process_fd to detect the process
  has been killed.
  @return ok if successfully terminated the process and reaped it. Invalid otherwise
  with the correct error message.
  */
ray::Status TerminateChildProcessAndWaitForTimeout(pid_t pid, int fd, int timeout_ms);

std::ostream &operator<<(std::ostream &os, const TempCgroupDirectory &temp_cgroup_dir) {
  return os << temp_cgroup_dir.GetPath();
}

std::ostream &operator<<(std::ostream &os,
                         const std::unique_ptr<TempCgroupDirectory> &ptr) {
  if (ptr == nullptr) {
    return os << "<null>";
  }
  return os << *ptr;
}
