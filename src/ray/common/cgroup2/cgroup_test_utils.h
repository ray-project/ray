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

#include <sys/types.h>

#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

class TempCgroupDirectory {
 public:
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

/**
  Starts a process in the given cgroup. Assumes the cgroup already exists and
  that the caller has read-write the lowest-common ancestor of the cgroup
  the current process is running in and the target cgroup.

  The spawned process will wait forever for the parent to unblock it and then
  reap it.

  @param target_cgroup_path target cgroup to create a process in.
  @return Status::OK with a pair of the processfd and pid if successful
  @return Status::InvalidArgument if target cgroup does exist or current process
  has insufficient permissions.
  @return Status::Invalid if process cannot be forked/cloned or processfd cannot
  be obtained.
*/
ray::StatusOr<std::pair<pid_t, int>> StartChildProcessInCgroup(
    const std::string &target_cgroup_path);

/**
  Kills the specified process and polls its processfd to reap it with a timeout.

  @param pid
  @param process_fd can be used as a fd and as a pid. It can be created using
  clone or pidfd_open or clone.
  @param timeout_ms

  @return Status::OK if successfully terminated the process and reaped it.
  @return Status::InvalidArgument if could not send SIGKILL to the process or poll its fd.
  @return Status::Invalid if could not reap the process within the timeout.
*/
ray::Status TerminateChildProcessAndWaitForTimeout(pid_t pid, int fd, int timeout_ms);

// Convenience methods so you can print the TempCgroupDirectory's path directly
// instead of calling temp_cgroup_dir.GetPath() everytime.
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
