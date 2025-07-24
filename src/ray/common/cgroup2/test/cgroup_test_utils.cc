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

#include "ray/common/cgroup2/test/cgroup_test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/sched.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>

#include "absl/strings/str_format.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/util/logging.h"

std::string GenerateRandomFilename(size_t len) {
  std::string output;
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(0, 61);
  output.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    int randomChar = dist(mt);
    if (randomChar < 26) {
      output.push_back('a' + randomChar);
    } else if (randomChar < 26 + 26) {
      output.push_back('A' + randomChar - 26);
    } else {
      output.push_back('0' + randomChar - 52);
    }
  }
  return output;
}

ray::StatusOr<std::unique_ptr<TempCgroupDirectory>> TempCgroupDirectory::Create(
    const std::string &base_path, mode_t mode) {
  std::string name = GenerateRandomFilename(kCgroupNameLength);
  std::string path = base_path + std::filesystem::path::preferred_separator + name;
  if (mkdir(path.c_str(), mode) == -1) {
    return ray::Status::IOError(
        absl::StrFormat("Failed to create cgroup directory at path %s.\n"
                        "Cgroup tests expect tmpfs and cgroupv2 to be mounted "
                        "and only run on Linux.\n"
                        "Error: %s",
                        path,
                        strerror(errno)));
  }
  auto output = std::make_unique<TempCgroupDirectory>(std::move(name), std::move(path));
  return ray::StatusOr<std::unique_ptr<TempCgroupDirectory>>(std::move(output));
}

TempCgroupDirectory::~TempCgroupDirectory() noexcept(false) {
  if (rmdir(path_.c_str()) != 0) {
    throw std::runtime_error(
        absl::StrFormat("Failed to delete a cgroup directory at %s. Please manually "
                        "delete it with rmdir. \n%s",
                        path_,
                        strerror(errno)));
  }
}

ray::StatusOr<std::unique_ptr<TempDirectory>> TempDirectory::Create() {
  std::string path = "/tmp/XXXXXX";
  char *ret = mkdtemp(path.data());
  if (ret == nullptr) {
    return ray::Status::UnknownError(
        absl::StrFormat("Failed to create a temp directory. "
                        "Cgroup tests expect tmpfs to be mounted and only run on Linux.\n"
                        "Error: %s",
                        strerror(errno)));
  }
  std::unique_ptr<TempDirectory> temp_dir =
      std::make_unique<TempDirectory>(std::move(path));
  return ray::StatusOr<std::unique_ptr<TempDirectory>>(std::move(temp_dir));
}
TempDirectory::~TempDirectory() { std::filesystem::remove_all(path_); }

ray::Status TerminateChildProcessAndWaitForTimeout(pid_t pid, int fd, int timeout_ms) {
  if (kill(pid, SIGTERM) == -1) {
    return ray::Status::Invalid(
        absl::StrFormat("Failed to send SIGKILL to pid: %i.\n"
                        "Error: %s",
                        pid,
                        strerror(errno)));
  }
  struct pollfd poll_fd = {
      .fd = fd,
      .events = POLLIN,
  };

  int poll_status = poll(&poll_fd, 1, timeout_ms);
  if (poll_status == -1) {
    return ray::Status::Invalid(absl::StrFormat(
        "Failed to poll process pid: %i, fd: %i. Process was not killed. Please "
        "kill it manually to prevent a leak.\n"
        "Error: %s",
        pid,
        fd,
        strerror(errno)));
  }
  if (poll_status == 0) {
    return ray::Status::Invalid(absl::StrFormat(
        "Process pid: %i, fd:%i was not killed within the timeout of %ims.",
        pid,
        fd,
        timeout_ms));
  }
  siginfo_t dummy = {0};
  int wait_id_status = waitid(P_PID, static_cast<id_t>(fd), &dummy, WEXITED);
  if (wait_id_status == -1) {
    if (errno != ECHILD)
      return ray::Status::Invalid(absl::StrFormat(
          "Failed to wait for process pid: %i, fd: %i. Process was not reaped, but "
          "it will be reaped by init after program exits.\n"
          "Error: %s",
          pid,
          fd,
          strerror(errno)));
  };
  return ray::Status::OK();
}
ray::StatusOr<std::pair<pid_t, int>> StartChildProcessInCgroup(
    const std::string &cgroup_path) {
  int cgroup_fd = open(cgroup_path.c_str(), O_RDONLY);
  if (cgroup_fd == -1) {
    return ray::Status::Invalid(
        absl::StrFormat("Unable to open fd for cgroup at %s.\n"
                        "Error: %s",
                        cgroup_path,
                        strerror(errno)));
  }

  // Will be set by clone3 if a child process is successfully created.
  pid_t child_pidfd = -1;

  clone_args cl_args = {};
  cl_args.flags = CLONE_PIDFD | CLONE_INTO_CGROUP;
  cl_args.cgroup = cgroup_fd;

  // Can be used both as a pid and as a fd.
  cl_args.pidfd = ((__u64)((uintptr_t)(&child_pidfd)));

  int child_pid = -1;

  if ((child_pid = syscall(__NR_clone3, &cl_args, sizeof(struct clone_args))) == -1) {
    RAY_LOG(ERROR) << "clone3 failed with error with error: " << strerror(errno);
    close(cgroup_fd);
    return ray::Status::Invalid(
        absl::StrFormat("Unable to clone process.\n"
                        "Error: %s",
                        strerror(errno)));
  }

  // Child process will execute this.
  if (child_pid == 0) {
    RAY_LOG(ERROR) << "Spawned child in cgroup " << cgroup_path << " with PID "
                   << getpid();
    pause();
    RAY_LOG(ERROR) << "Unpaused child in cgroup " << cgroup_path << " with PID "
                   << getpid();
    _exit(0);
  }

  // Parent process will continue here.
  close(cgroup_fd);
  RAY_LOG(ERROR) << "Successfully cloned with parent_pid=" << getpid()
                 << ", child_pid=" << child_pid << ", child_pidfd=" << child_pidfd << ".";
  return ray::StatusOr<std::pair<pid_t, int>>({child_pid, static_cast<int>(child_pidfd)});
}

TempFile::TempFile(std::string path) {
  path_ = path;
  fd_ = open(path_.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);  // NOLINT
  if (fd_ == -1) {
    throw std::runtime_error(
        absl::StrFormat("Failed to create a temp file. Cgroup tests expect "
                        "tmpfs to be mounted "
                        "and only run on Linux. Error: %s",
                        strerror(errno)));
  }
  file_output_stream_ = std::ofstream(path_, std::ios::trunc);
  if (!file_output_stream_.is_open()) {
    throw std::runtime_error("Could not open file on tmpfs.");
  }
}

TempFile::TempFile() {
  fd_ = mkstemp(path_.data());  // NOLINT
  if (fd_ == -1) {
    throw std::runtime_error(
        "Failed to create a temp file. Cgroup tests expect tmpfs to be "
        "mounted "
        "and only run on Linux");
  }
  if (unlink(path_.c_str()) == -1) {
    close(fd_);
    throw std::runtime_error("Failed to unlink temporary file.");
  }
  file_output_stream_ = std::ofstream(path_, std::ios::trunc);
  if (!file_output_stream_.is_open()) {
    throw std::runtime_error("Could not open mount file on tmpfs.");
  }
}

TempFile::~TempFile() {
  close(fd_);
  file_output_stream_.close();
}

void TempFile::AppendLine(const std::string &line) {
  file_output_stream_ << line;
  file_output_stream_.flush();
  if (file_output_stream_.fail()) {
    throw std::runtime_error("Could not write to mount file on tmpfs");
  }
}
