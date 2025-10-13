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

#include "ray/common/cgroup2/cgroup_test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/sched.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "absl/strings/str_format.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/util/logging.h"

ray::StatusOr<std::unique_ptr<TempCgroupDirectory>> TempCgroupDirectory::Create(
    const std::string &base_path, mode_t mode) {
  std::string random_name = ray::UniqueID::FromRandom().Hex();
  std::string name = random_name.substr(0, std::min<size_t>(6, random_name.size()));
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
  return output;
}

TempCgroupDirectory::~TempCgroupDirectory() noexcept(false) {
  // TODO(#54703): This can be refactored to disarm the destructor so that when you delete
  // a cgroup created with TempCgroupDirectory and delete it outside the handler, this
  // will not attempt to delete it.
  if (rmdir(path_.c_str()) == -1) {
    if (errno != ENOENT) {
      RAY_LOG(WARNING) << absl::StrFormat(
          "Failed to delete a cgroup directory at %s with error %s. Please manually "
          "delete it with rmdir.",
          path_,
          strerror(errno));
    }
  }
}

ray::StatusOr<std::unique_ptr<TempDirectory>> TempDirectory::Create() {
  std::string path = "/tmp/XXXXXX";
  char *ret = mkdtemp(path.data());
  if (ret == nullptr) {
    return ray::Status::Invalid(
        absl::StrFormat("Failed to create a temp directory on tmpfs with error %s."
                        "Cgroup tests expect tmpfs to be mounted and only run on Linux.",
                        strerror(errno)));
  }
  std::unique_ptr<TempDirectory> temp_dir =
      std::make_unique<TempDirectory>(std::move(path));
  return ray::StatusOr<std::unique_ptr<TempDirectory>>(std::move(temp_dir));
}

TempDirectory::~TempDirectory() {
  std::error_code error_code;
  RAY_CHECK(std::filesystem::remove_all(path_, error_code)) << absl::StrFormat(
      "Failed to delete temp directory at %s with error %s. Please manually "
      "delete it with rmdir.",
      path_,
      error_code.message());
}

/**
  Note: clone3 supports creating a process inside a cgroup instead of creating
  and then moving. However, clone3 does not have a glibc wrapper and
  must be called directly using syscall syscall (see man 2 syscall).
  This function needs linux kernel >= 5.7 to use the CLONE_INTO_CGROUP flag.
*/
#ifdef CLONE_INTO_CGROUP
ray::StatusOr<std::pair<pid_t, int>> StartChildProcessInCgroup(
    const std::string &cgroup_path) {
  int cgroup_fd = open(cgroup_path.c_str(), O_RDONLY);
  if (cgroup_fd == -1) {
    return ray::Status::InvalidArgument(
        absl::StrFormat("Unable to open fd for cgroup at %s with error %s.",
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
    close(cgroup_fd);
    return ray::Status::Invalid(
        absl::StrFormat("Failed to clone process into cgroup %s with error %s.",
                        cgroup_path,
                        strerror(errno)));
  }

  if (child_pid == 0) {
    // Child process will wait for parent to unblock it.
    pause();
    _exit(0);
  }

  // Parent process will continue here.
  close(cgroup_fd);
  return std::make_pair(child_pid, static_cast<int>(child_pidfd));
}
#else
// Fallback for older kernels. Uses fork/exec instead.
ray::StatusOr<std::pair<pid_t, int>> StartChildProcessInCgroup(
    const std::string &cgroup_path) {
  int new_pid = fork();
  if (new_pid == -1) {
    return ray::Status::Invalid(
        absl::StrFormat("Failed to fork process with error %s.", strerror(errno)));
  }

  if (new_pid == 0) {
    // Child process will pause and wait for parent to terminate and reap it.
    pause();
    _exit(0);
  }

  std::string cgroup_proc_file_path = cgroup_path + "/cgroup.procs";

  // Parent process has to move the process into a cgroup.
  int cgroup_fd = open(cgroup_proc_file_path.c_str(), O_RDWR);

  if (cgroup_fd == -1) {
    return ray::Status::Invalid(
        absl::StrFormat("Failed to open cgroup procs file at path %s with error %s.",
                        cgroup_proc_file_path,
                        strerror(errno)));
  }

  std::string pid_to_write = std::to_string(new_pid);

  if (write(cgroup_fd, pid_to_write.c_str(), pid_to_write.size()) == -1) {
    // Best effort killing of the child process because we couldn't move it
    // into the cgroup.
    kill(SIGKILL, new_pid);
    close(cgroup_fd);
    return ray::Status::Invalid(
        absl::StrFormat("Failed to write pid %i to cgroup procs file %s with error %s.",
                        new_pid,
                        cgroup_proc_file_path,
                        strerror(errno)));
  }

  close(cgroup_fd);

  int child_pidfd = static_cast<int>(syscall(SYS_pidfd_open, new_pid, 0));
  if (child_pidfd == -1) {
    // Best effort killing of the child process because we couldn't create
    // a pidfd from the process.
    kill(SIGKILL, new_pid);
    close(cgroup_fd);
    return ray::Status::Invalid(
        absl::StrFormat("Failed to create process fd for pid %i with error %s.",
                        new_pid,
                        strerror(errno)));
  }
  return std::make_pair(new_pid, child_pidfd);
}
#endif

ray::Status TerminateChildProcessAndWaitForTimeout(pid_t pid, int fd, int timeout_ms) {
  if (kill(pid, SIGKILL) == -1) {
    return ray::Status::InvalidArgument(absl::StrFormat(
        "Failed to send SIGTERM to pid: %i with error %s.", pid, strerror(errno)));
  }
  struct pollfd poll_fd = {
      .fd = fd,
      .events = POLLIN,
  };

  int poll_status = poll(&poll_fd, 1, timeout_ms);
  if (poll_status == -1) {
    return ray::Status::InvalidArgument(
        absl::StrFormat("Failed to poll process pid: %i, fd: %i with error %s. Process "
                        "was not killed. Kill it manually to prevent a leak.",
                        pid,
                        fd,
                        strerror(errno)));
  }
  if (poll_status == 0) {
    return ray::Status::Invalid(
        absl::StrFormat("Process pid: %i, fd: %i was not killed within the timeout of "
                        "%ims. Kill it manually to prevent a leak.",
                        pid,
                        fd,
                        timeout_ms));
  }
  siginfo_t dummy = {0};
  int wait_id_status = waitid(P_PID, static_cast<id_t>(fd), &dummy, WEXITED);
  if (wait_id_status == -1) {
    if (errno != ECHILD)
      return ray::Status::Invalid(
          absl::StrFormat("Failed to wait for process pid: %i, fd: %i with error %s. "
                          "Process was not reaped, but "
                          "it will be reaped by init after program exits.",
                          pid,
                          fd,
                          strerror(errno)));
  };
  return ray::Status::OK();
}

TempFile::TempFile(std::string path) {
  path_ = path;
  fd_ = open(path_.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);  // NOLINT
  RAY_CHECK(fd_ != -1) << absl::StrFormat(
      "Failed to create a temp file at path %s with error %s. Cgroup tests expect "
      "tmpfs to be mounted and only run on Linux.",
      path_,
      strerror(errno));
  file_output_stream_ = std::ofstream(path_, std::ios::trunc);
  RAY_CHECK(file_output_stream_.is_open()) << absl::StrFormat(
      "Failed to open file %s on tmpfs with error %s", path_, strerror(errno));
}

TempFile::TempFile() {
  fd_ = mkstemp(path_.data());  // NOLINT
  if (fd_ == -1) {
    throw std::runtime_error(
        "Failed to create a temp file. Cgroup tests expect tmpfs to be "
        "mounted "
        "and only run on Linux");
  }
  file_output_stream_ = std::ofstream(path_, std::ios::trunc);
  RAY_CHECK(file_output_stream_.is_open())
      << absl::StrFormat("Could not open temporary file at path %s.", path_);
}

TempFile::~TempFile() {
  RAY_CHECK(close(fd_) != -1) << absl::StrFormat(
      "Failed to close file descriptor with error %s.", strerror(errno));
  file_output_stream_.close();
  RAY_CHECK(unlink(path_.c_str()) != -1)
      << absl::StrFormat("Failed to unlink temporary file at path %s with error %s.",
                         path_,
                         strerror(errno));
}

void TempFile::AppendLine(const std::string &line) {
  file_output_stream_ << line;
  file_output_stream_.flush();
  // All current callers treat this is as a fatal error so this is a RAY_CHECK
  // instead of returning a Status.
  RAY_CHECK(file_output_stream_.good())
      << absl::StrFormat("Failed to write to temporary file at path %s.", path_);
}
