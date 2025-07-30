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
