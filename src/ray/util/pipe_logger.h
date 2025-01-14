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

// Util on logging with pipe.

#pragma once

#include <functional>
#include <limits>
#include <string>

#include "ray/util/compat.h"

namespace ray {

// Environmenr variable, which indicates the pipe size of read.
//
// TODO(hjiang): Should document the env variable after end-to-end integration has
// finished.
inline constexpr std::string_view kPipeLogReadBufSizeEnv = "RAY_PIPE_LOG_READ_BUF_SIZE";

// Configuration for log rotation. By default no rotation enabled.
struct LogRotationOption {
  // Max number of bytes in a rotated file.
  size_t rotation_max_size = std::numeric_limits<size_t>::max();
  // Max number of files for all rotated files.
  size_t rotation_max_file_count = 1;
};

class RotationFileHandle {
 public:
  RotationFileHandle(MEMFD_TYPE_NON_UNIQUE write_handle,
                     std::function<void()> termination_caller)
      : write_handle_(write_handle), termination_caller_(std::move(termination_caller)) {}
  RotationFileHandle(const RotationFileHandle &) = delete;
  RotationFileHandle &operator=(const RotationFileHandle &) = delete;

  // Used to write to.
  //
  // TODO(hjiang): I will followup with another PR to make a `FD` class, which is not
  // copiable to avoid manual `dup`.
#if defined(__APPLE__) || defined(__linux__)
  RotationFileHandle(RotationFileHandle &&rhs) {
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = -1;
    termination_caller_ = std::move(rhs.termination_caller_);
  }
  RotationFileHandle &operator=(RotationFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = -1;
    termination_caller_ = std::move(rhs.termination_caller_);
    return *this;
  }
  ~RotationFileHandle() {
    // Only invoke termination functor when handler at a valid state.
    if (write_handle_ != -1) {
      termination_caller_();
    }
  }

  int GetWriteHandle() const { return write_handle_; }

#elif defined(_WIN32)
  RotationFileHandle(RotationFileHandle &&rhs) {
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = nullptr;
    termination_caller_ = std::move(rhs.termination_caller_);
  }
  RotationFileHandle &operator=(RotationFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = nullptr;
    termination_caller_ = std::move(rhs.termination_caller_);
    return *this;
  }
  ~RotationFileHandle() {
    // Only invoke termination functor when handler at a valid state.
    if (write_handle_ != nullptr) {
      termination_caller_();
    }
  }

  HANDLE GetWriteHandle() const { return write_handle_; }

#endif

 private:
  MEMFD_TYPE_NON_UNIQUE write_handle_;

  // Termination hook, used to flush and call completion.
  std::function<void()> termination_caller_;
};

// This function creates a pipe so applications could write to.
// There're will be two threads spawned in the background, one thread continuously reads
// from the pipe, another one dumps whatever read from pipe to the persistent file.
//
// The returned [RotationFileHandle] owns the lifecycle for pipe file descriptors and
// background threads, the resource release happens at token's destruction.
//
// @param on_completion: called after all content has been persisted.
// @return pipe stream token, so application could stream content into, and synchronize on
// the destruction.
//
// Notice caller side should _NOT_ close the given file handle, it will be handled
// internally.
RotationFileHandle CreatePipeAndStreamOutput(const std::string &fname,
                                             const LogRotationOption &log_rotate_opt,
                                             std::function<void()> on_completion);

}  // namespace ray
