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
#include <memory>
#include <string>
#include <utility>

#include "ray/util/compat.h"
#include "ray/util/log_redirection_options.h"
#include "ray/util/util.h"
#include "spdlog/logger.h"

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace ray {

// Environmenr variable, which indicates the pipe size of read.
//
// TODO(hjiang): Should document the env variable after end-to-end integration has
// finished.
inline constexpr std::string_view kPipeLogReadBufSizeEnv = "RAY_PIPE_LOG_READ_BUF_SIZE";

class RedirectionFileHandle {
 public:
  RedirectionFileHandle() = default;
  RedirectionFileHandle(MEMFD_TYPE_NON_UNIQUE write_handle,
                        std::shared_ptr<spdlog::logger> logger,
                        std::function<void()> termination_caller)
      : write_handle_(write_handle),
        logger_(std::move(logger)),
        termination_caller_(std::move(termination_caller)) {}
  RedirectionFileHandle(const RedirectionFileHandle &) = delete;
  RedirectionFileHandle &operator=(const RedirectionFileHandle &) = delete;

  // Synchronously flush content to storage.
  //
  // TODO(hjiang): Current method only flushes whatever we send to logger, but not those
  // in the pipe; a better approach is flush pipe, send FLUSH indicator and block wait
  // until logger sync over.
  void Flush() {
    if (logger_ != nullptr) {
      logger_->flush();
    }
  }

  // Used to write to.
  //
  // TODO(hjiang): I will followup with another PR to make a `FD` class, which is not
  // copiable to avoid manual `dup`.
#if defined(__APPLE__) || defined(__linux__)
  RedirectionFileHandle(RedirectionFileHandle &&rhs) {
    logger_ = std::move(rhs.logger_);
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = -1;
    termination_caller_ = std::move(rhs.termination_caller_);
  }
  RedirectionFileHandle &operator=(RedirectionFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    logger_ = std::move(rhs.logger_);
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = -1;
    termination_caller_ = std::move(rhs.termination_caller_);
    return *this;
  }
  void Close() {
    // Only invoke termination functor when handler at a valid state.
    if (write_handle_ != -1) {
      termination_caller_();
    }
  }
  void CompleteWrite(const char *data, size_t len) {
    ssize_t bytes_written = write(write_handle_, data, len);
    RAY_CHECK_EQ(bytes_written, static_cast<ssize_t>(len));
  }

  int GetWriteHandle() const { return write_handle_; }

#elif defined(_WIN32)
  RedirectionFileHandle(RedirectionFileHandle &&rhs) {
    logger_ = std::move(rhs.logger_);
    write_fd_ = rhs.write_fd_;
    rhs.write_fd_ = nullptr;
    termination_caller_ = std::move(rhs.termination_caller_);
  }
  RedirectionFileHandle &operator=(RedirectionFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    logger_ = std::move(rhs.logger_);
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = nullptr;
    termination_caller_ = std::move(rhs.termination_caller_);
    return *this;
  }
  ~RedirectionFileHandle() {
    // Only invoke termination functor when handler at a valid state.
    if (write_handle_ != nullptr) {
      termination_caller_();
    }
  }
  void CompleteWrite(char *data, size_t len) {
    DWORD bytes_written = 0;
    WriteFile(write_handle_, data, len, &bytes_written, nullptr);
  }

  HANDLE GetWriteHandle() const { return write_handle_; }

#endif

 private:
  MEMFD_TYPE_NON_UNIQUE write_handle_;

  // Logger, which is responsible for writing content into all sinks.
  std::shared_ptr<spdlog::logger> logger_;

  // Termination hook, used to flush and call completion.
  std::function<void()> termination_caller_;
};

// This function creates a pipe so applications could write to.
// There're will be two threads spawned in the background, one thread continuously reads
// from the pipe, another one dumps whatever read from pipe to the persistent file.
//
// The returned [RedirectionFileHandle] owns the lifecycle for pipe file descriptors and
// background threads, the resource release happens at token's destruction.
//
// @param on_completion: called after all content has been persisted.
// @return pipe stream token, so application could stream content into, and synchronize on
// the destruction.
//
// Notice caller side should _NOT_ close the given file handle, it will be handled
// internally.
RedirectionFileHandle CreatePipeAndStreamOutput(
    const LogRedirectionOption &log_redirect_opt, std::function<void()> on_completion);

}  // namespace ray
