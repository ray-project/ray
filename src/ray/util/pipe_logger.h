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

// Util on logging with redirection, which supports a few logging features, i.e., log
// rotation, multiple sinks for logging, etc.

#pragma once

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "ray/util/compat.h"
#include "ray/util/stream_redirection_options.h"
#include "spdlog/logger.h"

namespace ray {

// File handle requires active destruction via owner calling [Close].
class RedirectionFileHandle {
 public:
  RedirectionFileHandle() = default;

  RedirectionFileHandle(int write_fd,
                        std::shared_ptr<spdlog::logger> logger,
                        std::function<void()> close_fn)
      : write_fd_(write_fd), logger_(std::move(logger)), close_fn_(std::move(close_fn)) {}
  RedirectionFileHandle(const RedirectionFileHandle &) = delete;
  RedirectionFileHandle &operator=(const RedirectionFileHandle &) = delete;
  ~RedirectionFileHandle() = default;

  RedirectionFileHandle(RedirectionFileHandle &&rhs) {
    write_fd_ = rhs.write_fd_;
    rhs.write_fd_ = -1;
    logger_ = std::move(rhs.logger_);
    close_fn_ = std::move(rhs.close_fn_);
  }
  RedirectionFileHandle &operator=(RedirectionFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    write_fd_ = rhs.write_fd_;
    rhs.write_fd_ = -1;
    logger_ = std::move(rhs.logger_);
    close_fn_ = std::move(rhs.close_fn_);
    return *this;
  }
  void Close() {
    if (write_fd_ != -1) {
      close_fn_();

      // Destruct all resources.
      write_fd_ = -1;
      logger_ = nullptr;
      close_fn_ = nullptr;
    }
  }

  // Synchronously flush content to storage.
  //
  // TODO(hjiang): Current method only flushes whatever we send to logger, but not those
  // in the pipe; a better approach is flush pipe, send FLUSH indicator and block wait
  // until logger sync over.
  void Flush() { logger_->flush(); }

  int GetWriteFd() const { return write_fd_; }

  // Write the given data into redirection handle; currently only for testing usage.
  void CompleteWrite(const char *data, size_t len) {
    RAY_CHECK_OK(::ray::CompleteWrite(write_fd_, data, len));
  }

 private:
  int write_fd_;

  std::shared_ptr<spdlog::logger> logger_;

  std::function<void()> close_fn_;
};

// This function creates a pipe so applications could write to.
// There're will be two threads spawned in the background, one thread continuously reads
// from the pipe, another one dumps whatever read from pipe to the persistent file.
//
// The returned [RedirectionFileHandle] owns the lifecycle for pipe file descriptors and
// background threads, the resource release happens at token's destruction.
//
// @return pipe stream token, so application could stream content into, flush stream and
// block wait on flush and close completion.
//
// Notice caller side should _NOT_ close the given file handle, it will be handled
// internally.
RedirectionFileHandle CreateRedirectionFileHandle(
    const StreamRedirectionOption &stream_redirect_opt);

}  // namespace ray
