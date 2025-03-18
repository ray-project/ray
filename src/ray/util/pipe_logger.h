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

#include "ray/util/compat.h"
#include "ray/util/stream_redirection_options.h"
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

// File handle requires active destruction via owner calling [Close].
//
// TODO(hjiang): Wrap fd with spdlog sink to manage stream flush and close.
class RedirectionFileHandle {
 public:
  RedirectionFileHandle() = default;

  // @param termination_synchronizer is used to block wait until destruction operation
  // finishes.
  RedirectionFileHandle(
      MEMFD_TYPE_NON_UNIQUE write_handle,
      std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>
          pipe_ostream,
      std::function<void()> flush_fn,
      std::function<void()> close_fn)
      : write_handle_(write_handle),
        pipe_ostream_(std::move(pipe_ostream)),
        flush_fn_(std::move(flush_fn)),
        close_fn_(std::move(close_fn)) {
    RAY_CHECK(flush_fn_);
    RAY_CHECK(close_fn_);
  }
  RedirectionFileHandle(const RedirectionFileHandle &) = delete;
  RedirectionFileHandle &operator=(const RedirectionFileHandle &) = delete;
  ~RedirectionFileHandle() = default;

  RedirectionFileHandle(RedirectionFileHandle &&rhs) {
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = INVALID_FD;
    pipe_ostream_ = std::move(rhs.pipe_ostream_);
    flush_fn_ = std::move(rhs.flush_fn_);
    close_fn_ = std::move(rhs.close_fn_);
  }
  RedirectionFileHandle &operator=(RedirectionFileHandle &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    write_handle_ = rhs.write_handle_;
    rhs.write_handle_ = INVALID_FD;
    pipe_ostream_ = std::move(rhs.pipe_ostream_);
    flush_fn_ = std::move(rhs.flush_fn_);
    close_fn_ = std::move(rhs.close_fn_);
    return *this;
  }
  void Close() {
    if (write_handle_ != INVALID_FD) {
      close_fn_();
      write_handle_ = INVALID_FD;

      // Unset flush and close functor to close logger and underlying file handle.
      flush_fn_ = nullptr;
      close_fn_ = nullptr;
    }
  }

  // Synchronously flush content to storage.
  //
  // TODO(hjiang): Current method only flushes whatever we send to logger, but not those
  // in the pipe; a better approach is flush pipe, send FLUSH indicator and block wait
  // until logger sync over.
  void Flush() {
    RAY_CHECK(flush_fn_);
    flush_fn_();
  }

  MEMFD_TYPE_NON_UNIQUE GetWriteHandle() const { return write_handle_; }

  // Write the given data into redirection handle; currently only for testing usage.
  void CompleteWrite(const char *data, size_t len) { pipe_ostream_->write(data, len); }

 private:
  MEMFD_TYPE_NON_UNIQUE write_handle_;

  // A high-level wrapper for [write_handle_].
  std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>
      pipe_ostream_;

  // Used to flush log message.
  std::function<void()> flush_fn_;

  // Used to close write handle, and block until destruction completes
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
