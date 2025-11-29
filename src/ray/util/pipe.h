// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cerrno>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <unistd.h>
#endif

#include "ray/util/logging.h"

namespace ray {

class PipeWriter {
 public:
  explicit PipeWriter(intptr_t pipe_handle) {
#ifdef _WIN32
    RAY_CHECK(pipe_handle != 0 && pipe_handle != -1)
        << "Invalid pipe handle " << pipe_handle;
    fd_ = _open_osfhandle(pipe_handle, _O_WRONLY | _O_BINARY);
    if (fd_ == -1) {
      RAY_LOG(FATAL) << "Failed to open pipe handle " << pipe_handle
                     << ", errno=" << errno;
    }
#else
    fd_ = static_cast<int>(pipe_handle);
    RAY_CHECK_GE(fd_, 0) << "Invalid pipe fd";
#endif
  }

  ~PipeWriter() { CloseWrite(); }

  PipeWriter(const PipeWriter &) = delete;
  PipeWriter &operator=(const PipeWriter &) = delete;

  PipeWriter(PipeWriter &&other) noexcept { MoveFrom(std::move(other)); }

  PipeWriter &operator=(PipeWriter &&other) noexcept {
    if (this != &other) {
      CloseWrite();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  // Write the full payload to the pipe; fatal on failure.
  void Write(const std::string &payload) {
    RAY_CHECK(!closed_) << "Attempted to write to a closed pipe";
    std::string_view remaining(payload);
    while (!remaining.empty()) {
#ifdef _WIN32
      int n = _write(fd_, remaining.data(), static_cast<unsigned int>(remaining.size()));
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        RAY_LOG(FATAL) << "Failed to write to pipe fd " << fd_ << ", errno=" << errno;
      }
#else
      ssize_t n = write(fd_, remaining.data(), remaining.size());
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        RAY_LOG(FATAL) << "Failed to write to pipe fd " << fd_ << ", errno=" << errno;
      }
#endif
      remaining.remove_prefix(static_cast<size_t>(n));
    }
  }

  void CloseWrite() {
    if (closed_) {
      return;
    }
#ifdef _WIN32
    _close(fd_);
#else
    close(fd_);
#endif
    fd_ = -1;
    closed_ = true;
  }

 private:
  void MoveFrom(PipeWriter &&other) noexcept {
    fd_ = other.fd_;
    closed_ = other.closed_;
    other.fd_ = -1;
    other.closed_ = true;
  }

  int fd_{-1};
  bool closed_{false};
};

}  // namespace ray
