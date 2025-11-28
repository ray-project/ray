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

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>
#endif

#include "ray/util/logging.h"

namespace ray {

// A cross-platform anonymous pipe for passing a short payload from child to parent.
// API mirrors python/ray/_private/pipe.py to keep behavior consistent.
class Pipe {
 public:
  Pipe() {
#ifdef _WIN32
    // Create an inheritable anonymous pipe.
    int fds[2];
    int rc = _pipe(fds, 0, _O_BINARY);
    if (rc != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    // Ensure the write handle is inheritable when passed to children.
    intptr_t handle = _get_osfhandle(write_fd_);
    RAY_CHECK(handle != -1);
    HANDLE h = reinterpret_cast<HANDLE>(handle);
    if (!SetHandleInformation(h, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)) {
      RAY_LOG(FATAL) << "Failed to mark pipe handle inheritable, errno="
                     << GetLastError();
    }
#else
    int fds[2];
    if (pipe(fds) != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    int rc = fcntl(write_fd_, F_SETFD, 0);  // make inheritable
    if (rc != 0) {
      RAY_LOG(FATAL) << "Failed to clear FD_CLOEXEC on write fd, errno=" << errno;
    }
#endif
  }

  ~Pipe() {
    CloseRead();
    CloseWrite();
  }

  Pipe(const Pipe &) = delete;
  Pipe &operator=(const Pipe &) = delete;

  Pipe(Pipe &&other) noexcept { MoveFrom(std::move(other)); }
  Pipe &operator=(Pipe &&other) noexcept {
    if (this != &other) {
      CloseRead();
      CloseWrite();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  int ReadFD() const { return read_fd_; }
  int WriteFD() const { return write_fd_; }

  // Handle value to pass to children (fd on POSIX, HANDLE on Windows).
  intptr_t WriteHandle() const {
#ifdef _WIN32
    return _get_osfhandle(write_fd_);
#else
    return write_fd_;
#endif
  }

  void CloseWrite() {
    if (write_fd_ < 0) {
      return;
    }
#ifdef _WIN32
    _close(write_fd_);
#else
    close(write_fd_);
#endif
    write_fd_ = -1;
  }

  void CloseRead() {
    if (read_fd_ < 0) {
      return;
    }
#ifdef _WIN32
    _close(read_fd_);
#else
    close(read_fd_);
#endif
    read_fd_ = -1;
  }

  // Blocking read until writer closes or timeout. Returns decoded string.
  std::string Read(int timeout_s = 30, size_t chunk_size = 64) {
    RAY_CHECK(read_fd_ >= 0) << "Attempting to read from a closed pipe read end.";
    const double deadline =
        static_cast<double>(timeout_s) + static_cast<double>(CurrentMonotonicSeconds());
    std::vector<char> buffer;
    buffer.reserve(chunk_size);
    while (CurrentMonotonicSeconds() < deadline) {
      const double remaining = std::max(0.0, deadline - CurrentMonotonicSeconds());
#ifdef _WIN32
      DWORD bytes_avail = 0;
      HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(read_fd_));
      if (!PeekNamedPipe(h, nullptr, 0, nullptr, &bytes_avail, nullptr)) {
        const DWORD err = GetLastError();
        if (err == ERROR_BROKEN_PIPE) {
          bytes_avail = 0;
        } else {
          RAY_LOG(FATAL) << "PeekNamedPipe failed, err=" << err;
        }
      }
      if (bytes_avail == 0) {
        // No data yet.
        Sleep(static_cast<DWORD>(std::max(1.0, remaining * 1000)));
        continue;
      }
      const size_t to_read = std::min<size_t>(chunk_size, bytes_avail);
      std::vector<char> chunk(to_read);
      int n = _read(read_fd_, chunk.data(), static_cast<unsigned int>(to_read));
      if (n <= 0) {
        break;
      }
#else
      timeval tv;
      tv.tv_sec = static_cast<int>(remaining);
      tv.tv_usec = static_cast<int>((remaining - tv.tv_sec) * 1e6);
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(read_fd_, &rfds);
      int ready = select(read_fd_ + 1, &rfds, nullptr, nullptr, &tv);
      if (ready == 0) {
        continue;  // timeout, loop again
      }
      if (ready < 0) {
        if (errno == EINTR) {
          continue;
        }
        RAY_LOG(FATAL) << "select failed on pipe read fd, errno=" << errno;
      }
      std::vector<char> chunk(chunk_size);
      ssize_t n = read(read_fd_, chunk.data(), chunk.size());
      if (n <= 0) {
        break;
      }
#endif
      buffer.insert(buffer.end(), chunk.begin(), chunk.begin() + n);
    }

    if (!buffer.empty()) {
      return std::string(buffer.begin(), buffer.end());
    }
    RAY_LOG(FATAL) << "Timed out waiting for data from pipe or got EOF without data.";
    return "";
  }

 private:
  double CurrentMonotonicSeconds() const {
#ifdef _WIN32
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return static_cast<double>(counter.QuadPart) / static_cast<double>(freq.QuadPart);
#else
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
#endif
  }

  void MoveFrom(Pipe &&other) noexcept {
    read_fd_ = other.read_fd_;
    write_fd_ = other.write_fd_;
    other.read_fd_ = -1;
    other.write_fd_ = -1;
  }

  int read_fd_{-1};
  int write_fd_{-1};
};

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
