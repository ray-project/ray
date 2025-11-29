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
#include <memory>
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

namespace pipe_internal {

inline double CurrentMonotonicSeconds() {
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

inline void CloseFd(int fd) {
  if (fd < 0) return;
#ifdef _WIN32
  _close(fd);
#else
  close(fd);
#endif
}

/// Set close-on-exec flag so fd is NOT inherited by child processes.
inline void SetFdCloseOnExec(int fd) {
#ifndef _WIN32
  int flags = fcntl(fd, F_GETFD);
  if (flags >= 0) {
    fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
  }
#else
  // On Windows, handles are not inherited by default unless marked inheritable.
  intptr_t handle = _get_osfhandle(fd);
  if (handle != -1) {
    SetHandleInformation(reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, 0);
  }
#endif
}

/// Clear close-on-exec flag so fd IS inherited by child processes.
inline void ClearFdCloseOnExec(int fd) {
#ifndef _WIN32
  int flags = fcntl(fd, F_GETFD);
  if (flags >= 0) {
    fcntl(fd, F_SETFD, flags & ~FD_CLOEXEC);
  }
#else
  // On Windows, mark handle as inheritable.
  intptr_t handle = _get_osfhandle(fd);
  if (handle != -1) {
    SetHandleInformation(
        reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
  }
#endif
}

/// Read data from a pipe file descriptor with timeout.
inline std::string ReadFromFd(int fd, int timeout_s = 30, size_t chunk_size = 64) {
  RAY_CHECK(fd >= 0) << "Attempting to read from an invalid pipe fd.";
  const double deadline = static_cast<double>(timeout_s) + CurrentMonotonicSeconds();
  std::vector<char> buffer;
  buffer.reserve(chunk_size);

  while (CurrentMonotonicSeconds() < deadline) {
    const double remaining = std::max(0.0, deadline - CurrentMonotonicSeconds());
#ifdef _WIN32
    DWORD bytes_avail = 0;
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (!PeekNamedPipe(h, nullptr, 0, nullptr, &bytes_avail, nullptr)) {
      const DWORD err = GetLastError();
      if (err == ERROR_BROKEN_PIPE) {
        break;  // Writer closed
      } else {
        RAY_LOG(FATAL) << "PeekNamedPipe failed, err=" << err;
      }
    }
    if (bytes_avail == 0) {
      Sleep(static_cast<DWORD>(std::min(50.0, remaining * 1000)));
      continue;
    }
    const size_t to_read = std::min<size_t>(chunk_size, bytes_avail);
    std::vector<char> chunk(to_read);
    int n = _read(fd, chunk.data(), static_cast<unsigned int>(to_read));
    if (n <= 0) {
      break;
    }
#else
    timeval tv;
    tv.tv_sec = static_cast<int>(remaining);
    tv.tv_usec = static_cast<int>((remaining - tv.tv_sec) * 1e6);
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    int ready = select(fd + 1, &rfds, nullptr, nullptr, &tv);
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
    ssize_t n = read(fd, chunk.data(), chunk.size());
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

}  // namespace pipe_internal

// Forward declarations
class PipeReader;

/// Anonymous pipe pair for passing data between processes.
/// API mirrors python/ray/_private/pipe.py to keep behavior consistent.
///
/// Use cases:
/// 1. Parent creates pipe, two child processes communicate:
///    - One child reads (e.g., ray_client_server)
///    - Another child writes (e.g., Runtime Env Agent)
///
/// 2. Parent creates pipe, parent reads, child writes:
///    - Parent reads (e.g., node_manager reading agent port)
///    - Child writes (e.g., runtime_env_agent)
///
/// Example (parent reads):
///   PipePair pipe;
///   intptr_t writer_handle = pipe.MakeWriterHandle();
///   StartChildProcess(..., writer_handle);
///   pipe.CloseWriterHandle();  // Close parent's copy after child inherits
///   std::string data = pipe.MakeReader()->Read();
class PipePair {
 public:
  PipePair() {
#ifdef _WIN32
    int fds[2];
    int rc = _pipe(fds, 0, _O_BINARY);
    if (rc != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    // On Windows, pipes are NOT inheritable by default. We only make them
    // inheritable when MakeReaderHandle()/MakeWriterHandle() is called.
#else
    int fds[2];
    if (pipe(fds) != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    // On POSIX, pipe() creates fds without FD_CLOEXEC, meaning they would be
    // inherited by child processes. We set FD_CLOEXEC here so that they are
    // NOT inherited by default. Only when MakeReaderHandle()/MakeWriterHandle()
    // is called do we clear FD_CLOEXEC to allow inheritance.
    for (int fd : {read_fd_, write_fd_}) {
      pipe_internal::SetFdCloseOnExec(fd);
    }
#endif
  }

  ~PipePair() { Close(); }

  PipePair(const PipePair &) = delete;
  PipePair &operator=(const PipePair &) = delete;

  PipePair(PipePair &&other) noexcept { MoveFrom(std::move(other)); }
  PipePair &operator=(PipePair &&other) noexcept {
    if (this != &other) {
      Close();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  /// Get read handle to pass to a subprocess.
  /// After subprocess starts, call CloseReaderHandle() to close the parent's copy.
  /// This clears FD_CLOEXEC so the fd can be inherited by child processes.
  /// @return The read handle (fd on POSIX, HANDLE on Windows).
  intptr_t MakeReaderHandle() {
    RAY_CHECK(read_fd_ >= 0) << "read_fd already taken or closed";
    int fd = read_fd_;
    read_fd_for_close_ = fd;
    read_fd_ = -1;
#ifdef _WIN32
    // Make handle inheritable on Windows
    pipe_internal::ClearFdCloseOnExec(fd);
    return _get_osfhandle(fd);
#else
    // Clear FD_CLOEXEC so child can inherit this fd
    pipe_internal::ClearFdCloseOnExec(fd);
    return fd;
#endif
  }

  /// Get write handle to pass to a subprocess.
  /// After subprocess starts, call CloseWriterHandle() to close the parent's copy.
  /// This clears FD_CLOEXEC so the fd can be inherited by child processes.
  /// @return The write handle (fd on POSIX, HANDLE on Windows).
  intptr_t MakeWriterHandle() {
    RAY_CHECK(write_fd_ >= 0) << "write_fd already taken or closed";
    int fd = write_fd_;
    write_fd_for_close_ = fd;
    write_fd_ = -1;
#ifdef _WIN32
    // Make handle inheritable on Windows
    pipe_internal::ClearFdCloseOnExec(fd);
    return _get_osfhandle(fd);
#else
    // Clear FD_CLOEXEC so child can inherit this fd
    pipe_internal::ClearFdCloseOnExec(fd);
    return fd;
#endif
  }

  /// Create a PipeReader for the parent process to read.
  /// The returned PipeReader owns the fd and will close it.
  std::unique_ptr<PipeReader> MakeReader();

  /// Close the parent's copy of the read fd after subprocess inherits it.
  void CloseReaderHandle() {
    if (read_fd_for_close_ >= 0) {
      pipe_internal::CloseFd(read_fd_for_close_);
      read_fd_for_close_ = -1;
    }
  }

  /// Close the parent's copy of the write fd after subprocess inherits it.
  void CloseWriterHandle() {
    if (write_fd_for_close_ >= 0) {
      pipe_internal::CloseFd(write_fd_for_close_);
      write_fd_for_close_ = -1;
    }
  }

  /// Close any remaining fds (cleanup).
  void Close() {
    if (read_fd_ >= 0) {
      pipe_internal::CloseFd(read_fd_);
      read_fd_ = -1;
    }
    if (write_fd_ >= 0) {
      pipe_internal::CloseFd(write_fd_);
      write_fd_ = -1;
    }
    CloseReaderHandle();
    CloseWriterHandle();
  }

 private:
  void MoveFrom(PipePair &&other) noexcept {
    read_fd_ = other.read_fd_;
    write_fd_ = other.write_fd_;
    read_fd_for_close_ = other.read_fd_for_close_;
    write_fd_for_close_ = other.write_fd_for_close_;
    other.read_fd_ = -1;
    other.write_fd_ = -1;
    other.read_fd_for_close_ = -1;
    other.write_fd_for_close_ = -1;
  }

  int read_fd_{-1};
  int write_fd_{-1};
  int read_fd_for_close_{-1};
  int write_fd_for_close_{-1};
};

/// Read helper that accepts an inheritable pipe handle from another process.
class PipeReader {
 public:
  explicit PipeReader(intptr_t pipe_handle) {
#ifdef _WIN32
    RAY_CHECK(pipe_handle != 0 && pipe_handle != -1)
        << "Invalid pipe handle " << pipe_handle;
    fd_ = _open_osfhandle(pipe_handle, _O_RDONLY | _O_BINARY);
    if (fd_ == -1) {
      RAY_LOG(FATAL) << "Failed to open pipe handle " << pipe_handle
                     << ", errno=" << errno;
    }
#else
    fd_ = static_cast<int>(pipe_handle);
    RAY_CHECK_GE(fd_, 0) << "Invalid pipe fd";
#endif
  }

  ~PipeReader() { Close(); }

  PipeReader(const PipeReader &) = delete;
  PipeReader &operator=(const PipeReader &) = delete;

  PipeReader(PipeReader &&other) noexcept : fd_(other.fd_), closed_(other.closed_) {
    other.fd_ = -1;
    other.closed_ = true;
  }

  PipeReader &operator=(PipeReader &&other) noexcept {
    if (this != &other) {
      Close();
      fd_ = other.fd_;
      closed_ = other.closed_;
      other.fd_ = -1;
      other.closed_ = true;
    }
    return *this;
  }

  /// Read data from the pipe.
  /// @param timeout_s How long to wait for data before timing out.
  /// @param chunk_size Size of each read chunk.
  /// @return The data read from the pipe as a string.
  std::string Read(int timeout_s = 30, size_t chunk_size = 64) {
    RAY_CHECK(!closed_) << "Attempting to read from a closed pipe.";
    return pipe_internal::ReadFromFd(fd_, timeout_s, chunk_size);
  }

  void Close() {
    if (closed_) return;
    pipe_internal::CloseFd(fd_);
    fd_ = -1;
    closed_ = true;
  }

 private:
  int fd_{-1};
  bool closed_{false};
};

// Implementation of PipePair::MakeReader (needs PipeReader to be defined first)
inline std::unique_ptr<PipeReader> PipePair::MakeReader() {
  RAY_CHECK(read_fd_ >= 0) << "read_fd already taken or closed";
  int fd = read_fd_;
  read_fd_ = -1;
  return std::make_unique<PipeReader>(static_cast<intptr_t>(fd));
}

/// Write helper that accepts an inheritable pipe handle from another process.
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

  ~PipeWriter() { Close(); }

  PipeWriter(const PipeWriter &) = delete;
  PipeWriter &operator=(const PipeWriter &) = delete;

  PipeWriter(PipeWriter &&other) noexcept : fd_(other.fd_), closed_(other.closed_) {
    other.fd_ = -1;
    other.closed_ = true;
  }

  PipeWriter &operator=(PipeWriter &&other) noexcept {
    if (this != &other) {
      Close();
      fd_ = other.fd_;
      closed_ = other.closed_;
      other.fd_ = -1;
      other.closed_ = true;
    }
    return *this;
  }

  /// Write the full payload to the pipe; fatal on failure.
  void Write(const std::string &payload) {
    RAY_CHECK(!closed_) << "Attempted to write to a closed pipe";
    std::string_view remaining(payload);
    while (!remaining.empty()) {
#ifdef _WIN32
      int n = _write(fd_, remaining.data(), static_cast<unsigned int>(remaining.size()));
      if (n < 0) {
        if (errno == EINTR) continue;
        RAY_LOG(FATAL) << "Failed to write to pipe fd " << fd_ << ", errno=" << errno;
      }
#else
      ssize_t n = write(fd_, remaining.data(), remaining.size());
      if (n < 0) {
        if (errno == EINTR) continue;
        RAY_LOG(FATAL) << "Failed to write to pipe fd " << fd_ << ", errno=" << errno;
      }
#endif
      remaining.remove_prefix(static_cast<size_t>(n));
    }
  }

  void Close() {
    if (closed_) return;
    pipe_internal::CloseFd(fd_);
    fd_ = -1;
    closed_ = true;
  }

 private:
  int fd_{-1};
  bool closed_{false};
};

/// Write to multiple pipe handles simultaneously.
/// This is useful when multiple consumers need to receive the same data,
/// e.g., reporting a port to both Raylet and ray_client_server.
class MultiPipeWriter {
 public:
  /// Initialize with a list of pipe handles.
  /// @param pipe_handles List of pipe handles (fd on POSIX, HANDLE on Windows).
  ///                     Negative values are ignored.
  explicit MultiPipeWriter(const std::vector<intptr_t> &pipe_handles) {
    for (intptr_t h : pipe_handles) {
      if (h >= 0) {
        writers_.emplace_back(h);
      }
    }
  }

  ~MultiPipeWriter() = default;

  MultiPipeWriter(const MultiPipeWriter &) = delete;
  MultiPipeWriter &operator=(const MultiPipeWriter &) = delete;

  MultiPipeWriter(MultiPipeWriter &&) = default;
  MultiPipeWriter &operator=(MultiPipeWriter &&) = default;

  /// Write data to all pipes.
  void Write(const std::string &data) {
    for (auto &writer : writers_) {
      writer.Write(data);
    }
  }

  /// Close all pipe write ends.
  void Close() {
    for (auto &writer : writers_) {
      writer.Close();
    }
  }

 private:
  std::vector<PipeWriter> writers_;
};

}  // namespace ray
