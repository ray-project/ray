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
#include <chrono>
#include <cstdint>
#include <sstream>
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
#include <unistd.h>
#endif

#include "ray/util/logging.h"

namespace ray {

/// Anonymous pipe for IPC. API mirrors python/ray/_private/pipe.py.
class Pipe {
 public:
  Pipe() {
#ifdef _WIN32
    int fds[2];
    if (_pipe(fds, 0, _O_BINARY) != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
#else
    int fds[2];
    if (pipe(fds) != 0) {
      RAY_LOG(FATAL) << "Failed to create pipe, errno=" << errno;
    }
    read_fd_ = fds[0];
    write_fd_ = fds[1];
    SetFdCloseOnExec(read_fd_);
    SetFdCloseOnExec(write_fd_);
#endif
  }

  /// Wrap an external writer handle. On POSIX it's fd, on Windows it's HANDLE.
  static Pipe FromWriterHandle(intptr_t writer_handle) {
    Pipe p;
    p.Close();
#ifdef _WIN32
    p.write_fd_ = OpenHandleAsFd(writer_handle, _O_BINARY | _O_WRONLY);
#else
    p.write_fd_ = static_cast<int>(writer_handle);
    SetFdCloseOnExec(p.write_fd_);
#endif
    return p;
  }

  /// Wrap an external reader handle. On POSIX it's fd, on Windows it's HANDLE.
  static Pipe FromReaderHandle(intptr_t reader_handle) {
    Pipe p;
    p.Close();
#ifdef _WIN32
    p.read_fd_ = OpenHandleAsFd(reader_handle, _O_BINARY | _O_RDONLY);
#else
    p.read_fd_ = static_cast<int>(reader_handle);
    SetFdCloseOnExec(p.read_fd_);
#endif
    return p;
  }

  ~Pipe() { Close(); }

  Pipe(const Pipe &) = delete;
  Pipe &operator=(const Pipe &) = delete;

  Pipe(Pipe &&other) noexcept { MoveFrom(std::move(other)); }
  Pipe &operator=(Pipe &&other) noexcept {
    if (this != &other) {
      Close();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  /// Get inheritable read handle for subprocess. Call CloseReaderHandle() after fork.
  intptr_t MakeReaderHandle() {
    RAY_CHECK(read_fd_ >= 0) << "read_fd already taken or closed";
    int fd = read_fd_;
    read_fd_for_close_ = fd;
    read_fd_ = -1;
    ClearFdCloseOnExec(fd);
#ifdef _WIN32
    return _get_osfhandle(fd);
#else
    return fd;
#endif
  }

  /// Get inheritable write handle for subprocess. Call CloseWriterHandle() after fork.
  intptr_t MakeWriterHandle() {
    RAY_CHECK(write_fd_ >= 0) << "write_fd already taken or closed";
    int fd = write_fd_;
    write_fd_for_close_ = fd;
    write_fd_ = -1;
    ClearFdCloseOnExec(fd);
#ifdef _WIN32
    return _get_osfhandle(fd);
#else
    return fd;
#endif
  }

  /// Read from pipe with timeout. Returns data or FAILs on timeout/EOF.
  std::string Read(int timeout_s = 30, size_t chunk_size = 64) {
    RAY_CHECK(read_fd_ >= 0) << "read_fd already taken or closed";
    const double deadline = static_cast<double>(timeout_s) + CurrentMonotonicSeconds();
    std::vector<char> buffer;
    buffer.reserve(chunk_size);

    while (true) {
      const double remaining = deadline - CurrentMonotonicSeconds();
      if (remaining <= 0) {
        break;
      }
#ifdef _WIN32
      DWORD bytes_avail = 0;
      HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(read_fd_));
      if (!PeekNamedPipe(h, nullptr, 0, nullptr, &bytes_avail, nullptr)) {
        if (GetLastError() == ERROR_BROKEN_PIPE) break;
        RAY_LOG(FATAL) << "PeekNamedPipe failed, err=" << GetLastError();
      }
      if (bytes_avail == 0) {
        Sleep(static_cast<DWORD>(std::min(50.0, remaining * 1000)));
        continue;
      }
      std::vector<char> chunk(std::min<size_t>(chunk_size, bytes_avail));
      int n = _read(read_fd_, chunk.data(), static_cast<unsigned int>(chunk.size()));
      if (n <= 0) break;
#else
      timeval tv;
      tv.tv_sec = static_cast<int>(remaining);
      tv.tv_usec = static_cast<int>((remaining - tv.tv_sec) * 1e6);
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(read_fd_, &rfds);
      int ready = select(read_fd_ + 1, &rfds, nullptr, nullptr, &tv);
      if (ready == 0) continue;
      if (ready < 0) {
        if (errno == EINTR) continue;
        RAY_LOG(FATAL) << "select failed, errno=" << errno;
      }
      std::vector<char> chunk(chunk_size);
      ssize_t n = read(read_fd_, chunk.data(), chunk.size());
      if (n <= 0) break;
#endif
      buffer.insert(buffer.end(), chunk.begin(), chunk.begin() + n);
    }

    if (!buffer.empty()) {
      return std::string(buffer.begin(), buffer.end());
    }
    RAY_LOG(FATAL) << "Timed out or EOF before any data was read.";
    return "";
  }

  /// Write data to the pipe.
  void Write(const std::string &payload) {
    RAY_CHECK(write_fd_ >= 0) << "write_fd already taken or closed";
    std::string_view remaining(payload);
    while (!remaining.empty()) {
#ifdef _WIN32
      int n = _write(
          write_fd_, remaining.data(), static_cast<unsigned int>(remaining.size()));
#else
      ssize_t n = write(write_fd_, remaining.data(), remaining.size());
#endif
      if (n <= 0) {
        if (n < 0 && errno == EINTR) continue;
        RAY_LOG(FATAL) << "Failed to write to pipe, errno=" << errno;
      }
      remaining.remove_prefix(static_cast<size_t>(n));
    }
  }

  void CloseReaderHandle() { ResetFd(read_fd_for_close_); }
  void CloseWriterHandle() { ResetFd(write_fd_for_close_); }

  void Close() {
    ResetFd(read_fd_);
    ResetFd(write_fd_);
    CloseReaderHandle();
    CloseWriterHandle();
  }

  /// Parse comma-separated handles and set close-on-exec.
  static std::vector<intptr_t> ParseHandlesAndSetCloexec(const std::string &handles_str) {
    std::vector<intptr_t> result;
    if (handles_str.empty()) return result;

    std::stringstream ss(handles_str);
    std::string token;
    while (std::getline(ss, token, ',')) {
      if (token.empty()) continue;
#ifdef _WIN32
      intptr_t handle = static_cast<intptr_t>(std::stoll(token));
      SetHandleInformation(reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, 0);
      result.push_back(handle);
#else
      int fd = std::stoi(token);
      SetFdCloseOnExec(fd);
      result.push_back(static_cast<intptr_t>(fd));
#endif
    }
    return result;
  }

  /// Format a list of handles into a comma-separated string.
  static std::string FormatHandles(const std::vector<intptr_t> &handles) {
    std::string result;
    for (size_t i = 0; i < handles.size(); ++i) {
      if (i > 0) result += ",";
      result += std::to_string(handles[i]);
    }
    return result;
  }

  /// Set FD_CLOEXEC flag so fd is NOT inherited by child processes.
  static void SetFdCloseOnExec(int fd) {
#ifdef _WIN32
    intptr_t handle = _get_osfhandle(fd);
    if (handle != -1) {
      SetHandleInformation(reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, 0);
    }
#else
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0) fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
#endif
  }

  /// Clear FD_CLOEXEC flag so fd IS inherited by child processes.
  static void ClearFdCloseOnExec(int fd) {
#ifdef _WIN32
    intptr_t handle = _get_osfhandle(fd);
    if (handle != -1) {
      SetHandleInformation(
          reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
    }
#else
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0) fcntl(fd, F_SETFD, flags & ~FD_CLOEXEC);
#endif
  }

 private:
  static double CurrentMonotonicSeconds() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
  }

  static void CloseFd(int fd) {
    if (fd < 0) return;
#ifdef _WIN32
    _close(fd);
#else
    close(fd);
#endif
  }

#ifdef _WIN32
  static void SetHandleCloseOnExec(HANDLE handle) {
    if (handle != INVALID_HANDLE_VALUE && handle != nullptr) {
      SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
    }
  }

  static int OpenHandleAsFd(intptr_t handle, int flags) {
    SetHandleCloseOnExec(reinterpret_cast<HANDLE>(handle));
    int fd = _open_osfhandle(handle, flags);
    if (fd < 0) RAY_LOG(WARNING) << "_open_osfhandle failed for handle " << handle;
    return fd;
  }
#endif

  void ResetFd(int &fd) {
    if (fd >= 0) {
      CloseFd(fd);
      fd = -1;
    }
  }

  void MoveFrom(Pipe &&other) noexcept {
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

}  // namespace ray
