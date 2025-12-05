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

#include <cstdint>
#include <string>
#include <vector>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/// Anonymous pipe for IPC. API mirrors python/ray/_private/pipe.py.
class Pipe {
 public:
  Pipe();
  ~Pipe();

  Pipe(const Pipe &) = delete;
  Pipe &operator=(const Pipe &) = delete;

  Pipe(Pipe &&other) noexcept;
  Pipe &operator=(Pipe &&other) noexcept;

  /// Wrap an external writer handle. On POSIX it's fd, on Windows it's HANDLE.
  static Pipe FromWriterHandle(intptr_t writer_handle);

  /// Wrap an external reader handle. On POSIX it's fd, on Windows it's HANDLE.
  static Pipe FromReaderHandle(intptr_t reader_handle);

  /// Get inheritable read handle for subprocess. Call CloseReaderHandle() after fork.
  intptr_t MakeReaderHandle();

  /// Get inheritable write handle for subprocess. Call CloseWriterHandle() after fork.
  intptr_t MakeWriterHandle();

  /// Read from pipe with timeout. Returns data or error Status on timeout/EOF.
  StatusOr<std::string> Read(int timeout_s = 30, size_t chunk_size = 64);

  /// Write data to the pipe. Returns Status on error.
  Status Write(const std::string &payload);

  void CloseReaderHandle();
  void CloseWriterHandle();
  void Close();

  /// Parse comma-separated handles and set close-on-exec.
  static std::vector<intptr_t> ParseHandlesAndSetCloexec(const std::string &handles_str);

 private:
  void ResetFd(int &fd);
  void MoveFrom(Pipe &&other) noexcept;

  /// Set FD_CLOEXEC flag so fd is NOT inherited by child processes.
  static void SetFdCloseOnExec(int fd);

  /// Clear FD_CLOEXEC flag so fd IS inherited by child processes.
  static void ClearFdCloseOnExec(int fd);

  int read_fd_{-1};
  int write_fd_{-1};
  int read_fd_for_close_{-1};
  int write_fd_for_close_{-1};
};

}  // namespace ray
