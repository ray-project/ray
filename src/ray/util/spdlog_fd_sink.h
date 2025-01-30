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

#pragma once

#include <spdlog/sinks/base_sink.h>

#include "ray/util/compat.h"
#include "ray/util/util.h"

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace spdlog::sinks {

// A sink which logs to the file descriptor.
template <typename Mutex>
class fd_sink final : public base_sink<Mutex> {
 public:
  // [fd] is not owned by [FdSink], which means the file descriptor should be closed by
  // caller.
  explicit fd_sink(int fd) : fd_(fd) {}

 protected:
  void sink_it_(const details::log_msg &msg) override {
    memory_buf_t formatted;
    base_sink<Mutex>::formatter_->format(msg, formatted);

#if defined(__APPLE__) || defined(__linux__)
    RAY_CHECK_EQ(write(fd_, formatted.data(), formatted.size()), formatted.size())
        << "Fails to write because " << strerror(errno);
#elif defined(_WIN32)
    LPDWORD bytes_written;
    BOOL success =
        WriteFile(fd_, formatted.data(), (DWORD)formatted.size(), &bytes_written, NULL);
    RAY_CHECK(success);
    RAY_CHECK_EQ((LPDWORD)formatted.size(), bytes_written);
#endif
  }
  void flush_() override {
#if defined(__APPLE__) || defined(__linux__)
    RAY_CHECK_EQ(close(fd_), 0) << "Fails to close file because " << strerror(errno);
#elif defined(_WIN32)
    RAY_CHECK(CloseHandle(fd_));
#endif
  }

 private:
  MEMFD_TYPE_NON_UNIQUE fd_;
};

using fd_sink_mt = fd_sink<std::mutex>;
using fd_sink_st = fd_sink<details::null_mutex>;

}  // namespace spdlog::sinks
