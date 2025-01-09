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

#include "ray/util/pipe_logger.h"

#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <string_view>
#include <thread>

#include "absl/strings/str_split.h"
#include "ray/util/util.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"

namespace ray {

namespace {

// An indicator which represents EOF, so read thread could exit.
const std::string kEofIndicator = GenerateUUIDV4();

// Default pipe log read buffer size.
constexpr size_t kDefaultPipeLogReadBufSize = 1024;

size_t GetPipeLogReadSizeOrDefault() {
  // TODO(hjiang): Write a util function `GetEnvOrDefault`.
  const char *var_value = std::getenv(kPipeLogReadBufSizeEnv.data());
  if (var_value != nullptr) {
    size_t read_buf_size = 0;
    if (absl::SimpleAtoi(var_value, &read_buf_size) && read_buf_size > 0) {
      return read_buf_size;
    }
  }
  return kDefaultPipeLogReadBufSize;
}

// TODO(hjiang): Make thread annotation for std::mutex.
struct StreamDumper {
  std::mutex mu;
  bool stopped = false;
  std::condition_variable cv;
  std::deque<std::string> content;
};

// Read bytes from handle into [data], return number of bytes read.
// If read fails, throw exception.
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
size_t Read(int read_fd, char *data, size_t len) {
  // TODO(hjiang): Notice frequent read could cause performance issue.
  ssize_t bytes_read = read(read_fd, data, len);
  // TODO(hjiang): Add macros which checks for syscalls.
  RAY_CHECK(bytes_read != -1) << "Fails to read from pipe because " << strerror(errno);
  return bytes_read;
}
void CompleteWriteEOFIndicator(int write_fd) {
  ssize_t bytes_written = write(write_fd, kEofIndicator.data(), kEofIndicator.length());
  RAY_CHECK_EQ(bytes_written, kEofIndicator.length());
  bytes_written = write(write_fd, "\n", /*count=*/1);
  RAY_CHECK_EQ(bytes_written, 1);
}
#elif defined(_WIN32)
#include <windows.h>
size_t Read(HANDLE read_handle, char *data, size_t len) {
  DWORD bytes_read = 0;
  BOOL success = ReadFile(read_handle, data, len, &bytes_read, nullptr);
  RAY_CHECK(success) << "Fails to read from pipe.";
  return bytes_read;
}
void CompleteWriteEOFIndicator(HANDLE write_handle) {
  size_t bytes_written = 0;
  BOOL result = WriteFile(
      write_handle, kEofIndicator.c_str(), kEofIndicator.size(), &bytes_written, nullptr);
  RAY_CHECK_EQ(bytes_written, kEofIndicator.size());
  RAY_CHECK(result);
}
#endif

template <typename ReadFunc>
std::shared_ptr<StreamDumper> CreateStreamDumper(ReadFunc read_func,
                                                 std::shared_ptr<spdlog::logger> logger,
                                                 std::function<void()> on_completion) {
  auto stream_dumper = std::make_shared<StreamDumper>();

  // Create two threads, so there's no IO operation within critical section thus no
  // blocking on write.
  std::thread([read_func = std::move(read_func), stream_dumper = stream_dumper]() {
    const size_t buf_size = GetPipeLogReadSizeOrDefault();
    // TODO(hjiang): Should resize without initialization.
    std::string content(buf_size, '\0');
    // Logging are written in lines, `last_line` records part of the strings left in
    // last `read` syscall.
    std::string last_line;

    while (true) {
      size_t bytes_read = read_func(content.data(), content.length());
      std::string_view cur_content{content.data(), bytes_read};
      std::vector<std::string_view> newlines = absl::StrSplit(cur_content, '\n');

      for (int idx = 0; idx < newlines.size() - 1; ++idx) {
        std::string cur_new_line = std::move(last_line);
        cur_new_line += newlines[idx];

        // Reached the end of stream.
        if (cur_new_line == kEofIndicator) {
          std::lock_guard lck(stream_dumper->mu);
          stream_dumper->stopped = true;
          stream_dumper->cv.notify_one();
          return;
        }

        last_line.clear();

        // We only log non-empty lines.
        if (!cur_new_line.empty()) {
          std::lock_guard lck(stream_dumper->mu);
          stream_dumper->content.emplace_back(std::move(cur_new_line));
          stream_dumper->cv.notify_one();
        }
      }

      // Special handle the last segment we've read.
      //
      // Nothing to do if we've read a complete newline.
      if (content.back() == '\n') {
        continue;
      }

      // Otherwise record the newline so we could reuse in the next read iteration.
      last_line += newlines.back();
    }
  }).detach();

  std::thread([stream_dumper = stream_dumper,
               logger = std::move(logger),
               on_completion = std::move(on_completion)]() {
    while (true) {
      std::string curline;
      {
        std::unique_lock lck(stream_dumper->mu);
        stream_dumper->cv.wait(lck, [stream_dumper]() {
          return !stream_dumper->content.empty() || stream_dumper->stopped;
        });

        // Keep logging until all content flushed.
        if (!stream_dumper->content.empty()) {
          curline = std::move(stream_dumper->content.front());
          stream_dumper->content.pop_front();
        } else if (stream_dumper->stopped) {
          logger->flush();
          on_completion();
          return;
        }
      }

      // Perform IO operation out of critical section.
      logger->log(spdlog::level::info, curline);
    }
  }).detach();

  return stream_dumper;
}

std::shared_ptr<spdlog::logger> CreateLogger(const std::string &fname,
                                             const LogRotationOption &log_rotate_opt) {
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      fname, log_rotate_opt.rotation_max_size, log_rotate_opt.rotation_max_file);
  file_sink->set_level(spdlog::level::info);

  auto logger = std::make_shared<spdlog::logger>(fname, std::move(file_sink));
  logger->set_pattern("%v");  // Only message string is logged.
  return logger;
}

}  // namespace

PipeStreamToken CreatePipeAndStreamOutput(const std::string &fname,
                                          const LogRotationOption &log_rotate_opt,
                                          std::function<void()> on_completion) {
#if defined(__APPLE__) || defined(__linux__)
  int pipefd[2] = {0};
  // TODO(hjiang): We shoud have our own syscall macro.
  RAY_CHECK_EQ(pipe(pipefd), 0);
  int read_fd = pipefd[0];
  int write_fd = pipefd[1];

  auto read_func = [read_fd](char *data, size_t len) { return Read(read_fd, data, len); };
  auto termination_caller = [write_fd]() {
    CompleteWriteEOFIndicator(write_fd);
    RAY_CHECK_EQ(close(write_fd), 0);
  };

#elif defined(_WIN32)
  SECURITY_ATTRIBUTES sa;
  sa.nLength = sizeof(SECURITY_ATTRIBUTES);
  sa.lpSecurityDescriptor = nullptr;
  sa.bInheritHandle = TRUE;

  HANDLE read_handle;
  HANDLE write_handle;
  RAY_CHECK(CreatePipe(&read_handle, &write_handle, &sa, 0));

  auto read_func = [read_handle](char *data, size_t len) {
    return Read(read_handle, data, len);
  } auto termination_caller = [write_handle]() {
    CompleteWriteEOFIndicator(write_handle);
  };

#endif

  auto logger = CreateLogger(fname, log_rotate_opt);
  CreateStreamDumper(std::move(read_func), logger, std::move(on_completion));

  PipeStreamToken stream_token;
  stream_token.termination_caller = std::move(termination_caller);

#if defined(__APPLE__) || defined(__linux__)
  stream_token.write_fd = write_fd;
#else  // __linux__
  stream_token.write_handle = write_handle;
#endif

  return stream_token;
}

}  // namespace ray
