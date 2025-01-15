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
#include <iostream>
#include <mutex>
#include <string_view>
#include <thread>

#include "absl/strings/str_split.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace ray {

namespace {
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
size_t Read(int read_fd, char *data, size_t len) {
  // TODO(hjiang): Notice frequent read could cause performance issue.
  ssize_t bytes_read = read(read_fd, data, len);
  // TODO(hjiang): Add macros which checks for syscalls.
  RAY_CHECK(bytes_read != -1) << "Fails to read from pipe because " << strerror(errno);
  return bytes_read;
}
#elif defined(_WIN32)
size_t Read(HANDLE read_handle, char *data, size_t len) {
  DWORD bytes_read = 0;
  BOOL success = ReadFile(read_handle, data, len, &bytes_read, nullptr);
  RAY_CHECK(success) << "Fails to read from pipe.";
  return bytes_read;
}
#endif

template <typename ReadFunc>
std::shared_ptr<StreamDumper> CreateStreamDumper(ReadFunc read_func,
                                                 std::function<void()> close_read_handle,
                                                 std::shared_ptr<spdlog::logger> logger,
                                                 std::function<void()> on_completion) {
  auto stream_dumper = std::make_shared<StreamDumper>();

  // Create two threads, so there's no IO operation within critical section thus no
  // blocking on write.
  std::thread([read_func = std::move(read_func),
               close_read_handle = std::move(close_read_handle),
               stream_dumper = stream_dumper]() {
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

      for (size_t idx = 0; idx < newlines.size() - 1; ++idx) {
        std::string cur_new_line = std::move(last_line);
        cur_new_line += newlines[idx];

        // Reached the end of stream.
        if (cur_new_line.empty()) {
          {
            std::lock_guard lck(stream_dumper->mu);
            stream_dumper->stopped = true;
            stream_dumper->cv.notify_one();
          }

          // Place IO operation out of critical section.
          close_read_handle();

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

// Create a spdlog logger with all sinks specified by the given option.
std::shared_ptr<spdlog::logger> CreateLogger(
    const LogRedirectionOption &log_redirect_opt) {
  std::vector<spdlog::sink_ptr> logging_sinks;
  if (log_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max()) {
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        log_redirect_opt.file_path,
        log_redirect_opt.rotation_max_size,
        log_redirect_opt.rotation_max_file_count);
    file_sink->set_level(spdlog::level::info);
    logging_sinks.emplace_back(std::move(file_sink));
  }
  if (log_redirect_opt.tee_to_stdout) {
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    stdout_sink->set_level(spdlog::level::info);
    logging_sinks.emplace_back(std::move(stdout_sink));
  }
  if (log_redirect_opt.tee_to_stderr) {
    auto stderr_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    stderr_sink->set_level(spdlog::level::info);
    logging_sinks.emplace_back(std::move(stderr_sink));
  }
  auto logger = std::make_shared<spdlog::logger>(
      /*name=*/absl::StrFormat("pipe-logger-%s", log_redirect_opt.file_path),
      std::make_move_iterator(logging_sinks.begin()),
      std::make_move_iterator(logging_sinks.end()));
  logger->set_level(spdlog::level::info);
  logger->set_pattern("%v");  // Only message string is logged.
  return logger;
}

// Pipe streamer is only used in certain cases:
// 1. Log roration is requested;
// 2. Multiple sinks are involved.
bool ShouldUsePipeStream(const LogRedirectionOption &log_redirect_opt) {
  const bool need_rotation =
      log_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max();
  return need_rotation || log_redirect_opt.tee_to_stdout ||
         log_redirect_opt.tee_to_stderr;
}

// TODO(hjiang): Implement open file for windows.
#if defined(__APPLE__) || defined(__linux__)
RedirectionFileHandle OpenFileForRedirection(const std::string &file_path,
                                             std::function<void()> on_completion) {
  int fd = open(file_path.data(), O_WRONLY | O_CREAT, 0644);
  RAY_CHECK_NE(fd, -1) << "Fails to open file " << file_path << " with failure reason "
                       << strerror(errno);

  auto termination_caller = [fd, on_completion = std::move(on_completion)]() {
    RAY_CHECK_EQ(fsync(fd), 0) << "Fails to flush data to disk because "
                               << strerror(errno);
    if (close(fd) != 0) {
      RAY_LOG(ERROR) << "Fails to close redirection file because " << strerror(errno);
    }
    on_completion();
  };

  return RedirectionFileHandle{fd, /*logger=*/nullptr, std::move(termination_caller)};
}
#elif defined(_WIN32)
#include <windows.h>
RedirectionFileHandle OpenFileForRedirection(const std::string &file_path,
                                             std::function<void()> on_completion) {
  // Convert std::string to LPCWSTR (Windows-style wide string)
  std::wstring wide_file_path(file_path.begin(), file_path.end());

  HANDLE file_handle =
      CreateFileW(wide_file_path.c_str(),
                  GENERIC_WRITE,
                  0,              // No sharing
                  NULL,           // Default security attributes
                  CREATE_ALWAYS,  // Create new file or overwrite existing
                  FILE_ATTRIBUTE_NORMAL,
                  NULL);  // No template file

  if (file_handle == INVALID_HANDLE_VALUE) {
    DWORD error_code = GetLastError();
    throw std::runtime_error("Fails to open file " + file_path +
                             " with failure reason: " + std::to_string(error_code));
  }

  auto termination_caller = [file_handle, on_completion = std::move(on_completion)]() {
    if (!FlushFileBuffers(file_handle)) {
      DWORD error_code = GetLastError();
      throw std::runtime_error("Fails to flush data to disk because " +
                               std::to_string(error_code));
    }

    if (!CloseHandle(file_handle)) {
      DWORD error_code = GetLastError();
      throw std::runtime_error("Fails to close redirection file because " +
                               std::to_string(error_code));
    }

    on_completion();
  };

  return RedirectionFileHandle{reinterpret_cast<intptr_t>(file_handle),
                               /*logger=*/nullptr,
                               std::move(termination_caller)};
}
#endif

}  // namespace

RedirectionFileHandle CreatePipeAndStreamOutput(
    const LogRedirectionOption &log_redirect_opt, std::function<void()> on_completion) {
  const bool should_use_pipe_stream = ShouldUsePipeStream(log_redirect_opt);
  if (!should_use_pipe_stream) {
    return OpenFileForRedirection(log_redirect_opt.file_path, std::move(on_completion));
  }

#if defined(__APPLE__) || defined(__linux__)
  int pipefd[2] = {0};
  // TODO(hjiang): We shoud have our own syscall macro.
  RAY_CHECK_EQ(pipe(pipefd), 0);
  int read_fd = pipefd[0];
  int write_fd = pipefd[1];

  auto read_func = [read_fd](char *data, size_t len) { return Read(read_fd, data, len); };
  auto close_read_handle = [read_fd]() { RAY_CHECK_EQ(close(read_fd), 0); };
  auto termination_caller = [write_fd]() {
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
  };
  auto close_read_handle = [read_handle]() { RAY_CHECK(CloseHandle(read_handle)); };
  auto termination_caller = [write_handle, read_handle]() {
    RAY_CHECK(CloseHandle(write_handle));
  };

#endif

  auto logger = CreateLogger(log_redirect_opt);
  CreateStreamDumper(std::move(read_func),
                     std::move(close_read_handle),
                     logger,
                     std::move(on_completion));

#if defined(__APPLE__) || defined(__linux__)
  RedirectionFileHandle stream_token{
      write_fd, std::move(logger), std::move(termination_caller)};
#elif defined(_WIN32)
  RedirectionFileHandle stream_token{
      write_handle, std::move(logger), std::move(termination_caller)};
#endif

  return stream_token;
}

}  // namespace ray
