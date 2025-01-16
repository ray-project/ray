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
#include <future>
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

// TODO(hjiang): Investigate how we could notify reader thread to stop by close write pipe
// directly, instead of relying on eof indicator.
//
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

struct StreamDumper {
  absl::Mutex mu;
  bool stopped ABSL_GUARDED_BY(mu) = false;
  std::deque<std::string> content ABSL_GUARDED_BY(mu);
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
void CompleteWriteEOFIndicator(int write_fd) {
  ssize_t bytes_written = write(write_fd, kEofIndicator.data(), kEofIndicator.length());
  RAY_CHECK_EQ(bytes_written, static_cast<ssize_t>(kEofIndicator.length()));
  bytes_written = write(write_fd, "\n", /*count=*/1);
  RAY_CHECK_EQ(bytes_written, 1);
}
#endif

template <typename ReadFunc, typename WriteFunc, typename FlushFunc>
void StartStreamDump(ReadFunc read_func,
                     WriteFunc write_func,
                     FlushFunc flush_func,
                     std::function<void()> close_read_handle,
                     std::function<void()> on_close_completion) {
  auto stream_dumper = std::make_shared<StreamDumper>();

  // Create two threads, so there's no IO operation within critical section thus no
  // blocking on write.
  std::thread([read_func = std::move(read_func),
               close_read_handle = std::move(close_read_handle),
               stream_dumper = stream_dumper]() {
    SetThreadName("PipeReaderThd");

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
        if (cur_new_line == kEofIndicator) {
          {
            absl::MutexLock lock(&stream_dumper->mu);
            stream_dumper->stopped = true;
          }

          // Place IO operation out of critical section.
          close_read_handle();

          return;
        }

        last_line.clear();

        // We only log non-empty lines.
        if (!cur_new_line.empty()) {
          absl::MutexLock lock(&stream_dumper->mu);
          stream_dumper->content.emplace_back(std::move(cur_new_line));
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
               write_func = std::move(write_func),
               flush_func = std::move(flush_func),
               on_close_completion = std::move(on_close_completion)]() {
    SetThreadName("PipeDumpThd");

    while (true) {
      std::string curline;
      {
        auto has_new_content_or_stopped =
            [stream_dumper]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(stream_dumper->mu) {
              return !stream_dumper->content.empty() || stream_dumper->stopped;
            };

        absl::MutexLock lock(&stream_dumper->mu);
        stream_dumper->mu.Await(absl::Condition(&has_new_content_or_stopped));

        // Keep logging until all content flushed.
        if (!stream_dumper->content.empty()) {
          curline = std::move(stream_dumper->content.front());
          stream_dumper->content.pop_front();
        } else if (stream_dumper->stopped) {
          flush_func();
          on_close_completion();
          return;
        }
      }

      // Perform IO operation out of critical section.
      write_func(curline);
    }
  }).detach();
}

// Create a spdlog logger with all sinks specified by the given option.
std::shared_ptr<spdlog::logger> CreateLogger(
    const StreamRedirectionOption &log_redirect_opt) {
  std::vector<spdlog::sink_ptr> logging_sinks;
  spdlog::sink_ptr file_sink = nullptr;
  if (log_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max()) {
    file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        log_redirect_opt.file_path,
        log_redirect_opt.rotation_max_size,
        log_redirect_opt.rotation_max_file_count);
  } else {
    file_sink =
        std::make_shared<spdlog::sinks::basic_file_sink_st>(log_redirect_opt.file_path);
  }
  file_sink->set_level(spdlog::level::info);
  auto logger = std::make_shared<spdlog::logger>(
      /*name=*/absl::StrFormat("pipe-logger-%s", log_redirect_opt.file_path),
      std::move(file_sink));
  logger->set_level(spdlog::level::info);
  logger->set_pattern("%v");  // Only message string is logged.
  return logger;
}

// Pipe streamer is only used in certain cases:
// 1. Log roration is requested;
// 2. Multiple sinks are involved.
bool ShouldUsePipeStream(const StreamRedirectionOption &log_redirect_opt) {
  const bool need_rotation =
      log_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max();
  return need_rotation || log_redirect_opt.tee_to_stdout ||
         log_redirect_opt.tee_to_stderr;
}

#if defined(__APPLE__) || defined(__linux__)
RedirectionFileHandle OpenFileForRedirection(const std::string &file_path) {
  int fd = open(file_path.data(), O_WRONLY | O_CREAT, 0644);
  RAY_CHECK_NE(fd, -1) << "Fails to open file " << file_path << " with failure reason "
                       << strerror(errno);

  auto flush_fn = [fd]() {
    RAY_CHECK_EQ(fsync(fd), 0) << "Fails to flush data to disk because "
                               << strerror(errno);
  };
  auto close_fn = [fd]() {
    RAY_CHECK_EQ(fsync(fd), 0) << "Fails to flush data to disk because "
                               << strerror(errno);
    RAY_CHECK_EQ(close(fd), 0) << "Fails to close redirection file because "
                               << strerror(errno);
  };

  return RedirectionFileHandle{fd, std::move(flush_fn), std::move(close_fn)};
}
#endif

}  // namespace

#if defined(__APPLE__) || defined(__linux__)
RedirectionFileHandle CreateRedirectionFileHandle(
    const StreamRedirectionOption &log_redirect_opt, const StdStreamFd &std_stream_fd) {
  // Case-1: only redirection, but not rotation and tee involved.
  const bool should_use_pipe_stream = ShouldUsePipeStream(log_redirect_opt);
  if (!should_use_pipe_stream) {
    return OpenFileForRedirection(log_redirect_opt.file_path);
  }

  // Case-2: redirection with rotation, or tee is involved.
  //
  // Used to synchronize on asynchronous stream logging.
  // Shared pointer is used here to workaround the known limitation `std::function`
  // requires captured to be copy constructible.
  auto promise = std::make_shared<std::promise<void>>();
  // Invoked after flush and close finished.
  auto on_close_completion = [promise = promise]() { promise->set_value(); };

  // TODO(hjiang): Use `boost::iostreams` to represent pipe write fd, which supports
  // cross-platform and line-wise read.
  int pipefd[2] = {0};
  // TODO(hjiang): We shoud have our own syscall macro.
  RAY_CHECK_EQ(pipe(pipefd), 0);
  int read_fd = pipefd[0];
  int write_fd = pipefd[1];

  auto read_func = [read_fd](char *data, size_t len) { return Read(read_fd, data, len); };
  auto close_read_handle = [read_fd]() { RAY_CHECK_EQ(close(read_fd), 0); };
  auto close_fn = [write_fd, promise]() {
    CompleteWriteEOFIndicator(write_fd);
    RAY_CHECK_EQ(close(write_fd), 0);
    // Block until destruction finishes.
    promise->get_future().get();
  };

  auto logger = CreateLogger(log_redirect_opt);

  // [content] doesn't have trailing newliner.
  auto write_fn = [logger,
                   log_redirect_opt = log_redirect_opt,
                   std_stream_fd = std_stream_fd](const std::string &content) {
    if (logger != nullptr) {
      logger->log(spdlog::level::info, content);
    }
    if (log_redirect_opt.tee_to_stdout) {
      RAY_CHECK_EQ(write(std_stream_fd.stdout_fd, content.data(), content.length()),
                   content.length());
      RAY_CHECK_EQ(write(std_stream_fd.stdout_fd, "\n", 1), 1);
    }
    if (log_redirect_opt.tee_to_stderr) {
      RAY_CHECK_EQ(write(std_stream_fd.stderr_fd, content.data(), content.length()),
                   content.length());
      RAY_CHECK_EQ(write(std_stream_fd.stderr_fd, "\n", 1), 1);
    }
  };
  auto flush_fn =
      [logger, log_redirect_opt = log_redirect_opt, std_stream_fd = std_stream_fd]() {
        if (logger != nullptr) {
          logger->flush();
        }
        if (log_redirect_opt.tee_to_stdout) {
          fsync(std_stream_fd.stdout_fd);
        }
      };

  StartStreamDump(std::move(read_func),
                  std::move(write_fn),
                  flush_fn,
                  std::move(close_read_handle),
                  std::move(on_close_completion));

  RedirectionFileHandle redirection_file_handle{
      write_fd, std::move(flush_fn), std::move(close_fn)};

  return redirection_file_handle;
}

#elif defined(_WIN32)
RedirectionFileHandle CreateRedirectionFileHandle(
    const StreamRedirectionOption &log_redirect_opt, const StdStreamFd &std_stream_fd) {
  return RedirectionFileHandle{};
}
#endif

}  // namespace ray
