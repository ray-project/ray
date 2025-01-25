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
#include "ray/util/thread_utils.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace ray {

namespace {

struct StreamDumper {
  absl::Mutex mu;
  bool stopped ABSL_GUARDED_BY(mu) = false;
  std::deque<std::string> content ABSL_GUARDED_BY(mu);
};

// Used to write to dup-ed stdout and stderr; use shared pointer to make it copy
// constructible.
struct StdOstream {
  std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>
      stdout_ostream;
  std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>
      stderr_ostream;
};

// Start two threads:
// 1. A reader thread which continuously reads from [pipe_stream] until close;
// 2. A dumper thread which writes content to sink via [write_func].
template <typename WriteFunc, typename FlushFunc>
void StartStreamDump(
    std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_source>>
        pipe_instream,
    WriteFunc write_func,
    FlushFunc flush_func,
    std::function<void()> on_close_completion) {
  auto stream_dumper = std::make_shared<StreamDumper>();

  // Create two threads, so there's no IO operation within critical section thus no
  // blocking on write.
  std::thread([pipe_instream = std::move(pipe_instream),
               stream_dumper = stream_dumper]() {
    SetThreadName("PipeReaderThd");

    std::string newline;
    while (std::getline(*pipe_instream, newline)) {
      // Backfill newliner for current segment.
      if (!pipe_instream->eof()) {
        newline += '\n';
      }

      absl::MutexLock lock(&stream_dumper->mu);
      stream_dumper->content.emplace_back(std::move(newline));
    }

    {
      absl::MutexLock lock(&stream_dumper->mu);
      stream_dumper->stopped = true;
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
      write_func(std::move(curline));
    }
  }).detach();
}

// Create a spdlog logger with all sinks specified by the given option.
std::shared_ptr<spdlog::logger> CreateLogger(
    const StreamRedirectionOption &stream_redirect_opt) {
  std::vector<spdlog::sink_ptr> logging_sinks;
  spdlog::sink_ptr file_sink = nullptr;
  if (stream_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max()) {
    file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        stream_redirect_opt.file_path,
        stream_redirect_opt.rotation_max_size,
        stream_redirect_opt.rotation_max_file_count);
  } else {
    file_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(
        stream_redirect_opt.file_path);
  }
  file_sink->set_level(spdlog::level::info);
  auto logger = std::make_shared<spdlog::logger>(
      /*name=*/absl::StrFormat("pipe-logger-%s", stream_redirect_opt.file_path),
      std::move(file_sink));
  logger->set_level(spdlog::level::info);
  logger->set_pattern("%v");  // Only message string is logged.
  return logger;
}

// Pipe streamer is only used in certain cases:
// 1. Log roration is requested;
// 2. Multiple sinks are involved.
bool ShouldUsePipeStream(const StreamRedirectionOption &stream_redirect_opt) {
  const bool need_rotation =
      stream_redirect_opt.rotation_max_size != std::numeric_limits<size_t>::max();
  return need_rotation || stream_redirect_opt.tee_to_stdout ||
         stream_redirect_opt.tee_to_stderr;
}

RedirectionFileHandle OpenFileForRedirection(const std::string &file_path) {
  boost::iostreams::file_descriptor_sink sink{file_path, std::ios_base::out};
  auto handle = sink.handle();
  auto ostream =
      std::make_shared<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
          std::move(sink));
  auto flush_fn = [ostream, handle]() {
    // Flush stream internal buffer to fd.
    ostream->flush();
// Flush file handle.
#if defined(__APPLE__) || defined(__linux__)
    RAY_CHECK_EQ(fdatasync(handle), 0);
#elif defined(_WIN32)
    RAY_CHECK(FlushFileBuffers(handle));
#endif
  };
  auto close_fn = [flush_fn, ostream]() {
    flush_fn();
    ostream->close();
  };
  return RedirectionFileHandle{
      handle, std::move(ostream), std::move(flush_fn), std::move(close_fn)};
}
}  // namespace

RedirectionFileHandle CreateRedirectionFileHandle(
    const StreamRedirectionOption &stream_redirect_opt) {
  // Case-1: only redirection, but not rotation and tee involved.
  const bool should_use_pipe_stream = ShouldUsePipeStream(stream_redirect_opt);
  if (!should_use_pipe_stream) {
    return OpenFileForRedirection(stream_redirect_opt.file_path);
  }

  // Case-2: redirection with rotation, or tee is involved.
  //
  // Used to synchronize on asynchronous stream logging.
  // Shared pointer is used here to workaround the known limitation `std::function`
  // requires captured to be copy constructible.
  auto promise = std::make_shared<std::promise<void>>();
  // Invoked after flush and close finished.
  auto on_close_completion = [promise = promise]() { promise->set_value(); };

  StdOstream std_ostream{};

#if defined(__APPLE__) || defined(__linux__)
  if (stream_redirect_opt.tee_to_stdout) {
    int duped_stdout_fd = dup(STDOUT_FILENO);
    RAY_CHECK_NE(duped_stdout_fd, -1) << "Fails to duplicate stdout: " << strerror(errno);

    boost::iostreams::file_descriptor_sink sink{
        duped_stdout_fd, /*file_descriptor_flags=*/boost::iostreams::close_handle};
    std_ostream.stdout_ostream = std::make_shared<
        boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
        std::move(sink));
  }
  if (stream_redirect_opt.tee_to_stderr) {
    int duped_stderr_fd = dup(STDERR_FILENO);
    RAY_CHECK_NE(duped_stderr_fd, -1) << "Fails to duplicate stderr: " << strerror(errno);

    boost::iostreams::file_descriptor_sink sink{
        duped_stderr_fd, /*file_descriptor_flags=*/boost::iostreams::close_handle};
    std_ostream.stderr_ostream = std::make_shared<
        boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
        std::move(sink));
  }

  int pipefd[2] = {0};
  RAY_CHECK_EQ(pipe(pipefd), 0);
  int read_handle = pipefd[0];
  int write_handle = pipefd[1];
  boost::iostreams::file_descriptor_source pipe_read_source{
      read_handle, /*file_descriptor_flags=*/boost::iostreams::close_handle};
  boost::iostreams::file_descriptor_sink pipe_write_sink{
      write_handle, /*file_descriptor_flags=*/boost::iostreams::close_handle};

#elif defined(_WIN32)
  if (stream_redirect_opt.tee_to_stdout) {
    HANDLE duped_stdout_handle;
    BOOL result = DuplicateHandle(GetCurrentProcess(),
                                  GetStdHandle(STD_OUTPUT_HANDLE),
                                  GetCurrentProcess(),
                                  &duped_stdout_handle,
                                  0,
                                  FALSE,
                                  DUPLICATE_SAME_ACCESS);
    RAY_CHECK(result) << "Fails to duplicate stdout handle";

    boost::iostreams::file_descriptor_sink sink{duped_stdout_handle, std::ios_base::out};
    std_ostream.stdout_ostream = std::make_shared<
        boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
        std::move(sink));
  }
  if (stream_redirect_opt.tee_to_stderr) {
    HANDLE duped_stderr_handle;
    BOOL result = DuplicateHandle(GetCurrentProcess(),
                                  GetStdHandle(STD_ERROR_HANDLE),
                                  GetCurrentProcess(),
                                  &duped_stderr_handle,
                                  0,
                                  FALSE,
                                  DUPLICATE_SAME_ACCESS);
    RAY_CHECK(result) << "Fails to duplicate stderr handle";

    boost::iostreams::file_descriptor_sink sink{duped_stderr_handle, std::ios_base::out};
    std_ostream.stderr_ostream = std::make_shared<
        boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
        std::move(sink));
  }

  HANDLE read_handle = nullptr;
  HANDLE write_handle = nullptr;
  SECURITY_ATTRIBUTES sa = {sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
  RAY_CHECK(CreatePipe(&read_handle, &write_handle, &sa, 0)) << "Fails to create pipe";
  boost::iostreams::file_descriptor_source pipe_read_source{read_handle,
                                                            std::ios_base::in};
  boost::iostreams::file_descriptor_sink pipe_write_sink{write_handle,
                                                         std::ios_base::out};

#endif

  auto pipe_instream = std::make_shared<
      boost::iostreams::stream<boost::iostreams::file_descriptor_source>>(
      std::move(pipe_read_source));
  auto pipe_ostream =
      std::make_shared<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
          std::move(pipe_write_sink));

  auto close_fn = [pipe_ostream, promise]() mutable {
    pipe_ostream->flush();
    pipe_ostream->close();
    // Block until destruction finishes.
    promise->get_future().get();
  };

  auto logger = CreateLogger(stream_redirect_opt);

  // [content] is exactly what application writes to pipe, including the trailing
  // newliner, if any.
  auto write_fn = [logger,
                   stream_redirect_opt = stream_redirect_opt,
                   std_ostream = std_ostream](std::string content) {
    if (stream_redirect_opt.tee_to_stdout) {
      std_ostream.stdout_ostream->write(content.data(), content.length());
    }
    if (stream_redirect_opt.tee_to_stderr) {
      std_ostream.stderr_ostream->write(content.data(), content.length());
    }
    if (logger != nullptr) {
      // spdlog adds newliner for every content, no need to maintan the application-passed
      // one.
      if (!content.empty() && content.back() == '\n') {
        content.pop_back();
      }
      logger->log(spdlog::level::info, content);
    }
  };
  auto flush_fn =
      [logger, stream_redirect_opt = stream_redirect_opt, std_ostream = std_ostream]() {
        if (logger != nullptr) {
          logger->flush();
        }
        if (stream_redirect_opt.tee_to_stdout) {
          std_ostream.stdout_ostream->flush();
        }
        if (stream_redirect_opt.tee_to_stderr) {
          std_ostream.stderr_ostream->flush();
        }
      };

  StartStreamDump(std::move(pipe_instream),
                  std::move(write_fn),
                  flush_fn,
                  std::move(on_close_completion));

  RedirectionFileHandle redirection_file_handle{
      write_handle, std::move(pipe_ostream), std::move(flush_fn), std::move(close_fn)};

  return redirection_file_handle;
}

}  // namespace ray
