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
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"
#include "ray/util/spdlog_fd_sink.h"
#include "ray/util/spdlog_newliner_sink.h"
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

// Start two threads:
// 1. A reader thread which continuously reads from [pipe_stream] until close;
// 2. A dumper thread which writes content to sink via [write_func].
void StartStreamDump(
    std::shared_ptr<boost::iostreams::stream<boost::iostreams::file_descriptor_source>>
        pipe_instream,
    std::shared_ptr<spdlog::logger> logger,
    std::function<void()> on_close_completion) {
  auto stream_dumper = std::make_shared<StreamDumper>();

  // Create two threads, so there's no IO operation within critical section thus no
  // blocking on write.
  std::thread([pipe_instream = std::move(pipe_instream),
               stream_dumper = stream_dumper]() {
    SetThreadName("PipeReaderThd");

    const size_t buf_size = RayConfig::instance().pipe_logger_read_buf_size();
    // Pre-allocate stream buffer to avoid excessive syscall.
    // TODO(hjiang): Should resize without initialization.
    std::string readsome_buffer(buf_size, '\0');

    std::string cur_segment{"a"};
    while (pipe_instream->read(cur_segment.data(), /*count=*/1)) {
      // Read available bytes in non-blocking style.
      while (true) {
        auto bytes_read =
            pipe_instream->readsome(readsome_buffer.data(), readsome_buffer.length());
        if (bytes_read == 0) {
          break;
        }
        std::string_view cur_readsome_buffer{readsome_buffer.data(),
                                             static_cast<uint64_t>(bytes_read)};
        cur_segment += cur_readsome_buffer;
      }

      // Already read all we have at the moment, stream into logger.
      {
        absl::MutexLock lock(&stream_dumper->mu);
        stream_dumper->content.emplace_back(std::move(cur_segment));
        cur_segment.clear();
      }

      // Read later bytes in blocking style.
      cur_segment = "a";
    }

    // Reached EOF.
    absl::MutexLock lock(&stream_dumper->mu);
    stream_dumper->stopped = true;
  }).detach();

  std::thread([stream_dumper = stream_dumper,
               logger = std::move(logger),
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
          logger->flush();
          on_close_completion();
          return;
        }
      }

      // Perform IO operation out of critical section.
      logger->log(spdlog::level::info, std::move(curline));
    }
  }).detach();
}

// Create a spdlog logger with all sinks specified by the given option.
std::shared_ptr<spdlog::logger> CreateLogger(
    const StreamRedirectionOption &stream_redirect_opt) {
  absl::InlinedVector<spdlog::sink_ptr, 3> sinks;

  // Setup file sink.
  spdlog::sink_ptr file_sink = nullptr;
  if (stream_redirect_opt.rotation_max_size != 0) {
    file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        stream_redirect_opt.file_path,
        stream_redirect_opt.rotation_max_size,
        stream_redirect_opt.rotation_max_file_count);
  } else {
    file_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(
        stream_redirect_opt.file_path);
  }
  file_sink->set_level(spdlog::level::info);
  // Spdlog logger's formatter only applies for its sink (which is newliner sink here),
  // but not internal sinks recursively (aka, rotation file sink won't be set); so have to
  // manually set formatter here.
  file_sink->set_formatter(std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string("")));
  auto newliner_sink = std::make_shared<spdlog_newliner_sink_st>(std::move(file_sink));
  sinks.emplace_back(std::move(newliner_sink));

  // Setup fd sink for stdout and stderr.
#if defined(__APPLE__) || defined(__linux__)
  if (stream_redirect_opt.tee_to_stdout) {
    int duped_stdout_fd = dup(STDOUT_FILENO);
    RAY_CHECK_NE(duped_stdout_fd, -1) << "Fails to duplicate stdout: " << strerror(errno);
    auto stdout_sink = std::make_shared<non_owned_fd_sink_st>(duped_stdout_fd);
    sinks.emplace_back(std::move(stdout_sink));
  }
  if (stream_redirect_opt.tee_to_stderr) {
    int duped_stderr_fd = dup(STDERR_FILENO);
    RAY_CHECK_NE(duped_stderr_fd, -1) << "Fails to duplicate stderr: " << strerror(errno);
    auto stderr_sink = std::make_shared<non_owned_fd_sink_st>(duped_stderr_fd);
    sinks.emplace_back(std::move(stderr_sink));
  }

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
    auto stdout_sink = std::make_shared<non_owned_fd_sink_st>(duped_stdout_handle);
    sinks.emplace_back(std::move(stdout_sink));
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
    auto stderr_sink = std::make_shared<non_owned_fd_sink_st>(duped_stderr_handle);
    sinks.emplace_back(std::move(stderr_sink));
  }
#endif

  auto logger = std::make_shared<spdlog::logger>(
      /*name=*/absl::StrFormat("pipe-logger-%s", stream_redirect_opt.file_path),
      std::make_move_iterator(sinks.begin()),
      std::make_move_iterator(sinks.end()));
  logger->set_level(spdlog::level::info);
  // Only message is logged without extra newliner.
  auto formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  logger->set_formatter(std::move(formatter));
  return logger;
}

// Pipe streamer is only used in certain cases:
// 1. Log rotation is requested;
// 2. Multiple sinks are involved.
bool ShouldUsePipeStream(const StreamRedirectionOption &stream_redirect_opt) {
  const bool need_rotation = stream_redirect_opt.rotation_max_size != 0;
  return need_rotation || stream_redirect_opt.tee_to_stdout ||
         stream_redirect_opt.tee_to_stderr;
}

RedirectionFileHandle OpenFileForRedirection(const std::string &file_path) {
  boost::iostreams::file_descriptor_sink fd_sink{file_path, std::ios_base::out};
  auto handle = fd_sink.handle();
  auto ostream =
      std::make_shared<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
          std::move(fd_sink));

  // In this case, we don't write to the file via logger, so no need to set formatter.
  // spglog is used here merely to reuse the same [RedirectionFileHandle] interface.
  auto logger_sink = std::make_shared<non_owned_fd_sink_st>(handle);
  auto logger = std::make_shared<spdlog::logger>(
      /*name=*/absl::StrFormat("pipe-logger-%s", file_path), std::move(logger_sink));

  // Lifecycle for the file handle is bound at [ostream] thus [close_fn].
  auto close_fn = [ostream = std::move(ostream)]() { ostream->close(); };

  return RedirectionFileHandle{handle, std::move(logger), std::move(close_fn)};
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

#if defined(__APPLE__) || defined(__linux__)
  int pipefd[2] = {0};
  RAY_CHECK_EQ(pipe(pipefd), 0);
  int read_handle = pipefd[0];
  int write_handle = pipefd[1];
#elif defined(_WIN32)
  HANDLE read_handle = nullptr;
  HANDLE write_handle = nullptr;
  SECURITY_ATTRIBUTES sa = {sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
  RAY_CHECK(CreatePipe(&read_handle, &write_handle, &sa, 0)) << "Fails to create pipe";
#endif

  boost::iostreams::file_descriptor_source pipe_read_source{
      read_handle, /*file_descriptor_flags=*/boost::iostreams::close_handle};
  boost::iostreams::file_descriptor_sink pipe_write_sink{
      write_handle, /*file_descriptor_flags=*/boost::iostreams::close_handle};

  auto pipe_instream = std::make_shared<
      boost::iostreams::stream<boost::iostreams::file_descriptor_source>>(
      std::move(pipe_read_source));
  auto pipe_ostream =
      std::make_shared<boost::iostreams::stream<boost::iostreams::file_descriptor_sink>>(
          std::move(pipe_write_sink));

  auto close_fn = [pipe_ostream, promise]() mutable {
    pipe_ostream->close();
    // Block until destruction finishes.
    promise->get_future().get();
  };

  auto logger = CreateLogger(stream_redirect_opt);
  StartStreamDump(std::move(pipe_instream), logger, std::move(on_close_completion));

  RedirectionFileHandle redirection_file_handle{
      write_handle, logger, std::move(close_fn)};

  return redirection_file_handle;
}

}  // namespace ray
