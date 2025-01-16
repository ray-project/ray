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

#include "ray/util/logging_utils.h"

#include <cstring>
#include <functional>
#include <mutex>
#include <vector>

#include "ray/util/compat.h"
#include "ray/util/pipe_logger.h"
#include "ray/util/util.h"

#if defined(_WIN32)
#include <fcntl.h>  // For _O_WTEXT
#include <io.h>     // For _open_osfhandle
#endif

namespace ray {

namespace {

#if defined(__APPLE__) || defined(__linux__)
int GetStdoutHandle() { return STDOUT_FILENO; }
int GetStderrHandle() { return STDERR_FILENO; }
#elif defined(_WIN32)
int GetStdoutHandle() { return _fileno(stdout); }
int GetStderrHandle() { return _fileno(stderr); }
#endif

// TODO(hjiang): Revisit later, should be able to save some heap alllocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<MEMFD_TYPE_NON_UNIQUE, RedirectionFileHandle>
    redirection_file_handles;

// Block synchronize on stream redirection related completion, should be call **EXACTLY
// ONCE** at program termination.
std::once_flag stream_exit_once_flag;
void SyncOnStreamRedirection() {
  for (auto &[_, handle] : redirection_file_handles) {
    handle.Close();
  }
}

#if defined(__APPLE__) || defined(__linux__)
// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(int stream_fd, const LogRedirectionOption &opt) {
  std::call_once(stream_exit_once_flag, []() {
    RAY_CHECK_EQ(std::atexit(SyncOnStreamRedirection), 0)
        << "Fails to register stream redirection termination hook.";
  });

  StdStreamFd std_stream_fd{};
  if (opt.tee_to_stdout) {
    std_stream_fd.stdout_fd = dup(STDOUT_FILENO);
    RAY_CHECK_NE(std_stream_fd.stdout_fd, -1)
        << "Fails to duplicate stdout: " << strerror(errno);
  }
  if (opt.tee_to_stderr) {
    std_stream_fd.stderr_fd = dup(STDERR_FILENO);
    RAY_CHECK_NE(std_stream_fd.stderr_fd, -1)
        << "Fails to duplicate stderr: " << strerror(errno);
  }

  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt, std_stream_fd);
  RAY_CHECK_NE(dup2(handle.GetWriteHandle(), stream_fd), -1)
      << "Fails to duplicate file descritor " << strerror(errno);

  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
}
#endif

void FlushOnRedirectedStream(int stream_fd) {
  auto iter = redirection_file_handles.find(stream_fd);
  RAY_CHECK(iter != redirection_file_handles.end())
      << "Stream with file descriptor " << stream_fd << " is not registered.";
  iter->second.Flush();
}

}  // namespace

#if defined(__APPLE__) || defined(__linux__)
void RedirectStdout(const LogRedirectionOption &opt) {
  RedirectStream(GetStdoutHandle(), opt);
}
#elif defined(_WIN32)
void RedirectStdout(const LogRedirectionOption &opt) { return; }
#endif

#if defined(__APPLE__) || defined(__linux__)
void RedirectStderr(const LogRedirectionOption &opt) {
  RedirectStream(GetStderrHandle(), opt);
}
#elif defined(_WIN32)
void RedirectStderr(const LogRedirectionOption &opt) { return; }
#endif

void FlushOnRedirectedStdout() { FlushOnRedirectedStream(GetStdoutHandle()); }
void FlushOnRedirectedStderr() { FlushOnRedirectedStream(GetStderrHandle()); }

}  // namespace ray
