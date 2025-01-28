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

#include "ray/util/stream_redirection_utils.h"

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

// TODO(hjiang): Revisit later, should be able to save some heap allocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<int, RedirectionFileHandle> redirection_file_handles;

// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(int stream_fd, const StreamRedirectionOption &opt) {
  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt);

#if defined(__APPLE__) || defined(__linux__)
  RAY_CHECK_NE(dup2(handle.GetWriteHandle(), stream_fd), -1)
      << "Fails to duplicate file descritor " << strerror(errno);
#elif defined(_WIN32)
  int pipe_write_fd =
      _open_osfhandle(reinterpret_cast<intptr_t>(handle.GetWriteHandle()), _O_WRONLY);
  RAY_CHECK_NE(_dup2(pipe_write_fd, stream_fd), -1)
      << "Fails to duplicate file descritor.";
#endif

  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
}

void FlushOnRedirectedStream(int stream_fd) {
  auto iter = redirection_file_handles.find(stream_fd);
  RAY_CHECK(iter != redirection_file_handles.end())
      << "Stream with file descriptor " << stream_fd << " is not registered.";
  iter->second.Flush();
}

}  // namespace

void RedirectStdout(const StreamRedirectionOption &opt) {
  RedirectStream(GetStdoutHandle(), opt);
}
void RedirectStderr(const StreamRedirectionOption &opt) {
  RedirectStream(GetStderrHandle(), opt);
}
void FlushOnRedirectedStdout() { FlushOnRedirectedStream(GetStdoutHandle()); }
void FlushOnRedirectedStderr() { FlushOnRedirectedStream(GetStderrHandle()); }

}  // namespace ray
