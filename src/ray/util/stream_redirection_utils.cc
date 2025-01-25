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
#include <utility>
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

struct RedirectionHandleWrapper {
  RedirectionFileHandle redirection_file_handle;
  // Used for restoration.
  MEMFD_TYPE_NON_UNIQUE saved_stream_handle;
};

// TODO(hjiang): Revisit later, should be able to save some heap allocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<int, RedirectionHandleWrapper> redirection_file_handles;

// Block synchronize on stream redirection related completion, should be call **EXACTLY
// ONCE** at program termination.
std::once_flag stream_exit_once_flag;
void SyncOnStreamRedirection() {
  for (auto &[stream_fd, handle] : redirection_file_handles) {
// Restore old stream fd.
#if defined(__APPLE__) || defined(__linux__)
    RAY_CHECK_NE(dup2(handle.saved_stream_handle, stream_fd), -1)
        << "Fails to restore file descriptor " << strerror(errno);
#elif defined(_WIN32)
    int duped_fd = _open_osfhandle(reinterpret_cast<intptr_t>(handle.saved_stream_handle),
                                   _O_WRONLY);
    RAY_CHECK_NE(_dup2(duped_fd, stream_fd), -1) << "Fails to duplicate file descriptor.";
#endif

    handle.redirection_file_handle.Close();
  }
}

// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(int stream_fd, const StreamRedirectionOption &opt) {
  std::call_once(stream_exit_once_flag, []() {
    RAY_CHECK_EQ(std::atexit(SyncOnStreamRedirection), 0)
        << "Fails to register stream redirection termination hook.";
  });

  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt);

#if defined(__APPLE__) || defined(__linux__)
  // Duplicate stream fd for later restoration.
  MEMFD_TYPE_NON_UNIQUE duped_stream_fd = dup(stream_fd);
  RAY_CHECK_NE(duped_stream_fd, -1)
      << "Fails to duplicate stream fd " << stream_fd << " because " << strerror(errno);

  RAY_CHECK_NE(dup2(handle.GetWriteHandle(), stream_fd), -1)
      << "Fails to duplicate file descriptor " << strerror(errno);
#elif defined(_WIN32)
  // Duplicate stream fd for later restoration.
  MEMFD_TYPE_NON_UNIQUE duped_stream_fd;
  BOOL result = DuplicateHandle(GetCurrentProcess(),
                                (HANDLE)_get_osfhandle(stream_fd),
                                GetCurrentProcess(),
                                &duped_stream_fd,
                                0,
                                FALSE,
                                DUPLICATE_SAME_ACCESS);
  RAY_CHECK(result);

  int pipe_write_fd =
      _open_osfhandle(reinterpret_cast<intptr_t>(handle.GetWriteHandle()), _O_WRONLY);
  RAY_CHECK_NE(_dup2(pipe_write_fd, stream_fd), -1)
      << "Fails to duplicate file descriptor.";
#endif

  RedirectionHandleWrapper handle_wrapper;
  handle_wrapper.redirection_file_handle = std::move(handle);
  handle_wrapper.saved_stream_handle = duped_stream_fd;

  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle_wrapper)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
}

void FlushOnRedirectedStream(int stream_fd) {
  auto iter = redirection_file_handles.find(stream_fd);
  RAY_CHECK(iter != redirection_file_handles.end())
      << "Stream with file descriptor " << stream_fd << " is not registered.";
  iter->second.redirection_file_handle.Flush();
}

}  // namespace

void RedirectStdout(const StreamRedirectionOption &opt) {
  RedirectStream(GetStdoutFd(), opt);
}
void RedirectStderr(const StreamRedirectionOption &opt) {
  RedirectStream(GetStderrFd(), opt);
}
void FlushOnRedirectedStdout() { FlushOnRedirectedStream(GetStdoutFd()); }
void FlushOnRedirectedStderr() { FlushOnRedirectedStream(GetStderrFd()); }

}  // namespace ray
