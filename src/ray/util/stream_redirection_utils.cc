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
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "ray/util/compat.h"
#include "ray/util/pipe_logger.h"
#include "ray/util/scoped_dup2_wrapper.h"
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
  std::unique_ptr<ScopedDup2Wrapper> scoped_dup2_wrapper;
};

// TODO(hjiang): Revisit later, should be able to save some heap allocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<MEMFD_TYPE_NON_UNIQUE, RedirectionHandleWrapper>
    redirection_file_handles;

// Block synchronize on stream redirection related completion, should be call **EXACTLY
// ONCE** at program termination.
std::once_flag stream_exit_once_flag;
void SyncOnStreamRedirection() {
  for (auto &[_, handle] : redirection_file_handles) {
    handle.scoped_dup2_wrapper = nullptr;
    handle.redirection_file_handle.Close();
  }
}

// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(MEMFD_TYPE_NON_UNIQUE stream_fd, const StreamRedirectionOption &opt) {
  std::call_once(stream_exit_once_flag, []() {
    RAY_CHECK_EQ(std::atexit(SyncOnStreamRedirection), 0)
        << "Fails to register stream redirection termination hook.";
  });

  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt);
  auto scoped_dup2_wrapper = ScopedDup2Wrapper::New(handle.GetWriteHandle(), stream_fd);

  RedirectionHandleWrapper handle_wrapper;
  handle_wrapper.redirection_file_handle = std::move(handle);
  handle_wrapper.scoped_dup2_wrapper = std::move(scoped_dup2_wrapper);

  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle_wrapper)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
}

void FlushOnRedirectedStream(MEMFD_TYPE_NON_UNIQUE stream_fd) {
  auto iter = redirection_file_handles.find(stream_fd);
  RAY_CHECK(iter != redirection_file_handles.end())
      << "Stream with file descriptor " << stream_fd << " is not registered.";
  iter->second.redirection_file_handle.Flush();
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
