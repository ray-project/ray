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
#include <future>
#include <mutex>
#include <vector>

#include "ray/util/compat.h"
#include "ray/util/pipe_logger.h"
#include "ray/util/util.h"

#if defined(_WIN32)
#include <io.h>
#endif

namespace ray {

namespace {

#if defined(__APPLE__) || defined(__linux__)
int GetStdoutHandle() { return STDOUT_FILENO; }
int GetStderrHandle() { return STDERR_FILENO; }
#elif defined(_WIN32)
HANDLE GetStdoutHandle() { return _fileno(stdout); }
HANDLE GetStderrHandle() { return _fileno(stderr); }
#endif

struct StreamRedirector {
  // Used to synchronize the logging completion.
  std::function<void()> completion_callback;
  RedirectionFileHandle redirection_handle;
};

// TODO(hjiang): Revisit later, should be able to save some heap alllocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
// Callbacks which should be synchronized before program termination.
absl::flat_hash_map<MEMFD_TYPE_NON_UNIQUE, std::unique_ptr<StreamRedirector>>
    stream_redirectors;

// Block synchronize on stream redirection related completion, should be call EXACTLY once
// at program termination.
std::once_flag stream_exit_once_flag;
void SyncOnStreamRedirection() {
  // TODO(hjiang): Could use absl::InlinedVector to save memory allocation.
  std::vector<std::function<void()>> completion_callbacks;
  completion_callbacks.reserve(stream_redirectors.size());

  for (auto &[_, redirector] : stream_redirectors) {
    completion_callbacks.emplace_back(redirector->completion_callback);
  }
  // Used to trigger completion hook function.
  stream_redirectors.clear();
  // Block wait stream destruction completion.
  for (auto &cb : completion_callbacks) {
    cb();
  }
}

void RedirectStream(MEMFD_TYPE_NON_UNIQUE stream_fd, const LogRedirectionOption &opt) {
  std::call_once(stream_exit_once_flag, []() { std::atexit(SyncOnStreamRedirection); });

  // Intentional leak, since its lifecycle lasts until program termination.
  auto *promise = new std::promise<void>{};
  auto on_completion = [promise]() { promise->set_value(); };
  RedirectionFileHandle handle = CreatePipeAndStreamOutput(opt, std::move(on_completion));

#if defined(__APPLE__) || defined(__linux__)
  RAY_CHECK_NE(dup2(handle.GetWriteHandle(), stream_fd), -1)
      << "Fails to duplicate file descritor " << strerror(errno);
#elif defined(_WIN32)
  RAY_CHECK_NE(_dup2(handle.GetWriteHandle(), stream_fd), -1)
      << "Fails to duplicate file descritor.";
#endif

  auto [iter, is_new] =
      stream_redirectors.emplace(stream_fd, std::make_unique<StreamRedirector>());
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
  auto &redirector = iter->second;
  redirector->completion_callback = [promise]() { promise->get_future().get(); };
  redirector->redirection_handle = std::move(handle);
}

void FlushOnRedirectedStream(MEMFD_TYPE_NON_UNIQUE stream_fd) {
  auto iter = stream_redirectors.find(stream_fd);
  RAY_CHECK(iter != stream_redirectors.end())
      << "Stream with file descriptor " << stream_fd << " is not registered.";
  auto &redirector = iter->second;
  redirector->redirection_handle.Flush();
}

}  // namespace

void RedirectStdout(const LogRedirectionOption &opt) {
  RedirectStream(GetStdoutHandle(), opt);
}

void RedirectStderr(const LogRedirectionOption &opt) {
  RedirectStream(GetStderrHandle(), opt);
}

void FlushOnRedirectedStdout() { FlushOnRedirectedStream(GetStdoutHandle()); }
void FlushOnRedirectedStderr() { FlushOnRedirectedStream(GetStderrHandle()); }

}  // namespace ray
