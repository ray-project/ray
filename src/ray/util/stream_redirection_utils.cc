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
#include "ray/util/internal/stream_redirection_utils.h"
#include "ray/util/util.h"

namespace ray {

namespace {

// TODO(hjiang): Revisit later, should be able to save some heap allocation with
// absl::InlinedVector.
//
// Maps from original stream file handle (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<MEMFD_TYPE_NON_UNIQUE, internal::RedirectionHandleWrapper>
    redirection_file_handles;

// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(MEMFD_TYPE_NON_UNIQUE stream_fd, const StreamRedirectionOption &opt) {
  internal::RedirectionHandleWrapper handle_wrapper(stream_fd, opt);
  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle_wrapper)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
}

void FlushOnRedirectedStdout(MEMFD_TYPE_NON_UNIQUE stream_handle) {
  auto iter = redirection_file_handles.find(stream_handle);
  RAY_CHECK(iter != redirection_file_handles.end())
      << "Stream handle " << stream_handle << " hasn't been registered.";
  iter->second.FlushOnRedirectedStream();
}

}  // namespace

void RedirectStdoutOncePerProcess(const StreamRedirectionOption &opt) {
  RedirectStream(GetStdoutHandle(), opt);
}
void RedirectStderrOncePerProcess(const StreamRedirectionOption &opt) {
  RedirectStream(GetStderrHandle(), opt);
}

void FlushOnRedirectedStdout() { FlushOnRedirectedStdout(GetStdoutHandle()); }
void FlushOnRedirectedStderr() { FlushOnRedirectedStdout(GetStderrHandle()); }

}  // namespace ray
