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

#include "ray/util/internal/stream_redirection_utils.h"

#include <memory>
#include <utility>

#include "ray/util/compat.h"

namespace ray::internal {

RedirectionHandleWrapper RedirectStreamImpl(MEMFD_TYPE_NON_UNIQUE stream_fd,
                                            const StreamRedirectionOption &opt) {
  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt);
  auto scoped_dup2_wrapper = ScopedDup2Wrapper::New(handle.GetWriteHandle(), stream_fd);

  RedirectionHandleWrapper handle_wrapper;
  handle_wrapper.redirection_file_handle = std::move(handle);
  handle_wrapper.scoped_dup2_wrapper = std::move(scoped_dup2_wrapper);

  return handle_wrapper;
}

void SyncOnStreamRedirection(RedirectionHandleWrapper &redirection_handle_wrapper) {
  redirection_handle_wrapper.scoped_dup2_wrapper = nullptr;
  redirection_handle_wrapper.redirection_file_handle.Close();
}

void FlushOnRedirectedStream(RedirectionHandleWrapper &redirection_handle_wrapper) {
  redirection_handle_wrapper.redirection_file_handle.Flush();
}

}  // namespace ray::internal
