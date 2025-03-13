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

#pragma once

#include <memory>

#include "ray/util/pipe_logger.h"
#include "ray/util/scoped_dup2_wrapper.h"

namespace ray::internal {

struct RedirectionHandleWrapper {
  RedirectionFileHandle redirection_file_handle;
  // Used for restoration.
  std::unique_ptr<ScopedDup2Wrapper> scoped_dup2_wrapper;
};

// Util functions to redirect stdout / stderr stream based on the given redirection option
// [opt]. This function is _NOT_ thread-safe.
//
// \param opt Option to redirection stream, including log file path and stdout/stderr
// redirection requirement.
RedirectionHandleWrapper RedirectStreamImpl(MEMFD_TYPE_NON_UNIQUE stream_fd,
                                            const StreamRedirectionOption &opt);

// Synchronizes any buffered output indicated by the redirection handle wrapper in
// blocking style, and restore the stream redirection.
void SyncOnStreamRedirection(RedirectionHandleWrapper &redirection_handle_wrapper);

// Flush on redirected stream synchronously.
//
// TODO(hjiang): Current implementation is naive, which directly flushes on spdlog logger
// and could miss those in the pipe; it's acceptable because we only use it in the unit
// test for now.
void FlushOnRedirectedStream(RedirectionHandleWrapper &redirection_handle_wrapper);

}  // namespace ray::internal
