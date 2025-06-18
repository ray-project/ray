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

#include "ray/util/internal/stream_redirection_handle.h"

#include <memory>
#include <utility>

#include "ray/util/compat.h"

namespace ray::internal {

StreamRedirectionHandle::StreamRedirectionHandle(int stream_fd,
                                                 const StreamRedirectionOption &opt)
    : filepath_(opt.file_path) {
  RedirectionFileHandle handle = CreateRedirectionFileHandle(opt);
  scoped_dup2_wrapper_ = ScopedDup2Wrapper::New(handle.GetWriteFd(), stream_fd);
  redirection_file_handle_ = std::move(handle);
}

StreamRedirectionHandle::~StreamRedirectionHandle() {
  scoped_dup2_wrapper_ = nullptr;
  redirection_file_handle_.Close();
}

void StreamRedirectionHandle::FlushOnRedirectedStream() {
  redirection_file_handle_.Flush();
}

}  // namespace ray::internal
