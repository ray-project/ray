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
#include <string>

#include "ray/util/pipe_logger.h"
#include "ray/util/scoped_dup2_wrapper.h"

namespace ray::internal {

class StreamRedirectionHandle {
 public:
  StreamRedirectionHandle(int stream_fd, const StreamRedirectionOption &opt);

  StreamRedirectionHandle(const StreamRedirectionHandle &) = delete;
  StreamRedirectionHandle &operator=(const StreamRedirectionHandle &) = delete;
  StreamRedirectionHandle(StreamRedirectionHandle &&) = default;
  StreamRedirectionHandle &operator=(StreamRedirectionHandle &&) = default;

  // Flush buffered output and restore stream redirection.
  ~StreamRedirectionHandle();

  const std::string &GetFilePath() const { return filepath_; }

  // Flush on redirected stream synchronously.
  //
  // TODO(hjiang): Current implementation is naive, which directly flushes on spdlog
  // logger and could miss those in the pipe; it's acceptable because we only use it in
  // the unit test for now.
  void FlushOnRedirectedStream();

 private:
  // File path which dumps the IO stream to.
  std::string filepath_;
  RedirectionFileHandle redirection_file_handle_;
  // Used for restoration.
  std::unique_ptr<ScopedDup2Wrapper> scoped_dup2_wrapper_;
};

}  // namespace ray::internal
