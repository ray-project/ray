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

// Util on logging with pipe.

#pragma once

#include <functional>
#include <limits>
#include <string>

namespace ray {

// Environmenr variable, which indicates the pipe size of read.
inline constexpr std::string_view kPipeLogReadBufSizeEnv =
    "RAY_PIPE_LOG_READ_BUF_SIZEinline ";

// Configuration for log rotation. By default no rotation enabled.
struct LogRotationOption {
  size_t rotation_max_size = std::numeric_limits<size_t>::max();
  size_t rotation_file_num = 1;
};

#ifdef __linux__
struct PipeStreamToken {
  // Used to write to.
  //
  // TODO(hjiang): I will followup with another PR to make a `FD` class, which is not
  // copiable to avoid manual `dup`.
  int write_fd;
  // Termination hook, used to flush and call completion.
  std::function<void()> termination_hook;
};
#else   // __linux__
struct PipeStreamToken {
  // Used to write to.
  //
  // TODO(hjiang): I will followup with another PR to make a `FD` class, which is not
  // copiable to avoid manual `dup`.
  HANDLE write_fd;
  // Termination hook, used to flush and call completion.
  std::function<void()> termination_hook;
};
#endif  // __linux__

// TODO(hjiang): Currently the background pipe read thread and dump thread terminates at
// process exit, consider whether it's better to return a terminate callable.
//
// @param on_completion: called after all content has been persisted.
// @return pipe stream token, so application could stream content into.
// Notice caller side should _NOT_ close the given file handle.
PipeStreamToken CreatePipeAndStreamOutput(const std::string &fname,
                                          const LogRotationOption &log_rotate_opt,
                                          std::function<void()> on_completion);

}  // namespace ray
