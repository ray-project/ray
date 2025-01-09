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
//
// TODO(hjiang): Should document the env variable after end-to-end integration has
// finished.
inline constexpr std::string_view kPipeLogReadBufSizeEnv = "RAY_PIPE_LOG_READ_BUF_SIZE";

// Configuration for log rotation. By default no rotation enabled.
struct LogRotationOption {
  // Max number of bytes in a rotated file.
  size_t rotation_max_size = std::numeric_limits<size_t>::max();
  // Max number of files for all rotated files.
  size_t rotation_max_file = 1;
};

#if defined(__APPLE__) || defined(__linux__)
struct PipeStreamToken {
  // Used to write to.
  //
  // TODO(hjiang): I will followup with another PR to make a `FD` class, which is not
  // copiable to avoid manual `dup`.
  int write_fd;
  // Termination hook, used to flush and call completion.
  std::function<void()> termination_caller;
};
#elif defined(_WIN32)
struct PipeStreamToken {
  // Used to write to.
  HANDLE write_fd;
  // Termination hook, used to flush and call completion.
  std::function<void()> termination_caller;
};
#endif  // __linux__

// This function creates a pipe so applications could write to.
// There're will be two threads spawned in the background, one thread continuously reads
// from the pipe, another one dumps whatever read from pipe to the persistent file.
//
// We follow a cooperative destruction model, which means
// - The stop and destruction is initiated by application with the termination function we
// returns;
// - Application is expected to block wait on the completion via the passed-in
// [on_completion].
//
// @param on_completion: called after all content has been persisted.
// @return pipe stream token, so application could stream content into, and synchronize on
// the destruction.
//
// Notice caller side should _NOT_ close the given file handle, it will be handled
// internally.
PipeStreamToken CreatePipeAndStreamOutput(const std::string &fname,
                                          const LogRotationOption &log_rotate_opt,
                                          std::function<void()> on_completion);

}  // namespace ray
