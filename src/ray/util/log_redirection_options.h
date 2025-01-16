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

#include <unistd.h>

#include <limits>
#include <string>

namespace ray {

// Configuration for log redirection. By default no rotation enabled.
//
// It's allowed to have multiple sinks are enabled in the option.
// For example, if redirection file set and `tee_to_stdout` both set to true, the stream
// content is written to both sinks.
struct LogRedirectionOption {
  // Redirected file path on local filesystem.
  std::string file_path;
  // Max number of bytes in a rotated file.
  size_t rotation_max_size = std::numeric_limits<size_t>::max();
  // Max number of files for all rotated files.
  size_t rotation_max_file_count = 1;
  // Whether to tee to stdout.
  bool tee_to_stdout = false;
  // Whether to tee to stderr.
  bool tee_to_stderr = false;
};

// File descriptors which indicates standard stream.
#if defined(__APPLE__) || defined(__linux__)
struct StdStreamFd {
  int stdout_fd = STDOUT_FILENO;
  int stderr_fd = STDERR_FILENO;
};
#elif defined(_WIN32)
// TODO(hjiang): not used for windows, implement later.
struct StdStreamFd {
  int stdout_fd = -1;
  int stderr_fd = -1;
};
#endif

}  // namespace ray
