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

#include <limits>
#include <string>

namespace ray {

// Configuration for std stream redirection. By default no rotation enabled.
//
// It's allowed to have multiple sinks are enabled in the option.
// For example, if redirection file set and `tee_to_stdout` both set to true, the stream
// content is written to both sinks.
struct StreamRedirectionOption {
  StreamRedirectionOption() = default;

  // Redirected file path on local filesystem.
  std::string file_path;
  // Max number of bytes in a rotated file.
  // 0 means rotation not enabled, by default 0.
  size_t rotation_max_size = 0;
  // Max number of files for all rotated files.
  size_t rotation_max_file_count = 1;
  // Whether to tee to stdout.
  bool tee_to_stdout = false;
  // Whether to tee to stderr.
  bool tee_to_stderr = false;
};

}  // namespace ray
