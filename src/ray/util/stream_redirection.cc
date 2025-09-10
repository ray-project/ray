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

#include "ray/util/stream_redirection.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/util/compat.h"
#include "ray/util/internal/stream_redirection_handle.h"

namespace ray {

namespace {

// Maps from original stream file fd (i.e. stdout/stderr) to its stream redirector.
absl::flat_hash_map<int, internal::StreamRedirectionHandle> redirection_file_handles;

// A validation function, which verifies redirection handles don't dump to the same file.
void ValidateOutputPathsUniqueness() {
  absl::InlinedVector<std::string_view, 2> filepaths;
  for (const auto &[_, handle] : redirection_file_handles) {
    const auto &cur_filepath = handle.GetFilePath();
    RAY_CHECK(!cur_filepath.empty());
    auto iter = std::find(filepaths.begin(), filepaths.end(), cur_filepath);
    RAY_CHECK(iter == filepaths.end());
    filepaths.emplace_back(cur_filepath);
  }
}

// Redirect the given [stream_fd] based on the specified option.
void RedirectStream(int stream_fd, const StreamRedirectionOption &opt) {
  internal::StreamRedirectionHandle handle_wrapper(stream_fd, opt);
  const bool is_new =
      redirection_file_handles.emplace(stream_fd, std::move(handle_wrapper)).second;
  RAY_CHECK(is_new) << "Redirection has been register for stream " << stream_fd;
  ValidateOutputPathsUniqueness();
}

}  // namespace

void RedirectStdoutOncePerProcess(const StreamRedirectionOption &opt) {
  RedirectStream(GetStdoutFd(), opt);
}
void RedirectStderrOncePerProcess(const StreamRedirectionOption &opt) {
  RedirectStream(GetStderrFd(), opt);
}

}  // namespace ray
