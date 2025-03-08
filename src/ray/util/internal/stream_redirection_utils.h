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

#include "ray/util/scoped_dup2_wrapper.h"
#include "ray/util/pipe_logger.h"

namespace ray::internal {

struct RedirectionHandleWrapper {
	RedirectionFileHandle redirection_file_handle;
	// Used for restoration.
	std::unique_ptr<ScopedDup2Wrapper> scoped_dup2_wrapper;
};

// Util functions to redirect stdout / stderr stream based on the given redirection option [opt].
// This function is _NOT_ thread-safe.
RedirectionHandleWrapper RedirectStdoutImpl(const StreamRedirectionOption &opt);
RedirectionHandleWrapper RedirectStderrImpl(const StreamRedirectionOption &opt);

void SyncOnStreamRedirection()

}  // namespace ray::internal
