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

// This file contains a few logging related util functions.

#pragma once

#include "ray/util/stream_redirection_options.h"

namespace ray {

// Util functions to redirect stdout / stderr stream.
//
// NOTICE:
// 1. This function should be called **at most once** per process; redirected stream
// will be flushed and synchronized at process termination to guarantee no data loss.
// 2. This function is _NOT_ thread-safe.
void RedirectStdoutOncePerProcess(const StreamRedirectionOption &opt);
void RedirectStderrOncePerProcess(const StreamRedirectionOption &opt);

}  // namespace ray
