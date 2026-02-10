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

// Minimal wrapper to log getenv call sites (for debugging SIGSEGV in getenv).
// No dependency on Ray logging; logs to stderr. The last "[RAY_GETENV]" line
// before a crash is the call that faulted.

#pragma once

#include <cstdlib>

namespace ray {

const char *RayGetEnv(const char *name, const char *file, int line);

}  // namespace ray

#define RAY_GETENV(name) ::ray::RayGetEnv((name), __FILE__, __LINE__)
