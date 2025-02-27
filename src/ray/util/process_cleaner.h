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

#include <functional>

namespace ray {

// This util function implements such feature:
// To perform certain cleanup work after main process dies (i.e. cleanup cgroup for main
// process), it spawns a subprocess which keeps listens to pipe; child process keeps
// listening to pipe and gets awaken when parent process exits.
//
// Notice it only works for unix platform, and it should be called AT MOST ONCE.
// If there're multiple cleanup functions to register, callers should wrap them all into
// one callback.
void SpawnSubprocessAndCleanup(std::function<void()> cleanup);

}  // namespace ray
