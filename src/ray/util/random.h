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
#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>
#endif

#include "absl/random/random.h"

/// A helper function to fill random bytes into the `data`.
/// Automatically reseeds the thread-local PRNG if the PID changes across `fork()`.
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);

  thread_local absl::BitGen generator;
#ifndef _WIN32
  thread_local pid_t generator_pid = getpid();
  const pid_t current_pid = getpid();
#else
  thread_local int generator_pid = _getpid();
  const int current_pid = _getpid();
#endif
  if (generator_pid != current_pid) {
    generator = absl::BitGen();
    generator_pid = current_pid;
  }
  for (size_t i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(
        absl::Uniform(generator, 0, std::numeric_limits<uint8_t>::max()));
  }
}
