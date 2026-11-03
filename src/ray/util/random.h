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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "absl/random/random.h"
#include "ray/util/logging.h"

namespace ray {
namespace internal {

// Seeds an absl::BitGen with 256 bits of fresh OS entropy via std::random_device.
// Both default-constructed absl::BitGen() and absl::MakeSeedSeq() draw from Abseil's
// process-global RandenPool (pool_urbg.cc), which is initialized once via
// absl::call_once and cloned across fork(), causing sibling forked processes and their
// newly spawned threads to draw identical seed sequences if RandenPool was initialized
// before fork().
inline absl::BitGen CreateForkSafeBitGen() {
  std::random_device rd;
  std::seed_seq seq{rd(), rd(), rd(), rd(), rd(), rd(), rd(), rd()};
  return absl::BitGen(seq);
}

}  // namespace internal
}  // namespace ray

/**
 * @brief A helper function to fill random bytes into `data`.
 *
 * Seeds from OS entropy (`std::random_device`) rather than Abseil's process-global
 * `RandenPool` (used by `absl::BitGen()` and `absl::MakeSeedSeq()`, whose state is
 * cloned across `fork()`), and automatically reseeds the thread-local PRNG if the PID
 * changes across `fork()`.
 *
 * @param[out] data Pointer to the byte container to fill with random bytes.
 */
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);

  thread_local absl::BitGen generator = ::ray::internal::CreateForkSafeBitGen();
#ifndef _WIN32
  thread_local pid_t generator_pid = getpid();
  const pid_t current_pid = getpid();
  if (generator_pid != current_pid) {
    generator = ::ray::internal::CreateForkSafeBitGen();
    generator_pid = current_pid;
  }
#endif
  for (size_t i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(
        absl::Uniform(generator, 0, std::numeric_limits<uint8_t>::max()));
  }
}
