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

#include <cstdint>
#include <iterator>
#include <limits>
#include <random>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#include "absl/random/random.h"

namespace ray::internal {

/**
 * @brief Create an `absl::BitGen` seeded from OS entropy instead of Abseil's global
 * `RandenPool`, which `fork()` copies into child processes.
 */
inline absl::BitGen CreateForkSafeBitGen() {
#ifdef _WIN32
  std::random_device rd;
  std::seed_seq seq{rd(), rd(), rd(), rd(), rd(), rd(), rd(), rd()};
#else
  uint32_t seed[8] = {};
  const int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
  RAY_CHECK(fd >= 0) << "Failed to open /dev/urandom";
  const ssize_t bytes_read = read(fd, seed, sizeof(seed));
  close(fd);
  RAY_CHECK(bytes_read == static_cast<ssize_t>(sizeof(seed)))
      << "Failed to read seed from /dev/urandom";
  std::seed_seq seq(std::begin(seed), std::end(seed));
#endif
  return absl::BitGen(seq);
}

}  // namespace ray::internal

/**
 * @brief A helper function to fill random bytes into `data`.
 *
 * Seeds from OS entropy rather than Abseil's `RandenPool` and automatically reseeds
 * the thread-local PRNG if the PID changes across `fork()`.
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
