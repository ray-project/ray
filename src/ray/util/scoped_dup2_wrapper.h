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

#include <memory>

#include "ray/util/compat.h"

namespace ray {

// A RAII-style wrapper for `dup2` syscall, which
// - Works on cross-platform;
// - Recovers file descriptor at destruction.
class ScopedDup2Wrapper {
 public:
  // Duplicate [oldfd] to [newfd], same semantics with syscall `dup2`.
  static std::unique_ptr<ScopedDup2Wrapper> New(MEMFD_TYPE_NON_UNIQUE oldfd,
                                                MEMFD_TYPE_NON_UNIQUE newfd);

  ScopedDup2Wrapper(const ScopedDup2Wrapper &) = delete;
  ScopedDup2Wrapper &operator=(const ScopedDup2Wrapper &) = delete;

  // Restore oldfd.
  ~ScopedDup2Wrapper();

 private:
  ScopedDup2Wrapper(MEMFD_TYPE_NON_UNIQUE newfd, MEMFD_TYPE_NON_UNIQUE restorefd)
      : newfd_(newfd), restorefd_(restorefd) {}

  MEMFD_TYPE_NON_UNIQUE newfd_;
  MEMFD_TYPE_NON_UNIQUE restorefd_;
};

}  // namespace ray
