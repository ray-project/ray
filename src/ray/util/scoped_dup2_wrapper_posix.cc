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

#include <unistd.h>

#include <cstring>
#include <memory>

#include "ray/util/logging.h"
#include "ray/util/scoped_dup2_wrapper.h"

namespace ray {

/*static*/ std::unique_ptr<ScopedDup2Wrapper> ScopedDup2Wrapper::New(int oldfd,
                                                                     int newfd) {
  const int restorefd = dup(newfd);
  RAY_CHECK_NE(restorefd, -1) << "Fails to duplicate newfd " << newfd << " because "
                              << strerror(errno);

  const int ret = dup2(oldfd, newfd);
  RAY_CHECK_NE(ret, -1) << "Fails to duplicate oldfd " << oldfd << " to " << newfd
                        << " because " << strerror(errno);

  return std::unique_ptr<ScopedDup2Wrapper>(new ScopedDup2Wrapper(newfd, restorefd));
}

ScopedDup2Wrapper::~ScopedDup2Wrapper() {
  int ret = dup2(restorefd_, newfd_);
  RAY_CHECK_NE(ret, -1) << "Fails to duplicate restorefd " << restorefd_ << " to "
                        << newfd_ << " because " << strerror(errno);

  RAY_CHECK_OK(Close(restorefd_));
}

}  // namespace ray
