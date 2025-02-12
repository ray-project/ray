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

#include "ray/util/dup2_wrapper.h"

namespace ray {

/*static*/ std::unique_ptr<ScopedDup2Wrapper> ScopedDup2Wrapper::New(HANDLE oldfd,
                                                                     HANLE newfd) {
  HANDLE restorefd = NULL;
  BOOL success = DuplicateHandle(GetCurrentProcess(),
                                 oldfd,
                                 GetCurrentProcess(),
                                 &restorefd,
                                 0,
                                 FALSE,
                                 DUPLICATE_SAME_ACCESS);
  RAY_CHECK(success);

  success = DuplicateHandle(GetCurrentProcess(),
                            oldfd,
                            GetCurrentProcess(),
                            &newfd,
                            0,
                            FALSE,
                            DUPLICATE_SAME_ACCESS);
  RAY_CHECK(success);

  return std::unique_ptr<ScopedDup2Wrapper>(new ScopedDup2Wrapper(oldfd, restorefd));
}

ScopedDup2Wrapper::~ScopedDup2Wrapper() {
  BOOL success = DuplicateHandle(GetCurrentProcess(),
                                 oldfd_,
                                 GetCurrentProcess(),
                                 &restorefd_,
                                 0,
                                 FALSE,
                                 DUPLICATE_SAME_ACCESS);
  RAY_CHECK(success);
}

}  // namespace ray
