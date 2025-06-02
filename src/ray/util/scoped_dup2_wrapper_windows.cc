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

#include <fcntl.h>  // For _O_WTEXT
#include <io.h>     // For _open_osfhandle
#include <windows.h>

#include <memory>

#include "ray/util/scoped_dup2_wrapper.h"

namespace ray {

/*static*/ std::unique_ptr<ScopedDup2Wrapper> ScopedDup2Wrapper::New(HANDLE oldfd,
                                                                     HANDLE newfd) {
  HANDLE restorefd = NULL;
  BOOL success = DuplicateHandle(GetCurrentProcess(),
                                 newfd,
                                 GetCurrentProcess(),
                                 &restorefd,
                                 0,
                                 FALSE,
                                 DUPLICATE_SAME_ACCESS);
  RAY_CHECK(success);

  int old_win_fd = _open_osfhandle(reinterpret_cast<intptr_t>(oldfd), _O_WRONLY);
  int new_win_fd = _open_osfhandle(reinterpret_cast<intptr_t>(newfd), _O_WRONLY);
  RAY_CHECK_NE(_dup2(old_win_fd, new_win_fd), -1) << "Fails to duplicate file descriptor";

  return std::unique_ptr<ScopedDup2Wrapper>(new ScopedDup2Wrapper(newfd, restorefd));
}

ScopedDup2Wrapper::~ScopedDup2Wrapper() {
  int restore_win_fd = _open_osfhandle(reinterpret_cast<intptr_t>(restorefd_), _O_WRONLY);
  int new_win_fd = _open_osfhandle(reinterpret_cast<intptr_t>(newfd_), _O_WRONLY);
  RAY_CHECK_NE(_dup2(restore_win_fd, new_win_fd), -1)
      << "Fails to duplicate file descriptor";
  RAY_CHECK_OK(Close(restorefd_));
}

}  // namespace ray
