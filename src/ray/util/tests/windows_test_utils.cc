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

#if defined(_WIN32)

#include "ray/util/tests/windows_test_utils.h"

#include <windows.h>

#include <memory>

#include "ray/util/util.h"

namespace ray {

std::string CompleteReadFile(const std::string &fname) {
  HANDLE file_handle = CreateFile(fname.c_str(),
                                  GENERIC_READ,
                                  FILE_SHARE_READ,
                                  NULL,
                                  OPEN_EXISTING,
                                  FILE_ATTRIBUTE_NORMAL,
                                  NULL);
  RAY_CHECK(file_handle != INVALID_HANDLE_VALUE);

  auto close_handle = [file_handle]() { CloseHandle(file_handle); };
  std::unique_ptr<void, decltype(close_handle)> handle_guard(file_handle, close_handle);

  LARGE_INTEGER file_size;
  RAY_CHECK(GetFileSizeEx(file_handle, &file_size));
  size_t size = static_cast<size_t>(file_size.QuadPart);
  std::string content(size, '\0');
  DWORD bytes_read = 0;
  RAY_CHECK(ReadFile(file_handle, content.data(), size, &bytes_read, NULL));
  return content;
}

}  // namespace ray

#endif
