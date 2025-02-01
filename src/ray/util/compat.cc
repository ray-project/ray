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

#include "ray/util/compat.h"

#include <cstring>

#include "ray/util/logging.h"

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace ray {

#if defined(__APPLE__) || defined(__linux__)
Status CompleteWrite(MEMFD_TYPE_NON_UNIQUE fd, const char *data, size_t len) {
  const ssize_t ret = write(fd, data, len);
  if (ret == -1) {
    return Status::IOError("") << "Fails to write to file because " << strerror(errno);
  }
  if (ret != static_cast<ssize_t>(len)) {
    return Status::IOError("") << "Fails to write all requested bytes, requests to write "
                               << len << " bytes, but actually write " << ret << " bytes";
  }
  return Status::OK();
}
void Flush(MEMFD_TYPE_NON_UNIQUE fd) {
  RAY_CHECK_EQ(fdatasync(fd), 0) << "Fails to flush file because " << strerror(errno);
}
Status Close(MEMFD_TYPE_NON_UNIQUE fd) {
  const int ret = close(fd);
  if (ret != 0) {
    return Status::IOError("") << "Fails to flush file because " << strerror(errno);
  }
  return Status::OK();
}
#elif defined(_WIN32)
Status CompleteWrite(MEMFD_TYPE_NON_UNIQUE fd, const char *data, size_t len) {
  DWORD bytes_written;
  BOOL success = WriteFile(fd, data, (DWORD)len, &bytes_written, NULL);
  if (!success) {
    return Status::IOError("") << "Fails to write to file";
  }
  if ((DWORD)len != bytes_written) {
    return Status::IOError("") << "Fails to write all requested bytes, requests to write "
                               << len << " bytes, but actually write " << bytes_written
                               << " bytes";
  }
  return Status::OK();
}
void Flush(MEMFD_TYPE_NON_UNIQUE fd) {
  RAY_CHECK(FlushFileBuffers(fd)) << "Fails to flush file";
}
Status Close(MEMFD_TYPE_NON_UNIQUE fd) {
  if (!CloseHandle(fd)) {
    return Status::IOError("") << "Fails to close file handle";
  }
  return Status::OK();
}
#endif

}  // namespace ray
