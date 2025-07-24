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
#if defined(HAVE_FDATASYNC) && !HAVE_DECL_FDATASYNC
extern int fdatasync(int fildes);
#endif
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace ray {

#if defined(__APPLE__) || defined(__linux__)
Status CompleteWrite(int fd, const char *data, size_t len) {
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
Status Flush(int fd) {
#if HAVE_FULLFSYNC
  // On macOS and iOS, fsync() doesn't guarantee durability past power
  // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
  // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
  // fsync().
  if (::fcntl(fd, F_FULLFSYNC) == 0) {
    return Status::OK();
  }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
  int ret = ::fdatasync(fd) == 0;
#else
  int ret = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

  RAY_CHECK(ret != -1 || errno != EIO) << "Fails to flush to file " << strerror(errno);
  if (ret == -1) {
    return Status::IOError("") << "Fails to flush file because " << strerror(errno);
  }
  return Status::OK();
}
Status Close(int fd) {
  const int ret = close(fd);
  if (ret != 0) {
    return Status::IOError("") << "Fails to flush file because " << strerror(errno);
  }
  return Status::OK();
}
#elif defined(_WIN32)
Status CompleteWrite(int fd, const char *data, size_t len) {
  const int ret = _write(fd, data, len);
  if (ret == -1) {
    return Status::IOError("") << "Fails to write to file because " << strerror(errno);
  }
  if (ret != static_cast<int>(len)) {
    return Status::IOError("") << "Fails to write all requested bytes, requests to write "
                               << len << " bytes, but actually write " << ret << " bytes";
  }
  return Status::OK();
}
Status Flush(int fd) {
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  if (handle == INVALID_HANDLE_VALUE) {
    return Status::IOError("") << "Fails to get file handle for flushing";
  }
  if (!FlushFileBuffers(handle)) {
    return Status::IOError("") << "Fails to flush file";
  }
  return Status::OK();
}
Status Close(int fd) {
  const int ret = _close(fd);
  if (ret != 0) {
    return Status::IOError("") << "Fails to flush file because " << strerror(errno);
  }
  return Status::OK();
}
#endif

}  // namespace ray
