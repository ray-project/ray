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

#include "ray/util/tests/unix_test_utils.h"

#include <boost/scope_exit.hpp>

#include "ray/util/util.h"

#if defined(__APPLE__) || defined(__linux__)

namespace ray {

std::string CompleteReadFile(const std::string &fname) {
  // Open the file with read-only access
  int fd = open(fname.c_str(), O_RDONLY);
  RAY_CHECK_GT(fd, 0);
  BOOST_SCOPE_EXIT(&fd) { close(fd); }
  BOOST_SCOPE_EXIT_END;

  struct stat file_stat;
  RAY_CHECK_EQ(fstat(fd, &file_stat), 0);
  size_t file_size = file_stat.st_size;

  // TODO(hjiang): Should use resize without initialization.
  std::string content(file_size, '\0');
  ssize_t bytes_read = read(fd, content.data(), file_size);
  RAY_CHECK_EQ(bytes_read, static_cast<ssize_t>(file_size));

  return content;
}

}  // namespace ray

#endif
