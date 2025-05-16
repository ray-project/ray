// Copyright 2020 The Ray Authors.
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

#include "ray/util/filesystem.h"

#include <cstdlib>
#include <fstream>
#include <string>

#ifdef _WIN32
#include <Windows.h>
#endif

namespace ray {

std::string GetFileName(const std::string &path) {
  return std::filesystem::path(path).filename().string();
}

std::string GetUserTempDir() {
  std::string result;
#if defined(__APPLE__) || defined(__linux__)
  // Prefer the hard-coded path for now, for compatibility.
  result = "/tmp";
#elif defined(_WIN32)
  result.resize(1 << 8);
  DWORD n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  if (n > result.size()) {
    result.resize(n);
    n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  }
  result.resize(0 < n && n <= result.size() ? static_cast<size_t>(n) : 0);
#else  // not Darwin, or Windows
  const char *candidates[] = {"TMPDIR", "TMP", "TEMP", "TEMPDIR"};
  const char *found = NULL;
  for (char const *candidate : candidates) {
    found = std::getenv(candidate);
    if (found) {
      break;
    }
  }
  result = found ? found : "/tmp";
#endif
  // Strip trailing separators
  while (!result.empty() && IsDirSep(result.back())) {
    result.pop_back();
  }
  return result;
}

StatusOr<std::string> ReadEntireFile(const std::string &fname) {
  std::ifstream file(fname);
  if (!file.good()) {
    return Status::IOError("") << "Failed to open file " << fname << " because "
                               << strerror(errno);
  }

  std::ostringstream buffer;
  buffer << file.rdbuf();
  if (!file.good()) {
    return Status::IOError("") << "Failed to read from file " << fname << " because "
                               << strerror(errno);
  }

  std::string content = buffer.str();
  file.close();

  return content;
}

}  // namespace ray
