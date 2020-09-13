#include "ray/util/filesystem.h"

#include <stdlib.h>

#include "ray/util/logging.h"

#ifdef _WIN32
#include <Windows.h>
#endif

namespace ray {

std::string GetExeSuffix() {
  std::string result;
#ifdef _WIN32
  result = ".exe";
#endif
  return result;
}

std::string GetFileName(const std::string &path) {
  size_t i = GetRootPathLength(path), j = path.size();
  while (j > i && !IsDirSep(path[j - 1])) {
    --j;
  }
  return path.substr(j);
}

size_t GetRootPathLength(const std::string &path) {
  size_t i = 0;
#ifdef _WIN32
  if (i + 2 < path.size() && IsDirSep(path[i]) && IsDirSep(path[i + 1]) &&
      !IsDirSep(path[i + 2])) {
    // UNC paths begin with two separators (but not 1 or 3)
    i += 2;
    for (int k = 0; k < 2; ++k) {
      while (i < path.size() && !IsDirSep(path[i])) {
        ++i;
      }
      while (i < path.size() && IsDirSep(path[i])) {
        ++i;
      }
    }
  } else if (i + 1 < path.size() && path[i + 1] == ':') {
    i += 2;
  }
#endif
  while (i < path.size() && IsDirSep(path[i])) {
    ++i;
  }
  return i;
}

std::string GetRayTempDir() { return JoinPaths(GetUserTempDir(), "ray"); }

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
#else  // not Linux, Darwin, or Windows
  const char *candidates[] = {"TMPDIR", "TMP", "TEMP", "TEMPDIR"};
  const char *found = NULL;
  for (char const *candidate : candidates) {
    found = getenv(candidate);
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
  RAY_CHECK(!result.empty());
  return result;
}

}  // namespace ray
