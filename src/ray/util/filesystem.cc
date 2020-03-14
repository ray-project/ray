#include "ray/util/filesystem.h"

#include <stdlib.h>

#include "ray/util/logging.h"

#if __cplusplus >= 201703L
#include <filesystem>
#elif defined(_WIN32)
#include <Windows.h>
#endif

namespace ray {

std::string get_ray_temp_dir() { return join_paths(get_user_temp_dir(), "ray"); }

std::string get_user_temp_dir() {
  std::string result;
#if defined(__APPLE__) || defined(__linux__)
  // Prefer the hard-coded path for now, for compatibility.
  result = "/tmp";
#elif __cplusplus >= 201703L
  result = std::filesystem::temp_directory_path();
#elif defined(_WIN32)
  result.resize(1 << 8);
  DWORD n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  if (n > result.size()) {
    result.resize(n);
    n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  }
  result.resize(0 < n && n <= result.size() ? static_cast<size_t>(n) : 0);
#else
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
  while (!result.empty() && is_dir_sep(result.back())) {
    result.pop_back();
  }
  RAY_CHECK(!result.empty());
  return result;
}

}  // namespace ray
