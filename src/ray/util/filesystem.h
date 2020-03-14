#ifndef RAY_UTIL_FILESYSTEM_H
#define RAY_UTIL_FILESYSTEM_H

#include <string>

namespace ray {

/// \return The portable directory separator (slash on all OSes).
static char GetAltDirSep() { return '/'; }

/// \return The platform directory separator (backslash on Windows, slash on other OSes).
static char GetDirSep() {
  char result;
#ifdef _WIN32
  result = '\\';
#else
  result = '/';
#endif
  return result;
}

/// \return The platform PATH separator (semicolon on Windows, colon on other OSes).
static char GetPathSep() {
  char result;
#ifdef _WIN32
  result = ';';
#else
  result = ':';
#endif
  return result;
}

/// \return A non-volatile temporary directory in which Ray can stores its files.
std::string GetRayTempDir();

/// \return The non-volatile temporary directory for the current user (often /tmp).
std::string GetUserTempDir();

/// \return Whether or not the given character is a directory separator on this platform.
static bool IsDirSep(char ch) {
  bool result = ch == GetDirSep();
#ifdef _WIN32
  result |= ch == GetAltDirSep();
#endif
  return result;
}

/// \return Whether or not the given character is a PATH separator on this platform.
static bool IsPathSep(char ch) { return ch == GetPathSep(); }

/// \return The result of joining multiple path components.
template <class... Paths>
std::string JoinPaths(std::string base, Paths... components) {
  std::string to_append[] = {components...};
  for (size_t i = 0; i < sizeof(to_append) / sizeof(*to_append); ++i) {
    const std::string &s = to_append[i];
    if (!base.empty() && !IsDirSep(base.back()) && !s.empty() && !IsDirSep(s[0])) {
      base += GetDirSep();
    }
    base += s;
  }
  return base;
}

}  // namespace ray

#endif  // RAY_UTIL_UTIL_H
