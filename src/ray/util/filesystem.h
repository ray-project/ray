#ifndef RAY_UTIL_FILESYSTEM_H
#define RAY_UTIL_FILESYSTEM_H

#include <string>

namespace ray {

/// \return The portable directory separator (slash on all OSes).
static char get_alt_dir_sep() { return '/'; }

/// \return The platform directory separator (backslash on Windows, slash on other OSes).
static char get_dir_sep() {
  char result;
#ifdef _WIN32
  result = '\\';
#else
  result = '/';
#endif
  return result;
}

/// \return The platform PATH separator (semicolon on Windows, colon on other OSes).
static char get_path_sep() {
  char result;
#ifdef _WIN32
  result = ';';
#else
  result = ':';
#endif
  return result;
}

/// \return A non-volatile temporary directory in which Ray can stores its files.
std::string get_ray_temp_dir();

/// \return The non-volatile temporary directory for the current user (often /tmp).
std::string get_user_temp_dir();

/// \return Whether or not the given character is a directory separator on this platform.
static bool is_dir_sep(char ch) {
  bool result = ch == get_dir_sep();
#ifdef _WIN32
  result |= ch == get_alt_dir_sep();
#endif
  return result;
}

/// \return Whether or not the given character is a PATH separator on this platform.
static bool is_path_sep(char ch) { return ch == get_path_sep(); }

/// \return The result of joining multiple path components.
template <class... Paths>
std::string join_paths(std::string base, Paths... components) {
  std::string to_append[] = {components...};
  for (size_t i = 0; i < sizeof(to_append) / sizeof(*to_append); ++i) {
    const std::string &s = to_append[i];
    if (!base.empty() && !is_dir_sep(base.back()) && !s.empty() && !is_dir_sep(s[0])) {
      base += get_dir_sep();
    }
    base += s;
  }
  return base;
}

}  // namespace ray

#endif  // RAY_UTIL_UTIL_H
