#ifndef STRINGS_H
#define STRINGS_H

#include <string.h>

static int strcasecmp(const char *s1, const char *s2) { return stricmp(s1, s2); }

static int strncasecmp(const char *s1, const char *s2, size_t n) {
  return strnicmp(s1, s2, n);
}

#endif /* STRINGS_H */
