#include <limits.h>
#include <sys/time.h>

#ifdef timezone
#pragma push_macro("timezone")  // Work around https://bugs.python.org/issue34657
#undef timezone
#define timezone timezone
#endif

int gettimeofday(struct timeval *tv, struct timezone *tz) {
  // Free implementation from: https://stackoverflow.com/a/26085827
  SYSTEMTIME systime;
  GetSystemTime(&systime);
  FILETIME filetime;
  SystemTimeToFileTime(&systime, &filetime);
  unsigned long long const epoch_time_offset = 11644473600ULL * 60 * 60 * 24,
                           time_high = filetime.dwHighDateTime,
                           time =
                               filetime.dwLowDateTime +
                               (time_high << (CHAR_BIT * sizeof(filetime.dwLowDateTime)));
  tv->tv_sec = static_cast<int>((time - epoch_time_offset) / 10000000);
  tv->tv_usec = static_cast<int>(systime.wMilliseconds * 1000);
  return 0;
}

#ifdef timezone
#pragma pop_macro("timezone")
#endif
