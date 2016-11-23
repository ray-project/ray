#ifndef TIME_H
#define TIME_H

#include <WinSock2.h> /* timeval */

int gettimeofday_highres(struct timeval *tv, struct timezone *tz);

static int gettimeofday(struct timeval *tv, struct timezone *tz) {
  return gettimeofday_highres(tv, tz);
}

#endif /* TIME_H */
