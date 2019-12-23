#ifndef TIME_H
#define TIME_H

#include <WinSock2.h>  // clients require timeval definition

struct timeval;
struct timezone;

#ifdef __cplusplus
extern "C"
#endif
    int
    gettimeofday(struct timeval *tv, struct timezone *tz);

#endif /* TIME_H */
