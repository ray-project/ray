#ifndef TIME_H
#define TIME_H

#ifdef timezone
#pragma push_macro("timezone")  // Work around https://bugs.python.org/issue34657
#undef timezone
#define timezone timezone
#endif

#include <WinSock2.h>  // clients require timeval definition

struct timeval;
struct timezone;

#ifdef __cplusplus
extern "C"
#endif
    int
    gettimeofday(struct timeval *tv, struct timezone *tz);

#ifdef timezone
#pragma pop_macro("timezone")
#endif

#endif /* TIME_H */
