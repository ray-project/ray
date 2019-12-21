#ifndef _PTHREAD_H
#define _PTHREAD_H 1

#ifndef _INC_WINDOWS
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>
#endif

typedef CRITICAL_SECTION pthread_mutex_t;

#endif /* pthread.h */
