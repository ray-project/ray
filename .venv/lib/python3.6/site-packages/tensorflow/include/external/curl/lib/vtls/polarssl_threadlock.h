#ifndef HEADER_CURL_POLARSSL_THREADLOCK_H
#define HEADER_CURL_POLARSSL_THREADLOCK_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 2013-2015, Daniel Stenberg, <daniel@haxx.se>, et al.
 * Copyright (C) 2010, Hoi-Ho Chan, <hoiho.chan@gmail.com>
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/
#include "curl_setup.h"

#if (defined USE_POLARSSL) || (defined USE_MBEDTLS)

#if defined(USE_THREADS_POSIX)
#  define POLARSSL_MUTEX_T       pthread_mutex_t
#elif defined(USE_THREADS_WIN32)
#  define POLARSSL_MUTEX_T       HANDLE
#endif

#if defined(USE_THREADS_POSIX) || defined(USE_THREADS_WIN32)

int Curl_polarsslthreadlock_thread_setup(void);
int Curl_polarsslthreadlock_thread_cleanup(void);
int Curl_polarsslthreadlock_lock_function(int n);
int Curl_polarsslthreadlock_unlock_function(int n);

#else

#define Curl_polarsslthreadlock_thread_setup() 1
#define Curl_polarsslthreadlock_thread_cleanup() 1
#define Curl_polarsslthreadlock_lock_function(x) 1
#define Curl_polarsslthreadlock_unlock_function(x) 1

#endif /* USE_THREADS_POSIX || USE_THREADS_WIN32 */

#endif /* USE_POLARSSL */

#endif /* HEADER_CURL_POLARSSL_THREADLOCK_H */
