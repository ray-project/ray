#ifndef HEADER_CURL_SELECT_H
#define HEADER_CURL_SELECT_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2017, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#ifdef HAVE_POLL_H
#include <poll.h>
#elif defined(HAVE_SYS_POLL_H)
#include <sys/poll.h>
#endif

/*
 * Definition of pollfd struct and constants for platforms lacking them.
 */

#if !defined(HAVE_STRUCT_POLLFD) && \
    !defined(HAVE_SYS_POLL_H) && \
    !defined(HAVE_POLL_H) && \
    !defined(POLLIN)

#define POLLIN      0x01
#define POLLPRI     0x02
#define POLLOUT     0x04
#define POLLERR     0x08
#define POLLHUP     0x10
#define POLLNVAL    0x20

struct pollfd
{
    curl_socket_t fd;
    short   events;
    short   revents;
};

#endif

#ifndef POLLRDNORM
#define POLLRDNORM POLLIN
#endif

#ifndef POLLWRNORM
#define POLLWRNORM POLLOUT
#endif

#ifndef POLLRDBAND
#define POLLRDBAND POLLPRI
#endif

/* there are three CSELECT defines that are defined in the public header that
   are exposed to users, but this *IN2 bit is only ever used internally and
   therefore defined here */
#define CURL_CSELECT_IN2 (CURL_CSELECT_ERR << 1)

int Curl_socket_check(curl_socket_t readfd, curl_socket_t readfd2,
                      curl_socket_t writefd,
                      time_t timeout_ms);

#define SOCKET_READABLE(x,z) \
  Curl_socket_check(x, CURL_SOCKET_BAD, CURL_SOCKET_BAD, z)
#define SOCKET_WRITABLE(x,z) \
  Curl_socket_check(CURL_SOCKET_BAD, CURL_SOCKET_BAD, x, z)

int Curl_poll(struct pollfd ufds[], unsigned int nfds, int timeout_ms);

/* On non-DOS and non-Winsock platforms, when Curl_ack_eintr is set,
 * EINTR condition is honored and function might exit early without
 * awaiting full timeout.  Otherwise EINTR will be ignored and full
 * timeout will elapse. */
extern int Curl_ack_eintr;

int Curl_wait_ms(int timeout_ms);

#ifdef TPF
int tpf_select_libcurl(int maxfds, fd_set* reads, fd_set* writes,
                       fd_set* excepts, struct timeval* tv);
#endif

/* Winsock and TPF sockets are not in range [0..FD_SETSIZE-1], which
   unfortunately makes it impossible for us to easily check if they're valid
*/
#if defined(USE_WINSOCK) || defined(TPF)
#define VALID_SOCK(x) 1
#define VERIFY_SOCK(x) Curl_nop_stmt
#else
#define VALID_SOCK(s) (((s) >= 0) && ((s) < FD_SETSIZE))
#define VERIFY_SOCK(x) do { \
  if(!VALID_SOCK(x)) { \
    SET_SOCKERRNO(EINVAL); \
    return -1; \
  } \
} WHILE_FALSE
#endif

#endif /* HEADER_CURL_SELECT_H */

