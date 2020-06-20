#ifndef WIN32_FD_H
#define WIN32_FD_H

#if defined(WIN32_REPLACE_FD_APIS) && WIN32_REPLACE_FD_APIS
#ifndef _CRT_DECLARE_NONSTDC_NAMES
#define _CRT_DECLARE_NONSTDC_NAMES 0
#endif
#endif

#include <limits.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include <errno.h>
#undef ECONNRESET
#undef EINPROGRESS
#undef ETIMEDOUT

#include <WS2tcpip.h>
#include <io.h>

enum {
  ECONNRESET = WSAECONNRESET,
  EINPROGRESS = WSAEINPROGRESS,
  ETIMEDOUT = WSAETIMEDOUT,
};
typedef ptrdiff_t ssize_t;
typedef unsigned long int nfds_t;
#endif

int fh_accept(int sockpfd, struct sockaddr *name, socklen_t *namelen);
int fh_bind(int sockpfd, const struct sockaddr *name, socklen_t namelen);
int fh_close(int pfd);
int fh_connect(int socket, const struct sockaddr *name, socklen_t namelen);
int fh_dup(int pfd);

/// Retrieves the underlying file handle for the given pseudo-file descriptor.
intptr_t fh_get(int pfd);

int fh_getpeername(int sockpfd, struct sockaddr *name, socklen_t *namelen);
int fh_getsockname(int sockpfd, struct sockaddr *name, socklen_t *namelen);
int fh_getsockopt(int sockpfd, int level, int name, void *val, socklen_t *len);
int fh_ioctlsocket(int sockpfd, long cmd, unsigned long *argp);
int fh_listen(int sockpfd, int backlog);

/// Opens a pseudo-file descriptor around the given file handle. Sockets are supported.
/// However, this is only intended for storing a HANDLE as an integer.
/// The file descriptor must not be used with external file I/O functions.
int fh_open(intptr_t handle, int mode);

int fh_poll(struct pollfd *fds, nfds_t nfds, int timeout);
ssize_t fh_read(int pfd, void *buffer, size_t size);

/// Releases the underlying file handle for the given pseudo-file descriptor and then
/// closes the pseudo-file descriptor.
intptr_t fh_release(int pfd);

ssize_t fh_recv(int sockpfd, void *buffer, size_t size, int flags);
int fh_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *errorfds,
              struct timeval *timeout);
ssize_t fh_send(int sockpfd, const void *buffer, size_t size, int flags);
int fh_setsockopt(int sockpfd, int level, int name, const void *val, socklen_t len);
int fh_socket(int domain, int type, int protocol);
ssize_t fh_write(int pfd, const void *buffer, size_t size);

#ifdef __cplusplus
}
#endif

#if defined(WIN32_REPLACE_FD_APIS) && WIN32_REPLACE_FD_APIS
#define accept fh_accept
#define bind fh_bind
static int close(int pfd) { return fh_close(pfd); }
#define connect fh_connect
static int dup(int pfd) { return fh_dup(pfd); }
#define getsockopt fh_getsockopt
#define ioctlsocket fh_ioctlsocket
#define listen fh_listen
static int open(intptr_t handle, int mode) { return fh_open(handle, mode); }
#define poll fh_poll
static ssize_t read(int pfd, void *buffer, size_t size) {
  return fh_read(pfd, buffer, size);
}
#define recv fh_recv
#define recvfrom fh_recvfrom
#define select fh_select
#define socket fh_socket
#define send fh_send
#define sendto fh_sendto
#define setsockopt fh_setsockopt
static ssize_t write(int pfd, const void *buffer, size_t size) {
  return fh_write(pfd, buffer, size);
}
#endif

#endif
