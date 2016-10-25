/* FLING: Exchanging file descriptors over sockets
 *
 * This is a little library for sending file descriptors over a socket
 * between processes. The reason for doing that (as opposed to using
 * filenames to share the files) is so (a) no files remain in the
 * filesystem after all the processes terminate, (b) to make sure that
 * there are no name collisions and (c) to be able to control who has
 * access to the data.
 *
 * Most of the code is from https://github.com/sharvil/flingfd */

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

/* This is neccessary for Mac OS X, see http://www.apuebook.com/faqs2e.html
 * (10). */
#if !defined(CMSG_SPACE) && !defined(CMSG_LEN)
#define CMSG_SPACE(len) \
  (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + __DARWIN_ALIGN32(len))
#define CMSG_LEN(len) (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + (len))
#endif

void init_msg(struct msghdr *msg, struct iovec *iov, char *buf, size_t buf_len);

/* Send a file descriptor "fd" and a payload "payload" of size "size"
 * over the socket "conn". Return 0 on success. */
int send_fd(int conn, int fd, const char *payload, int size);

/* Receive a file descriptor and a payload of size up to "size" from a
 * socket "conn". The payload will be written to "payload" and the file
 * descriptor will be returned. Returns -1 on failure. */
int recv_fd(int conn, char *payload, int size);
