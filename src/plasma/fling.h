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

#ifdef __cplusplus
extern "C" {
#endif

/* This is neccessary for Mac OS X, see http://www.apuebook.com/faqs2e.html
 * (10). */
#if !defined(CMSG_SPACE) && !defined(CMSG_LEN)
#define CMSG_SPACE(len) \
  (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + __DARWIN_ALIGN32(len))
#define CMSG_LEN(len) (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + (len))
#endif

void init_msg(struct msghdr *msg, struct iovec *iov, char *buf, size_t buf_len);

/**
 * Send a file descriptor over a unix domain socket.
 *
 * @param conn Unix domain socket to send the file descriptor over.
 * @param fd File descriptor to send over.
 * @return Status code which is < 0 on failure.
 */
int send_fd(int conn, int fd);

/**
 * Receive a file descriptor over a unix domain socket.
 *
 * @param conn Unix domain socket to receive the file descriptor from.
 * @return File descriptor or a value < 0 on failure.
 */
int recv_fd(int conn);

#ifdef __cplusplus
}
#endif
