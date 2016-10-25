#include "fling.h"

#include <string.h>

void init_msg(struct msghdr *msg,
              struct iovec *iov,
              char *buf,
              size_t buf_len) {
  iov->iov_base = buf;
  iov->iov_len = 1;

  msg->msg_iov = iov;
  msg->msg_iovlen = 1;
  msg->msg_control = buf;
  msg->msg_controllen = buf_len;
  msg->msg_name = NULL;
  msg->msg_namelen = 0;
}

int send_fd(int conn, int fd, const char *payload, int size) {
  struct msghdr msg;
  struct iovec iov;
  char buf[CMSG_SPACE(sizeof(int))];
  memset(&buf, 0, CMSG_SPACE(sizeof(int)));

  init_msg(&msg, &iov, buf, sizeof(buf));

  struct cmsghdr *header = CMSG_FIRSTHDR(&msg);
  header->cmsg_level = SOL_SOCKET;
  header->cmsg_type = SCM_RIGHTS;
  header->cmsg_len = CMSG_LEN(sizeof(int));
  *(int *) CMSG_DATA(header) = fd;

  /* send file descriptor and payload */
  return sendmsg(conn, &msg, 0) != -1 && send(conn, payload, size, 0) == -1;
}

int recv_fd(int conn, char *payload, int size) {
  struct msghdr msg;
  struct iovec iov;
  char buf[CMSG_SPACE(sizeof(int))];
  init_msg(&msg, &iov, buf, sizeof(buf));

  if (recvmsg(conn, &msg, 0) == -1)
    return -1;

  int found_fd = -1;
  int oh_noes = 0;
  for (struct cmsghdr *header = CMSG_FIRSTHDR(&msg); header != NULL;
       header = CMSG_NXTHDR(&msg, header))
    if (header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS) {
      int count =
          (header->cmsg_len - (CMSG_DATA(header) - (unsigned char *) header)) /
          sizeof(int);
      for (int i = 0; i < count; ++i) {
        int fd = ((int *) CMSG_DATA(header))[i];
        if (found_fd == -1) {
          found_fd = fd;
        } else {
          close(fd);
          oh_noes = 1;
        }
      }
    }

  /* The sender sent us more than one file descriptor. We've closed
   * them all to prevent fd leaks but notify the caller that we got
   * a bad message. */
  if (oh_noes) {
    close(found_fd);
    errno = EBADMSG;
    return -1;
  }

  ssize_t len = recv(conn, payload, size, 0);
  if (len < 0) {
    return -1;
  }

  return found_fd;
}
