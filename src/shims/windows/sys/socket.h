#ifndef SOCKET_H
#define SOCKET_H

typedef unsigned short sa_family_t;

#include <Winsock2.h>
#include <unistd.h>  // ssize_t

#define cmsghdr _WSACMSGHDR
#undef CMSG_DATA
#define CMSG_DATA WSA_CMSG_DATA
#define CMSG_SPACE WSA_CMSG_SPACE
#define CMSG_FIRSTHDR WSA_CMSG_FIRSTHDR
#define CMSG_LEN WSA_CMSG_LEN
#define CMSG_NXTHDR WSA_CMSG_NXTHDR

#define SCM_RIGHTS 1

#define iovec _WSABUF
#define iov_base buf
#define iov_len len
#define msghdr _WSAMSG
#define msg_name name
#define msg_namelen namelen
#define msg_iov lpBuffers
#define msg_iovlen dwBufferCount
#define msg_control Control.buf
#define msg_controllen Control.len
#define msg_flags dwFlags

int dumb_socketpair(SOCKET socks[2]);
ssize_t sendmsg(int sockfd, struct msghdr *msg, int flags);
ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags);
int socketpair(int domain, int type, int protocol, int sv[2]);

#ifdef __cplusplus
namespace {
inline int send(SOCKET s, const void *buf, int len, int flags) {
  // Call the const char* overload version
  int (*psend)(SOCKET s, const char *buf, int len, int flags) = ::send;
  return (*psend)(s, (const char *)buf, len, flags);
}
}  // namespace
#endif

#endif /* SOCKET_H */
