#include <sys/socket.h>

int socketpair(int domain, int type, int protocol, SOCKET sv[2]) {
  if ((domain != AF_UNIX && domain != AF_INET) || type != SOCK_STREAM) {
    return -1;
  }
  SOCKET sockets[2];
  int r = dumb_socketpair(sockets);
  sv[0] = sockets[0];
  sv[1] = sockets[1];
  return r;
}
