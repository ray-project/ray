#ifndef SOCKET_H
#define SOCKET_H

#include <Winsock2.h>
#include <unistd.h>  // ssize_t

int dumb_socketpair(SOCKET socks[2]);
int socketpair(int domain, int type, int protocol, SOCKET sv[2]);

#endif /* SOCKET_H */
