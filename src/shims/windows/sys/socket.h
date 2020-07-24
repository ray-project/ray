#ifndef SOCKET_H
#define SOCKET_H

#include <Winsock2.h>

#ifdef _WIN64
typedef long long ssize_t;
#else
typedef int ssize_t;
#endif

int dumb_socketpair(SOCKET socks[2]);
int socketpair(int domain, int type, int protocol, SOCKET sv[2]);

#endif /* SOCKET_H */
