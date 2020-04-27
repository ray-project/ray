#ifndef UN_H
#define UN_H

#include <sys/socket.h>

#define UNIX_PATH_MAX 108

typedef struct sockaddr_un {
  ADDRESS_FAMILY sun_family;
  char sun_path[UNIX_PATH_MAX];
} SOCKADDR_UN, *PSOCKADDR_UN;

#ifndef AF_LOCAL
#define AF_LOCAL AF_UNIX
#endif

#endif /* UN_H */
