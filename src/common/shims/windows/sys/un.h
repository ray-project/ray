#ifndef UN_H
#define UN_H

#include <sys/socket.h>

struct sockaddr_un {
  /** AF_UNIX. */
  sa_family_t sun_family;
  /** The pathname. */
  char sun_path[108];
};

#endif /* UN_H */
