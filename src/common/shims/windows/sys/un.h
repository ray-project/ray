#pragma once

#include <sys/socket.h>

struct sockaddr_un {
  sa_family_t sun_family; /* AF_UNIX */
  char sun_path[108];     /* pathname */
};
