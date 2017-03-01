#include "plasma.h"

#include "io.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "plasma_protocol.h"

void warn_if_sigpipe(int status, int client_sock) {
  if (status >= 0) {
    return;
  }
  if (errno == EPIPE || errno == EBADF) {
    LOG_WARN(
        "Received SIGPIPE or BAD FILE DESCRIPTOR when sending a message to "
        "client on fd %d. The client on the other end may have hung up.",
        client_sock);
    return;
  }
  LOG_FATAL("Failed to write message to client on fd %d.", client_sock);
}
