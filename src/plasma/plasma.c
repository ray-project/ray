#include "plasma.h"

#include "io.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "plasma_protocol.h"

bool plasma_object_ids_distinct(int num_object_ids, object_id object_ids[]) {
  for (int i = 0; i < num_object_ids; ++i) {
    for (int j = 0; j < i; ++j) {
      if (object_ids_equal(object_ids[i], object_ids[j])) {
        return false;
      }
    }
  }
  return true;
}

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
