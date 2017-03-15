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

/**
 * This will create a new ObjectInfo buffer. The first sizeof(int64_t) bytes
 * of this buffer are the length of the remaining message and the
 * remaining message is a serialized version of the object info.
 *
 * @param object_info The object info to be serialized
 * @return The object info buffer. It is the caller's responsibility to free
 *         this buffer with "free" after it has been used.
 */
uint8_t *create_object_info_buffer(ObjectInfoT *object_info) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateObjectInfo(fbb, object_info);
  fbb.Finish(message);
  uint8_t *notification = (uint8_t *) malloc(sizeof(int64_t) + fbb.GetSize());
  *((int64_t *) notification) = fbb.GetSize();
  memcpy(notification + sizeof(int64_t), fbb.GetBufferPointer(), fbb.GetSize());
  return notification;
}
