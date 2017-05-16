#include "plasma.h"

#include "io.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "plasma_protocol.h"

int warn_if_sigpipe(int status, int client_sock) {
  if (status >= 0) {
    return 0;
  }
  if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
    ARROW_LOG(WARNING) <<
        "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when "
        "sending a message to client on fd " << client_sock << ". The client on the other end may "
        "have hung up.";
    return errno;
  }
  ARROW_LOG(FATAL) << "Failed to write message to client on fd " << client_sock << ".";
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

ObjectTableEntry *get_object_table_entry(PlasmaStoreInfo *store_info,
                                         ObjectID object_id) {
  auto it = store_info->objects.find(object_id);
  if (it == store_info->objects.end()) {
    return NULL;
  }
  return it->second.get();
}
