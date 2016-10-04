/* PLASMA STORE: This is a simple object store server process
 *
 * It accepts incoming client connections on a unix domain socket
 * (name passed in via the -s option of the executable) and uses a
 * single thread to serve the clients. Each client establishes a
 * connection and can create objects, wait for objects and seal
 * objects through that connection.
 *
 * It keeps a hash table that maps object_ids (which are 20 byte long,
 * just enough to store and SHA1 hash) to memory mapped files. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <getopt.h>
#include <string.h>
#include <limits.h>
#include <poll.h>

#include "common.h"
#include "event_loop.h"
#include "io.h"
#include "uthash.h"
#include "utarray.h"
#include "fling.h"
#include "malloc.h"
#include "plasma.h"

void *dlmalloc(size_t);
void dlfree(void *);

/**
 * This is used by the Plasma Store to send a reply to the Plasma Client.
 */
void plasma_send_reply(int fd, plasma_reply *reply) {
  int reply_count = sizeof(plasma_reply);
  if (write(fd, reply, reply_count) != reply_count) {
    LOG_ERR("write error, fd = %d", fd);
    exit(-1);
  }
}

typedef struct {
  /* Object id of this object. */
  object_id object_id;
  /* Object info like size, creation time and owner. */
  plasma_object_info info;
  /* Memory mapped file containing the object. */
  int fd;
  /* Size of the underlying map. */
  int64_t map_size;
  /* Offset from the base of the mmap. */
  ptrdiff_t offset;
  /* Handle for the uthash table. */
  UT_hash_handle handle;
  /* Pointer to the object data. Needed to free the object. */
  uint8_t *pointer;
} object_table_entry;

/* Objects that are still being written by their owner process. */
object_table_entry *open_objects = NULL;

/* Objects that have already been sealed by their owner process and
 * can now be shared with other processes. */
object_table_entry *sealed_objects = NULL;

typedef struct {
  /* Object id of this object. */
  object_id object_id;
  /* Socket connections of waiting clients. */
  UT_array *conns;
  /* Handle for the uthash table. */
  UT_hash_handle handle;
} object_notify_entry;

/* Objects that processes are waiting for. */
object_notify_entry *objects_notify = NULL;

/* Create a new object buffer in the hash table. */
plasma_object create_object(int conn,
                            object_id object_id,
                            int64_t data_size,
                            int64_t metadata_size,
                            plasma_object *result) {
  LOG_DEBUG("creating object"); /* TODO(pcm): add object_id here */

  object_table_entry *entry;
  HASH_FIND(handle, open_objects, &object_id, sizeof(object_id), entry);
  CHECKM(entry == NULL, "Cannot create object twice.");

  uint8_t *pointer = dlmalloc(data_size + metadata_size);
  int fd;
  int64_t map_size;
  ptrdiff_t offset;
  get_malloc_mapinfo(pointer, &fd, &map_size, &offset);
  assert(fd != -1);

  entry = malloc(sizeof(object_table_entry));
  memcpy(&entry->object_id, &object_id, 20);
  entry->info.data_size = data_size;
  entry->info.metadata_size = metadata_size;
  entry->pointer = pointer;
  /* TODO(pcm): set the other fields */
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  HASH_ADD(handle, open_objects, object_id, sizeof(object_id), entry);
  object_handle handle = {.store_fd = fd, .mmap_size = map_size};
  result->handle = handle;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
}

/* Get an object from the hash table. */
int get_object(int conn, object_id object_id, plasma_object *result) {
  object_table_entry *entry;
  HASH_FIND(handle, sealed_objects, &object_id, sizeof(object_id), entry);
  if (entry) {
    object_handle handle = {.store_fd = entry->fd,
                            .mmap_size = entry->map_size};
    result->handle = handle;
    result->data_offset = entry->offset;
    result->metadata_offset = entry->offset + entry->info.data_size;
    result->data_size = entry->info.data_size;
    result->metadata_size = entry->info.metadata_size;
    return OBJECT_FOUND;
  } else {
    object_notify_entry *notify_entry;
    LOG_DEBUG("object not in hash table of sealed objects");
    HASH_FIND(handle, objects_notify, &object_id, sizeof(object_id),
              notify_entry);
    if (!notify_entry) {
      notify_entry = malloc(sizeof(object_notify_entry));
      memset(notify_entry, 0, sizeof(object_notify_entry));
      utarray_new(notify_entry->conns, &ut_int_icd);
      memcpy(&notify_entry->object_id, &object_id, 20);
      HASH_ADD(handle, objects_notify, object_id, sizeof(object_id),
               notify_entry);
    }
    utarray_push_back(notify_entry->conns, &conn);
  }
  return OBJECT_NOT_FOUND;
}

/* Check if an object is present. */
int contains_object(int conn, object_id object_id) {
  object_table_entry *entry;
  HASH_FIND(handle, sealed_objects, &object_id, sizeof(object_id), entry);
  return entry ? OBJECT_FOUND : OBJECT_NOT_FOUND;
}

/* Seal an object that has been created in the hash table. */
void seal_object(int conn,
                 object_id object_id,
                 UT_array **conns,
                 plasma_object *result) {
  LOG_DEBUG("sealing object");  // TODO(pcm): add object_id here
  object_table_entry *entry;
  HASH_FIND(handle, open_objects, &object_id, sizeof(object_id), entry);
  if (!entry) {
    return; /* TODO(pcm): return error */
  }
  HASH_DELETE(handle, open_objects, entry);
  HASH_ADD(handle, sealed_objects, object_id, sizeof(object_id), entry);
  /* Inform processes that the object is ready now. */
  object_notify_entry *notify_entry;
  HASH_FIND(handle, objects_notify, &object_id, sizeof(object_id),
            notify_entry);
  if (!notify_entry) {
    *conns = NULL;
    return;
  }
  object_handle handle = {.store_fd = entry->fd, .mmap_size = entry->map_size};
  result->handle = handle;
  result->data_offset = entry->offset;
  result->metadata_offset = entry->offset + entry->info.data_size;
  result->data_size = entry->info.data_size;
  result->metadata_size = entry->info.metadata_size;
  HASH_DELETE(handle, objects_notify, notify_entry);
  *conns = notify_entry->conns;
  free(notify_entry);
}

/* Delete an object that has been created in the hash table. */
void delete_object(int conn, object_id object_id) {
  LOG_DEBUG("deleting object");  // TODO(rkn): add object_id here
  object_table_entry *entry;
  HASH_FIND(handle, sealed_objects, &object_id, sizeof(object_id), entry);
  /* TODO(rkn): This should probably not fail, but should instead throw an
   * error. Maybe we should also support deleting objects that have been created
   * but not sealed. */
  CHECKM(entry != NULL, "To delete an object it must have been sealed.");
  uint8_t *pointer = entry->pointer;
  HASH_DELETE(handle, sealed_objects, entry);
  dlfree(pointer);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  int64_t type;
  int64_t length;
  plasma_request *req;
  read_message(client_sock, &type, &length, (uint8_t **) &req);
  plasma_reply reply;
  memset(&reply, 0, sizeof(reply));
  UT_array *conns;

  switch (type) {
  case PLASMA_CREATE:
    create_object(client_sock, req->object_id, req->data_size,
                  req->metadata_size, &reply.object);
    send_fd(client_sock, reply.object.handle.store_fd, (char *) &reply,
            sizeof(reply));
    break;
  case PLASMA_GET:
    if (get_object(client_sock, req->object_id, &reply.object) ==
        OBJECT_FOUND) {
      send_fd(client_sock, reply.object.handle.store_fd, (char *) &reply,
              sizeof(reply));
    }
    break;
  case PLASMA_CONTAINS:
    if (contains_object(client_sock, req->object_id) == OBJECT_FOUND) {
      reply.has_object = 1;
    }
    plasma_send_reply(client_sock, &reply);
    break;
  case PLASMA_SEAL:
    seal_object(client_sock, req->object_id, &conns, &reply.object);
    if (conns) {
      for (int *c = (int *) utarray_front(conns); c != NULL;
           c = (int *) utarray_next(conns, c)) {
        send_fd(*c, reply.object.handle.store_fd, (char *) &reply,
                sizeof(reply));
      }
      utarray_free(conns);
    }
    break;
  case PLASMA_DELETE:
    delete_object(client_sock, req->object_id);
    break;
  case DISCONNECT_CLIENT: {
    LOG_DEBUG("Disconnecting client on fd %d", client_sock);
    event_loop_remove_file(loop, client_sock);
  } break;
  default:
    /* This code should be unreachable. */
    CHECK(0);
  }

  free(req);
}

void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events) {
  int new_socket = accept_client(listener_sock);
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      context);
  LOG_DEBUG("new connection with fd %d", new_socket);
}

void start_server(char *socket_name) {
  int socket = bind_ipc_sock(socket_name);
  CHECK(socket >= 0);
  event_loop *loop = event_loop_create();
  event_loop_add_file(loop, socket, EVENT_LOOP_READ, new_client_connection,
                      NULL);
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  char *socket_name = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:")) != -1) {
    switch (c) {
    case 's':
      socket_name = optarg;
      break;
    default:
      exit(-1);
    }
  }
  if (!socket_name) {
    LOG_ERR("please specify socket for incoming connections with -s switch");
    exit(-1);
  }
  LOG_DEBUG("starting server listening on %s", socket_name);
  start_server(socket_name);
}
