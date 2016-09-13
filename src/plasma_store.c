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

#include "uthash.h"
#include "fling.h"
#include "malloc.h"
#include "plasma.h"
#include "event_loop.h"

#define MAX_NUM_CLIENTS 100

void* dlmalloc(size_t);

typedef struct {
  /* Event loop for the plasma store. */
  event_loop* loop;
} plasma_store_state;

void init_state(plasma_store_state* s) {
  s->loop = malloc(sizeof(event_loop));
  event_loop_init(s->loop);
}

typedef struct {
  /* Object id of this object. */
  plasma_id object_id;
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
} object_table_entry;

/* Objects that are still being written by their owner process. */
object_table_entry* open_objects = NULL;

/* Objects that have already been sealed by their owner process and
 * can now be shared with other processes. */
object_table_entry* sealed_objects = NULL;

typedef struct {
  /* Object id of this object. */
  plasma_id object_id;
  /* Number of processes waiting for the object. */
  int num_waiting;
  /* Socket connections to waiting clients. */
  int conn[MAX_NUM_CLIENTS];
  /* Handle for the uthash table. */
  UT_hash_handle handle;
} object_notify_entry;

/* Objects that processes are waiting for. */
object_notify_entry* objects_notify = NULL;

/* Create a new object buffer in the hash table. */
void create_object(int conn, plasma_request* req) {
  LOG_INFO("creating object"); /* TODO(pcm): add object_id here */

  object_table_entry* entry;
  HASH_FIND(handle, open_objects, &req->object_id, sizeof(plasma_id), entry);
  PLASMA_CHECK(entry == NULL, "Cannot create object twice.");

  void* pointer = dlmalloc(req->size);
  int fd;
  int64_t map_size;
  ptrdiff_t offset;
  get_malloc_mapinfo(pointer, &fd, &map_size, &offset);
  assert(fd != -1);

  entry = malloc(sizeof(object_table_entry));
  memcpy(&entry->object_id, &req->object_id, 20);
  entry->info.size = req->size;
  /* TODO(pcm): set the other fields */
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  HASH_ADD(handle, open_objects, object_id, sizeof(plasma_id), entry);
  plasma_reply reply;
  memset(&reply, 0, sizeof(reply));
  reply.offset = offset;
  reply.map_size = map_size;
  reply.object_size = req->size;
  send_fd(conn, fd, (char*) &reply, sizeof(reply));
}

/* Get an object from the hash table. */
void get_object(int conn, plasma_request* req) {
  object_table_entry* entry;
  HASH_FIND(handle, sealed_objects, &req->object_id, sizeof(plasma_id), entry);
  if (entry) {
    plasma_reply reply;
    memset(&reply, 0, sizeof(plasma_reply));
    reply.offset = entry->offset;
    reply.map_size = entry->map_size;
    reply.object_size = entry->info.size;
    send_fd(conn, entry->fd, (char*) &reply, sizeof(plasma_reply));
  } else {
    object_notify_entry* notify_entry;
    LOG_INFO("object not in hash table of sealed objects");
    HASH_FIND(handle, objects_notify, &req->object_id, sizeof(plasma_id),
              notify_entry);
    if (!notify_entry) {
      notify_entry = malloc(sizeof(object_notify_entry));
      memset(notify_entry, 0, sizeof(object_notify_entry));
      notify_entry->num_waiting = 0;
      memcpy(&notify_entry->object_id, &req->object_id, 20);
      HASH_ADD(handle, objects_notify, object_id, sizeof(plasma_id),
               notify_entry);
    }
    PLASMA_CHECK(notify_entry->num_waiting < MAX_NUM_CLIENTS - 1,
                 "This exceeds the maximum number of clients.");
    notify_entry->conn[notify_entry->num_waiting] = conn;
    notify_entry->num_waiting += 1;
  }
}

/* Seal an object that has been created in the hash table. */
void seal_object(int conn, plasma_request* req) {
  LOG_INFO("sealing object");  // TODO(pcm): add object_id here
  object_table_entry* entry;
  HASH_FIND(handle, open_objects, &req->object_id, sizeof(plasma_id), entry);
  if (!entry) {
    return; /* TODO(pcm): return error */
  }
  HASH_DELETE(handle, open_objects, entry);
  HASH_ADD(handle, sealed_objects, object_id, sizeof(plasma_id), entry);
  /* Inform processes that the object is ready now. */
  object_notify_entry* notify_entry;
  HASH_FIND(handle, objects_notify, &req->object_id, sizeof(plasma_id),
            notify_entry);
  if (!notify_entry) {
    return;
  }
  plasma_reply reply = {.offset = entry->offset,
                        .map_size = entry->map_size,
                        .object_size = entry->info.size};
  for (int i = 0; i < notify_entry->num_waiting; ++i) {
    send_fd(notify_entry->conn[i], entry->fd, (char*) &reply,
            sizeof(plasma_reply));
  }
  HASH_DELETE(handle, objects_notify, notify_entry);
  free(notify_entry);
}

void process_event(int conn, plasma_request* req) {
  switch (req->type) {
  case PLASMA_CREATE:
    create_object(conn, req);
    break;
  case PLASMA_GET:
    get_object(conn, req);
    break;
  case PLASMA_SEAL:
    seal_object(conn, req);
    break;
  default:
    LOG_ERR("invalid request %d", req->type);
    exit(-1);
  }
}

void run_event_loop(int socket) {
  plasma_store_state state;
  init_state(&state);
  event_loop_attach(state.loop, 0, NULL, socket, POLLIN);
  plasma_request req;
  while (1) {
    int num_ready = event_loop_poll(state.loop);
    if (num_ready < 0) {
      LOG_ERR("poll failed");
      exit(-1);
    }
    for (int i = 0; i < event_loop_size(state.loop); ++i) {
      struct pollfd* waiting = event_loop_get(state.loop, i);
      if (waiting->revents == 0)
        continue;
      if (waiting->fd == socket) {
        /* Handle new incoming connections. */
        int new_socket = accept(socket, NULL, NULL);
        event_loop_attach(state.loop, 0, NULL, new_socket, POLLIN);
        LOG_INFO("adding new client");
      } else {
        int r = read(waiting->fd, &req, sizeof(plasma_request));
        if (r == -1) {
          LOG_ERR("read error");
          continue;
        } else if (r == 0) {
          LOG_INFO("connection %d disconnected", i);
          event_loop_detach(state.loop, i, 1);
        } else {
          process_event(waiting->fd, &req);
        }
      }
    }
  }
}

void start_server(char* socket_name) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd == -1) {
    LOG_ERR("socket error");
    exit(-1);
  }
  int on = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*) &on, sizeof(on)) < 0) {
    LOG_ERR("setsockopt failed");
    close(fd);
    exit(-1);
  }
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, socket_name, sizeof(addr.sun_path) - 1);
  unlink(socket_name);
  bind(fd, (struct sockaddr*) &addr, sizeof(addr));
  listen(fd, 5);
  run_event_loop(fd);
}

int main(int argc, char* argv[]) {
  char* socket_name = NULL;
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
  LOG_INFO("starting server listening on %s", socket_name);
  start_server(socket_name);
}
