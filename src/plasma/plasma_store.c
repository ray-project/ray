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
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/un.h>
#include <getopt.h>
#include <string.h>
#include <signal.h>
#include <limits.h>
#include <poll.h>

#include "common.h"
#include "event_loop.h"
#include "eviction_policy.h"
#include "io.h"
#include "uthash.h"
#include "utarray.h"
#include "fling.h"
#include "malloc.h"
#include "plasma_protocol.h"
#include "plasma_store.h"
#include "plasma.h"

void *dlmalloc(size_t);
void dlfree(void *);

typedef struct {
  /* Object id of this object. */
  object_id object_id;
  /* An array of the clients that are waiting to get this object. */
  UT_array *waiting_clients;
  /* Handle for the uthash table. */
  UT_hash_handle handle;
} object_notify_entry;

/** Contains all information that is associated with a Plasma store client. */
struct client {
  /** The socket used to communicate with the client. */
  int sock;
  /** A pointer to the global plasma state. */
  plasma_store_state *plasma_state;
};

/* This is used to define the array of clients used to define the
 * object_table_entry type. */
UT_icd client_icd = {sizeof(client *), NULL, NULL, NULL};

typedef struct {
  /** Client file descriptor. This is used as a key for the hash table. */
  int subscriber_fd;
  /** The object IDs to notify the client about. We notify the client about the
   *  IDs in the order that the objects were sealed. */
  UT_array *object_ids;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} notification_queue;

struct plasma_store_state {
  /* Event loop of the plasma store. */
  event_loop *loop;
  /* Objects that processes are waiting for. */
  object_notify_entry *objects_notify;
  /** The pending notifications that have not been sent to subscribers because
   *  the socket send buffers were full. This is a hash table from client file
   *  descriptor to an array of object_ids to send to that client. */
  notification_queue *pending_notifications;
  /** The plasma store information, including the object tables, that is exposed
   *  to the eviction policy. */
  plasma_store_info *plasma_store_info;
  /** The state that is managed by the eviction policy. */
  eviction_state *eviction_state;
  /** Input buffer. This is allocated only once to avoid mallocs for every
   *  call to process_message. */
  UT_array *input_buffer;
  /** Buffer that holds memory for serializing plasma protocol messages. */
  protocol_builder *builder;
};

UT_icd byte_icd = {sizeof(uint8_t), NULL, NULL, NULL};

plasma_store_state *init_plasma_store(event_loop *loop, int64_t system_memory) {
  plasma_store_state *state = malloc(sizeof(plasma_store_state));
  state->loop = loop;
  state->objects_notify = NULL;
  state->pending_notifications = NULL;
  /* Initialize the plasma store info. */
  state->plasma_store_info = malloc(sizeof(plasma_store_info));
  state->plasma_store_info->objects = NULL;
  state->plasma_store_info->memory_capacity = system_memory;
  /* Initialize the eviction state. */
  state->eviction_state = make_eviction_state();
  utarray_new(state->input_buffer, &byte_icd);
  state->builder = make_protocol_builder();
  return state;
}

void push_notification(plasma_store_state *state, object_id object_id);

/* If this client is not already using the object, add the client to the
 * object's list of clients, otherwise do nothing. */
void add_client_to_object_clients(object_table_entry *entry,
                                  client *client_info) {
  /* Check if this client is already using the object. */
  for (int i = 0; i < utarray_len(entry->clients); ++i) {
    client **c = (client **) utarray_eltptr(entry->clients, i);
    if (*c == client_info) {
      return;
    }
  }
  /* If there are no other clients using this object, notify the eviction policy
   * that the object is being used. */
  if (utarray_len(entry->clients) == 0) {
    /* Tell the eviction policy that this object is being used. */
    int64_t num_objects_to_evict;
    object_id *objects_to_evict;
    begin_object_access(client_info->plasma_state->eviction_state,
                        client_info->plasma_state->plasma_store_info,
                        entry->object_id, &num_objects_to_evict,
                        &objects_to_evict);
    remove_objects(client_info->plasma_state, num_objects_to_evict,
                   objects_to_evict);
  }
  /* Add the client pointer to the list of clients using this object. */
  utarray_push_back(entry->clients, &client_info);
}

/* Create a new object buffer in the hash table. */
int create_object(client *client_context,
                  object_id obj_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  plasma_object *result) {
  LOG_DEBUG("creating object"); /* TODO(pcm): add object_id here */
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  /* TODO(swang): Return these error to the client instead of exiting. */
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &obj_id,
            sizeof(obj_id), entry);
  if (entry != NULL) {
    /* There is already an object with the same ID in the Plasma Store, so
     * ignore this requst. */
    return PlasmaError_ObjectExists;
  }
  /* Tell the eviction policy how much space we need to create this object. */
  int64_t num_objects_to_evict;
  object_id *objects_to_evict;
  bool success = require_space(
      plasma_state->eviction_state, plasma_state->plasma_store_info,
      data_size + metadata_size, &num_objects_to_evict, &objects_to_evict);
  remove_objects(plasma_state, num_objects_to_evict, objects_to_evict);
  /* Return an error to the client if not enough space could be freed to create
   * the object. */
  if (!success) {
    return PlasmaError_OutOfMemory;
  }
  /* Allocate space for the new object */
  uint8_t *pointer = dlmalloc(data_size + metadata_size);
  int fd;
  int64_t map_size;
  ptrdiff_t offset;
  get_malloc_mapinfo(pointer, &fd, &map_size, &offset);
  assert(fd != -1);

  entry = malloc(sizeof(object_table_entry));
  memset(entry, 0, sizeof(object_table_entry));
  memcpy(&entry->object_id, &obj_id, sizeof(entry->object_id));
  entry->info.obj_id = obj_id;
  entry->info.data_size = data_size;
  entry->info.metadata_size = metadata_size;
  entry->pointer = pointer;
  /* TODO(pcm): set the other fields */
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  entry->state = PLASMA_CREATED;
  utarray_new(entry->clients, &client_icd);
  HASH_ADD(handle, plasma_state->plasma_store_info->objects, object_id,
           sizeof(object_id), entry);
  result->handle.store_fd = fd;
  result->handle.mmap_size = map_size;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
  /* Notify the eviction policy that this object was created. This must be done
   * immediately before the call to add_client_to_object_clients so that the
   * eviction policy does not have an opportunity to evict the object. */
  object_created(plasma_state->eviction_state, plasma_state->plasma_store_info,
                 obj_id);
  /* Record that this client is using this object. */
  add_client_to_object_clients(entry, client_context);
  return PlasmaError_OK;
}

/* Get an object from the hash table. */
int get_object(client *client_context,
               int conn,
               object_id object_id,
               plasma_object *result) {
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  if (entry && entry->state == PLASMA_SEALED) {
    result->handle.store_fd = entry->fd;
    result->handle.mmap_size = entry->map_size;
    result->data_offset = entry->offset;
    result->metadata_offset = entry->offset + entry->info.data_size;
    result->data_size = entry->info.data_size;
    result->metadata_size = entry->info.metadata_size;
    /* If necessary, record that this client is using this object. In the case
     * where entry == NULL, this will be called from seal_object. */
    add_client_to_object_clients(entry, client_context);
    return OBJECT_FOUND;
  } else {
    object_notify_entry *notify_entry;
    LOG_DEBUG("object not in hash table of sealed objects");
    HASH_FIND(handle, plasma_state->objects_notify, &object_id,
              sizeof(object_id), notify_entry);
    if (!notify_entry) {
      notify_entry = malloc(sizeof(object_notify_entry));
      memset(notify_entry, 0, sizeof(object_notify_entry));
      utarray_new(notify_entry->waiting_clients, &client_icd);
      notify_entry->object_id = object_id;
      HASH_ADD(handle, plasma_state->objects_notify, object_id,
               sizeof(object_id), notify_entry);
    }
    utarray_push_back(notify_entry->waiting_clients, &client_context);
  }
  return OBJECT_NOT_FOUND;
}

/* Get an object from the local Plasma Store if exists. */
int get_object_local(client *client_context,
                     int conn,
                     object_id object_id,
                     plasma_object *result) {
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  if (entry && entry->state == PLASMA_SEALED) {
    result->handle.store_fd = entry->fd;
    result->handle.mmap_size = entry->map_size;
    result->data_offset = entry->offset;
    result->metadata_offset = entry->offset + entry->info.data_size;
    result->data_size = entry->info.data_size;
    result->metadata_size = entry->info.metadata_size;
    /* If necessary, record that this client is using this object. In the case
     * where entry == NULL, this will be called from seal_object. */
    add_client_to_object_clients(entry, client_context);
    return OBJECT_FOUND;
  }
  return OBJECT_NOT_FOUND;
}

int remove_client_from_object_clients(object_table_entry *entry,
                                      client *client_info) {
  /* Find the location of the client in the array. */
  for (int i = 0; i < utarray_len(entry->clients); ++i) {
    client **c = (client **) utarray_eltptr(entry->clients, i);
    if (*c == client_info) {
      /* Remove the client from the array. */
      utarray_erase(entry->clients, i, 1);
      /* If no more clients are using this object, notify the eviction policy
       * that the object is no longer being used. */
      if (utarray_len(entry->clients) == 0) {
        /* Tell the eviction policy that this object is no longer being used. */
        int64_t num_objects_to_evict;
        object_id *objects_to_evict;
        end_object_access(client_info->plasma_state->eviction_state,
                          client_info->plasma_state->plasma_store_info,
                          entry->object_id, &num_objects_to_evict,
                          &objects_to_evict);
        remove_objects(client_info->plasma_state, num_objects_to_evict,
                       objects_to_evict);
      }
      /* Return 1 to indicate that the client was removed. */
      return 1;
    }
  }
  /* Return 0 to indicate that the client was not removed. */
  return 0;
}

void release_object(client *client_context, object_id object_id) {
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  CHECK(entry != NULL);
  /* Remove the client from the object's array of clients. */
  CHECK(remove_client_from_object_clients(entry, client_context) == 1);
}

/* Check if an object is present. */
int contains_object(client *client_context, object_id object_id) {
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  return entry && (entry->state == PLASMA_SEALED) ? OBJECT_FOUND
                                                  : OBJECT_NOT_FOUND;
}

/* Seal an object that has been created in the hash table. */
void seal_object(client *client_context,
                 object_id object_id,
                 unsigned char digest[]) {
  LOG_DEBUG("sealing object");  // TODO(pcm): add object_id here
  plasma_store_state *plasma_state = client_context->plasma_state;
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  CHECK(entry != NULL);
  CHECK(entry->state == PLASMA_CREATED);
  /* Set the state of object to SEALED. */
  entry->state = PLASMA_SEALED;
  /* Set the object digest. */
  memcpy(entry->info.digest, digest, DIGEST_SIZE);
  /* Inform all subscribers that a new object has been sealed. */
  push_notification(plasma_state, object_id);

  /* Inform processes getting this object that the object is ready now. */
  object_notify_entry *notify_entry;
  HASH_FIND(handle, plasma_state->objects_notify, &object_id, sizeof(object_id),
            notify_entry);
  if (notify_entry) {
    plasma_object object;
    object.handle.store_fd = entry->fd;
    object.handle.mmap_size = entry->map_size;
    object.data_offset = entry->offset;
    object.metadata_offset = entry->offset + entry->info.data_size;
    object.data_size = entry->info.data_size;
    object.metadata_size = entry->info.metadata_size;
    HASH_DELETE(handle, plasma_state->objects_notify, notify_entry);
    /* Send notifications to the clients that were waiting for this object. */
    for (int i = 0; i < utarray_len(notify_entry->waiting_clients); ++i) {
      client **c = (client **) utarray_eltptr(notify_entry->waiting_clients, i);
      CHECK(plasma_send_GetReply((*c)->sock, plasma_state->builder, &object_id,
                                 &object, 1) >= 0);
      CHECK(send_fd((*c)->sock, object.handle.store_fd) >= 0);
      /* Record that the client is using this object. */
      add_client_to_object_clients(entry, *c);
    }
    utarray_free(notify_entry->waiting_clients);
    free(notify_entry);
  }
}

/* Delete an object that has been created in the hash table. This should only
 * be called on objects that are returned by the eviction policy to evict. */
void delete_object(plasma_store_state *plasma_state, object_id object_id) {
  LOG_DEBUG("deleting object");
  object_table_entry *entry;
  HASH_FIND(handle, plasma_state->plasma_store_info->objects, &object_id,
            sizeof(object_id), entry);
  /* TODO(rkn): This should probably not fail, but should instead throw an
   * error. Maybe we should also support deleting objects that have been created
   * but not sealed. */
  CHECKM(entry != NULL, "To delete an object it must be in the object table.");
  CHECKM(entry->state == PLASMA_SEALED,
         "To delete an object it must have been sealed.");
  CHECKM(utarray_len(entry->clients) == 0,
         "To delete an object, there must be no clients currently using it.");
  uint8_t *pointer = entry->pointer;
  HASH_DELETE(handle, plasma_state->plasma_store_info->objects, entry);
  dlfree(pointer);
  utarray_free(entry->clients);
  free(entry);
  /* Inform all subscribers that the object has been deleted. */
  push_notification(plasma_state, object_id);
}

void remove_objects(plasma_store_state *plasma_state,
                    int64_t num_objects_to_evict,
                    object_id *objects_to_evict) {
  if (num_objects_to_evict > 0) {
    for (int i = 0; i < num_objects_to_evict; ++i) {
      delete_object(plasma_state, objects_to_evict[i]);
    }
    /* Free the array of objects to evict. This array was originally allocated
     * by the eviction policy. */
    free(objects_to_evict);
  }
}

void push_notification(plasma_store_state *plasma_state, object_id object_id) {
  notification_queue *queue, *temp_queue;
  HASH_ITER(hh, plasma_state->pending_notifications, queue, temp_queue) {
    utarray_push_back(queue->object_ids, &object_id);
    send_notifications(plasma_state->loop, queue->subscriber_fd, plasma_state,
                       0);
  }
}

/* Send more notifications to a subscriber. */
void send_notifications(event_loop *loop,
                        int client_sock,
                        void *context,
                        int events) {
  plasma_store_state *plasma_state = context;
  notification_queue *queue;
  HASH_FIND_INT(plasma_state->pending_notifications, &client_sock, queue);
  CHECK(queue != NULL);

  int num_processed = 0;
  /* Loop over the array of pending notifications and send as many of them as
   * possible. */
  for (int i = 0; i < utarray_len(queue->object_ids); ++i) {
    object_id *obj_id = (object_id *) utarray_eltptr(queue->object_ids, i);
    object_table_entry *entry = NULL;
    /* This object should already exist in plasma store state. */
    HASH_FIND(handle, plasma_state->plasma_store_info->objects, obj_id,
              sizeof(object_id), entry);

    object_info object_info;
    if (entry == NULL) {
      memset(&object_info, 0, sizeof(object_info));
      object_info.obj_id = *obj_id;
      object_info.is_deletion = true;
    } else {
      object_info = entry->info;
      object_info.is_deletion = false;
    }

    /* Attempt to send a notification about this object ID. */
    int nbytes =
        send(client_sock, (char const *) &object_info, sizeof(object_info), 0);
    if (nbytes >= 0) {
      CHECK(nbytes == sizeof(object_info));
    } else if (nbytes == -1 &&
               (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      LOG_DEBUG(
          "The socket's send buffer is full, so we are caching this "
          "notification and will send it later.");
      /* Add a callback to the event loop to send queued notifications whenever
       * there is room in the socket's send buffer. Callbacks can be added
       * more than once here and will be overwritten. The callback is removed
       * at the end of the method. */
      event_loop_add_file(plasma_state->loop, client_sock, EVENT_LOOP_WRITE,
                          send_notifications, plasma_state);
      break;
    } else {
      CHECKM(0, "This code should be unreachable.");
    }
    num_processed += 1;
  }
  /* Remove the sent notifications from the array. */
  utarray_erase(queue->object_ids, 0, num_processed);
  /* If we have sent all notifications, remove the fd from the event loop. */
  if (utarray_len(queue->object_ids) == 0) {
    event_loop_remove_file(loop, client_sock);
  }
}

/* Subscribe to notifications about sealed objects. */
void subscribe_to_updates(client *client_context, int conn) {
  LOG_DEBUG("subscribing to updates");
  plasma_store_state *plasma_state = client_context->plasma_state;
  int fd = recv_fd(conn);
  CHECK(fd >= 0);
  CHECKM(HASH_CNT(handle, plasma_state->plasma_store_info->objects) == 0,
         "plasma_subscribe should be called before any objects are created.");
  /* Create a new array to buffer notifications that can't be sent to the
   * subscriber yet because the socket send buffer is full. TODO(rkn): the queue
   * never gets freed. */
  notification_queue *queue =
      (notification_queue *) malloc(sizeof(notification_queue));
  queue->subscriber_fd = fd;
  utarray_new(queue->object_ids, &object_id_icd);
  HASH_ADD_INT(plasma_state->pending_notifications, subscriber_fd, queue);
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  client *client_context = context;
  plasma_store_state *state = client_context->plasma_state;
  int64_t type;
  read_buffer(client_sock, &type, state->input_buffer);

  uint8_t *input = (uint8_t *) utarray_front(state->input_buffer);
  object_id object_ids[1];
  int64_t num_objects;
  plasma_object objects[1];
  memset(&objects[0], 0, sizeof(objects));
  int error;

  flatcc_builder_reset(state->builder);

  /* Process the different types of requests. */
  switch (type) {
  case MessageType_PlasmaCreateRequest: {
    int64_t data_size;
    int64_t metadata_size;
    plasma_read_CreateRequest(input, &object_ids[0], &data_size,
                              &metadata_size);
    int error_code = create_object(client_context, object_ids[0], data_size,
                                   metadata_size, &objects[0]);
    CHECK(plasma_send_CreateReply(client_sock, state->builder, object_ids[0],
                                  &objects[0], error_code) >= 0);
    if (error_code == PlasmaError_OK) {
      CHECK(send_fd(client_sock, objects[0].handle.store_fd) >= 0);
    }
  } break;
  case MessageType_PlasmaGetRequest: {
    plasma_read_GetRequest(input, object_ids, 1);
    if (get_object(client_context, client_sock, object_ids[0], &objects[0]) ==
        OBJECT_FOUND) {
      CHECK(plasma_send_GetReply(client_sock, state->builder, object_ids,
                                 objects, 1) >= 0);
      CHECK(send_fd(client_sock, objects[0].handle.store_fd) >= 0);
    }
  } break;
  case MessageType_PlasmaGetLocalRequest: {
    plasma_read_GetLocalRequest(input, &object_ids[0], 1);
    if (get_object_local(client_context, client_sock, object_ids[0],
                         &objects[0]) == OBJECT_FOUND) {
      int has_object = 1;
      CHECK(plasma_send_GetLocalReply(client_sock, state->builder, object_ids,
                                      objects, &has_object, 1) >= 0);
      CHECK(send_fd(client_sock, objects[0].handle.store_fd) >= 0);
    } else {
      int has_object = 0;
      CHECK(plasma_send_GetLocalReply(client_sock, state->builder, object_ids,
                                      objects, &has_object, 1) >= 0);
    }
  } break;
  case MessageType_PlasmaReleaseRequest:
    plasma_read_ReleaseRequest(input, &object_ids[0]);
    release_object(client_context, object_ids[0]);
    break;
  case MessageType_PlasmaContainsRequest:
    plasma_read_ContainsRequest(input, &object_ids[0]);
    if (contains_object(client_context, object_ids[0]) == OBJECT_FOUND) {
      CHECK(plasma_send_ContainsReply(client_sock, state->builder,
                                      object_ids[0], 1) >= 0);
    } else {
      CHECK(plasma_send_ContainsReply(client_sock, state->builder,
                                      object_ids[0], 0) >= 0);
    }
    break;
  case MessageType_PlasmaSealRequest: {
    unsigned char digest[DIGEST_SIZE];
    plasma_read_SealRequest(input, &object_ids[0], &digest[0]);
    seal_object(client_context, object_ids[0], &digest[0]);
  } break;
  case MessageType_PlasmaEvictRequest: {
    /* This code path should only be used for testing. */
    int64_t num_bytes;
    plasma_read_EvictRequest(input, &num_bytes);
    int64_t num_objects_to_evict;
    object_id *objects_to_evict;
    int64_t num_bytes_evicted = choose_objects_to_evict(
        client_context->plasma_state->eviction_state,
        client_context->plasma_state->plasma_store_info, num_bytes,
        &num_objects_to_evict, &objects_to_evict);
    remove_objects(client_context->plasma_state, num_objects_to_evict,
                   objects_to_evict);
    CHECK(plasma_send_EvictReply(client_sock, state->builder,
                                 num_bytes_evicted) >= 0);
  } break;
  case MessageType_PlasmaSubscribeRequest:
    subscribe_to_updates(client_context, client_sock);
    break;
  case MessageType_PlasmaConnectRequest:
    CHECK(plasma_send_ConnectReply(client_sock, state->builder,
                                   state->plasma_store_info->memory_capacity) >=
          0);
    break;
  case DISCONNECT_CLIENT: {
    LOG_DEBUG("Disconnecting client on fd %d", client_sock);
    event_loop_remove_file(loop, client_sock);
    /* If this client was using any objects, remove it from the appropriate
     * lists. */
    plasma_store_state *plasma_state = client_context->plasma_state;
    object_table_entry *entry, *temp_entry;
    HASH_ITER(handle, plasma_state->plasma_store_info->objects, entry,
              temp_entry) {
      remove_client_from_object_clients(entry, client_context);
    }
  } break;
  default:
    /* This code should be unreachable. */
    CHECK(0);
  }
}

void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events) {
  plasma_store_state *plasma_state = context;
  int new_socket = accept_client(listener_sock);
  /* Create a new client object. This will also be used as the context to use
   * for events on this client's socket. TODO(rkn): free this somewhere. */
  client *client_context = (client *) malloc(sizeof(client));
  client_context->sock = new_socket;
  client_context->plasma_state = plasma_state;
  /* Add a callback to handle events on this socket. */
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message,
                      client_context);
  LOG_DEBUG("new connection with fd %d", new_socket);
}

/* Report "success" to valgrind. */
void signal_handler(int signal) {
  if (signal == SIGTERM) {
    exit(0);
  }
}

void start_server(char *socket_name, int64_t system_memory) {
  event_loop *loop = event_loop_create();
  plasma_store_state *state = init_plasma_store(loop, system_memory);
  int socket = bind_ipc_sock(socket_name, true);
  CHECK(socket >= 0);
  event_loop_add_file(loop, socket, EVENT_LOOP_READ, new_client_connection,
                      state);
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  char *socket_name = NULL;
  int64_t system_memory = -1;
  int c;
  while ((c = getopt(argc, argv, "s:m:")) != -1) {
    switch (c) {
    case 's':
      socket_name = optarg;
      break;
    case 'm': {
      char extra;
      int scanned = sscanf(optarg, "%" SCNd64 "%c", &system_memory, &extra);
      CHECK(scanned == 1);
      LOG_INFO("Allowing the Plasma store to use up to %.2fGB of memory.",
               ((double) system_memory) / 1000000000);
      break;
    }
    default:
      exit(-1);
    }
  }
  if (!socket_name) {
    LOG_FATAL("please specify socket for incoming connections with -s switch");
  }
  if (system_memory == -1) {
    LOG_FATAL("please specify the amount of system memory with -m switch");
  }
#ifdef __linux__
  int shm_fd = open("/dev/shm", O_RDONLY);
  struct statvfs shm_vfs_stats;
  fstatvfs(shm_fd, &shm_vfs_stats);
  int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
  close(shm_fd);
  if (system_memory > shm_mem_avail) {
    LOG_FATAL("system memory request exceeds memory available in /dev/shm");
  }
#endif
  LOG_DEBUG("starting server listening on %s", socket_name);
  start_server(socket_name, system_memory);
}
