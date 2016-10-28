/* PLASMA MANAGER: Local to a node, connects to other managers to send and
 * receive objects from them
 *
 * The storage manager listens on its main listening port, and if a request for
 * transfering an object to another object store comes in, it ships the data
 * using a new connection to the target object manager. */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <poll.h>
#include <assert.h>
#include <netinet/in.h>
#include <netdb.h>

#include "uthash.h"
#include "utlist.h"
#include "utarray.h"
#include "utstring.h"
#include "common.h"
#include "io.h"
#include "event_loop.h"
#include "plasma.h"
#include "plasma_client.h"
#include "plasma_manager.h"
#include "state/db.h"
#include "state/object_table.h"

typedef struct client_object_connection client_object_connection;

struct plasma_manager_state {
  /** Event loop. */
  event_loop *loop;
  /** Connection to the local plasma store for reading or writing data. */
  plasma_connection *plasma_conn;
  /** Hash table of all contexts for active connections to
   *  other plasma managers. These are used for writing data to
   *  other plasma stores. */
  client_connection *manager_connections;
  db_handle *db;
  /** Our address. */
  uint8_t addr[4];
  /** Our port. */
  int port;
  /** Hash table of outstanding fetch requests. The key is
   *  object id, value is a list of connections to the clients
   *  who are blocking on a fetch of this object. */
  client_object_connection *fetch_connections;
};

plasma_manager_state *g_manager_state = NULL;

/* The context for fetch and wait requests. These are per client, per object. */
struct client_object_connection {
  /** The ID of the object we are fetching or waiting for. */
  object_id object_id;
  /** The client connection context, shared between other
   *  client_object_connections for the same client. */
  client_connection *client_conn;
  /** The ID for the timer that will time out the current request to the state
   *  database or another plasma manager. */
  int64_t timer;
  /** How many retries we have left for the request. Decremented on every
   *  timeout. */
  int num_retries;
  /** Handle for a linked list. */
  client_object_connection *next;
  /** Pointer to the array containing the manager locations of
   *  this object. */
  char **manager_vector;
  /** The number of manager locations in the array manager_vector. */
  int manager_count;
  /** The next manager we should try to contact. This is set to an index in
   * manager_vector in the retry handler, in case the current attempt fails to
   * contact a manager. */
  int next_manager;
  /** Handle for the uthash table in the client connection
   *  context that keeps track of active object connection
   *  contexts. */
  UT_hash_handle active_hh;
  /** Handle for the uthash table in the manager state that
   *  keeps track of outstanding fetch requests. */
  UT_hash_handle fetch_hh;
};

/* Context for a client connection to another plasma manager. */
struct client_connection {
  /** Current state for this plasma manager. This is shared
   *  between all client connections to the plasma manager. */
  plasma_manager_state *manager_state;
  /** Current position in the buffer. */
  int64_t cursor;
  /** Buffer that this connection is reading from. If this is a connection to
   *  write data to another plasma store, then it is a linked
   *  list of buffers to write. */
  /* TODO(swang): Split into two queues, data transfers and data requests. */
  plasma_request_buffer *transfer_queue;
  /** File descriptor for the socket connected to the other
   *  plasma manager. */
  int fd;
  /** The objects that we are waiting for and their callback
   *  contexts, for either a fetch or a wait operation. */
  client_object_connection *active_objects;
  /** The number of objects that we have left to return for
   *  this fetch or wait operation. */
  int num_return_objects;
  /** Fields specific to connections to plasma managers.  Key that uniquely
   * identifies the plasma manager that we're connected to. We will use the
   * string <address>:<port> as an identifier. */
  char *ip_addr_port;
  /** Handle for the uthash table. */
  UT_hash_handle manager_hh;
};

void free_client_object_connection(client_object_connection *object_conn) {
  for (int i = 0; i < object_conn->manager_count; ++i) {
    free(object_conn->manager_vector[i]);
  }
  free(object_conn->manager_vector);
  free(object_conn);
}

int send_client_reply(client_connection *conn, plasma_reply *reply) {
  CHECK(conn->num_return_objects >= 0);
  --conn->num_return_objects;
  /* TODO(swang): Handle errors in write. */
  int n = write(conn->fd, (uint8_t *) reply, sizeof(plasma_reply));
  return (n != sizeof(plasma_reply));
}

/**
 * Get the context for the given object ID for the given client
 * connection, if there is one active.
 *
 * @param client_conn The client connection context.
 * @param object_id The object ID whose context we want.
 * @return A pointer to the active object context, or NULL if
 *         there isn't one.
 */
client_object_connection *get_object_connection(client_connection *client_conn,
                                                object_id object_id) {
  client_object_connection *object_conn;
  HASH_FIND(active_hh, client_conn->active_objects, &object_id,
            sizeof(object_id), object_conn);
  return object_conn;
}

client_object_connection *add_object_connection(client_connection *client_conn,
                                                object_id object_id) {
  /* TODO(swang): Support registration of wait operations. */
  /* Create a new context for this client connection and object. */
  client_object_connection *object_conn =
      malloc(sizeof(client_object_connection));
  if (!object_conn) {
    return NULL;
  }
  object_conn->object_id = object_id;
  object_conn->client_conn = client_conn;
  object_conn->manager_count = 0;
  object_conn->manager_vector = NULL;
  object_conn->next_manager = 0;
  /* Register the object context with the client context. */
  HASH_ADD(active_hh, client_conn->active_objects, object_id, sizeof(object_id),
           object_conn);
  /* Register the object context with the manager state. */
  client_object_connection *fetch_connections;
  HASH_FIND(fetch_hh, client_conn->manager_state->fetch_connections, &object_id,
            sizeof(object_id), fetch_connections);
  LOG_DEBUG("Registering fd %d for fetch.", client_conn->fd);
  if (!fetch_connections) {
    fetch_connections = NULL;
    LL_APPEND(fetch_connections, object_conn);
    HASH_ADD(fetch_hh, client_conn->manager_state->fetch_connections, object_id,
             sizeof(object_id), fetch_connections);
  } else {
    LL_APPEND(fetch_connections, object_conn);
  }
  return object_conn;
}

void remove_object_connection(client_connection *client_conn,
                              client_object_connection *object_conn) {
  /* Deregister the object context with the client context. */
  HASH_DELETE(active_hh, client_conn->active_objects, object_conn);
  /* Deregister the object context with the manager state. */
  client_object_connection *object_conns;
  HASH_FIND(fetch_hh, client_conn->manager_state->fetch_connections,
            &(object_conn->object_id), sizeof(object_conn->object_id),
            object_conns);
  CHECK(object_conns);
  int len;
  client_object_connection *tmp;
  LL_COUNT(object_conns, tmp, len);
  if (len == 1) {
    HASH_DELETE(fetch_hh, client_conn->manager_state->fetch_connections,
                object_conns);
  }
  LL_DELETE(object_conns, object_conn);
  /* Free the object. */
  free_client_object_connection(object_conn);
}

/* Helper function to parse a string of the form <IP address>:<port> into the
 * given ip_addr and port pointers. The ip_addr buffer must already be
 * allocated. */
/* TODO(swang): Move this function to Ray common. */
void parse_ip_addr_port(const char *ip_addr_port, char *ip_addr, int *port) {
  char port_str[6];
  int parsed = sscanf(ip_addr_port, "%15[0-9.]:%5[0-9]", ip_addr, port_str);
  CHECK(parsed == 2);
  *port = atoi(port_str);
}

plasma_manager_state *init_plasma_manager_state(const char *store_socket_name,
                                                const char *manager_addr,
                                                int manager_port,
                                                const char *db_addr,
                                                int db_port) {
  plasma_manager_state *state = malloc(sizeof(plasma_manager_state));
  state->loop = event_loop_create();
  state->plasma_conn = plasma_connect(store_socket_name, NULL, 0);
  state->manager_connections = NULL;
  state->fetch_connections = NULL;
  if (db_addr) {
    state->db = db_connect(db_addr, db_port, "plasma_manager", manager_addr,
                           manager_port);
    db_attach(state->db, state->loop);
    LOG_DEBUG("Connected to db at %s:%d, assigned client ID %d", db_addr,
              db_port, get_client_id(state->db));
  } else {
    state->db = NULL;
    LOG_DEBUG("No db connection specified");
  }
  sscanf(manager_addr, "%hhu.%hhu.%hhu.%hhu", &state->addr[0], &state->addr[1],
         &state->addr[2], &state->addr[3]);
  state->port = manager_port;
  return state;
}

void destroy_plasma_manager_state(plasma_manager_state *state) {
  client_connection *manager_conn, *tmp;
  HASH_ITER(manager_hh, state->manager_connections, manager_conn, tmp) {
    HASH_DELETE(manager_hh, state->manager_connections, manager_conn);
    plasma_request_buffer *head = manager_conn->transfer_queue;
    while (head) {
      LL_DELETE(manager_conn->transfer_queue, head);
      free(head);
      head = manager_conn->transfer_queue;
    }
    close(manager_conn->fd);
    free(manager_conn->ip_addr_port);
    free(manager_conn);
  }
  /* There should not be any outstanding client connections if
   * we're shutting down. */
  CHECK(state->fetch_connections == NULL);

  free(state->plasma_conn);
  event_loop_destroy(state->loop);
  free(state);
}

event_loop *get_event_loop(plasma_manager_state *state) {
  return state->loop;
}

/* Handle a command request that came in through a socket (transfering data,
 * or accepting incoming data). */
void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events);

void write_object_chunk(client_connection *conn, plasma_request_buffer *buf) {
  LOG_DEBUG("Writing data to fd %d", conn->fd);
  ssize_t r, s;
  /* Try to write one BUFSIZE at a time. */
  s = buf->data_size + buf->metadata_size - conn->cursor;
  if (s > BUFSIZE)
    s = BUFSIZE;
  r = write(conn->fd, buf->data + conn->cursor, s);

  if (r != s) {
    if (r > 0) {
      LOG_ERR("partial write on fd %d", conn->fd);
    } else {
      LOG_ERR("write error");
      exit(-1);
    }
  } else {
    conn->cursor += r;
  }
  if (r == 0) {
    /* If we've finished writing this buffer, reset the cursor to zero. */
    LOG_DEBUG("writing on channel %d finished", conn->fd);
    conn->cursor = 0;
    /* We are done sending the object, so release it. The corresponding call to
     * plasma_get occurred in process_transfer_request. */
    plasma_release(conn->manager_state->plasma_conn, buf->object_id);
  }
}

void send_queued_request(event_loop *loop,
                         int data_sock,
                         void *context,
                         int events) {
  client_connection *conn = (client_connection *) context;
  if (conn->transfer_queue == NULL) {
    /* If there are no objects to transfer, temporarily remove this connection
     * from the event loop. It will be reawoken when we receive another
     * PLASMA_TRANSFER request. */
    event_loop_remove_file(loop, conn->fd);
    return;
  }

  plasma_request_buffer *buf = conn->transfer_queue;
  plasma_request manager_req = make_plasma_request(buf->object_id);
  switch (buf->type) {
  case PLASMA_TRANSFER:
    LOG_DEBUG("Requesting transfer on DB client %d",
              get_client_id(conn->manager_state->db));
    memcpy(manager_req.addr, conn->manager_state->addr,
           sizeof(manager_req.addr));
    manager_req.port = conn->manager_state->port;
    plasma_send_request(conn->fd, buf->type, &manager_req);
    break;
  case PLASMA_DATA:
    LOG_DEBUG("Transferring object to manager");
    if (conn->cursor == 0) {
      /* If the cursor is zero, we haven't sent any requests for this object
       * yet,
       * so send the initial PLASMA_DATA request. */
      manager_req.data_size = buf->data_size;
      manager_req.metadata_size = buf->metadata_size;
      plasma_send_request(conn->fd, PLASMA_DATA, &manager_req);
    }
    write_object_chunk(conn, buf);
    break;
  default:
    LOG_ERR("Buffered request has unknown type.");
  }

  /* We are done sending this request. */
  if (conn->cursor == 0) {
    LL_DELETE(conn->transfer_queue, buf);
    free(buf);
  }
}

int read_object_chunk(client_connection *conn, plasma_request_buffer *buf) {
  LOG_DEBUG("Reading data from fd %d to %p", conn->fd,
            buf->data + conn->cursor);
  ssize_t r, s;
  CHECK(buf != NULL);
  /* Try to read one BUFSIZE at a time. */
  s = buf->data_size + buf->metadata_size - conn->cursor;
  if (s > BUFSIZE) {
    s = BUFSIZE;
  }
  r = read(conn->fd, buf->data + conn->cursor, s);

  if (r == -1) {
    LOG_ERR("read error");
  } else if (r == 0) {
    LOG_DEBUG("end of file");
  } else {
    conn->cursor += r;
  }
  /* If the cursor is equal to the full object size, reset the cursor and we're
   * done. */
  if (conn->cursor == buf->data_size + buf->metadata_size) {
    conn->cursor = 0;
    return 1;
  } else {
    return 0;
  }
}

void process_data_chunk(event_loop *loop,
                        int data_sock,
                        void *context,
                        int events) {
  /* Read the object chunk. */
  client_connection *conn = (client_connection *) context;
  plasma_request_buffer *buf = conn->transfer_queue;
  int done = read_object_chunk(conn, buf);
  if (!done) {
    return;
  }

  /* Seal the object and release it. The release corresponds to the call to
   * plasma_create that occurred in process_data_request. */
  LOG_DEBUG("reading on channel %d finished", data_sock);
  plasma_seal(conn->manager_state->plasma_conn, buf->object_id);
  plasma_release(conn->manager_state->plasma_conn, buf->object_id);
  /* Notify any clients who were waiting on a fetch to this object. */
  client_object_connection *object_conn, *next;
  client_connection *client_conn;
  HASH_FIND(fetch_hh, conn->manager_state->fetch_connections, &(buf->object_id),
            sizeof(buf->object_id), object_conn);
  plasma_reply reply = {.object_id = buf->object_id, .has_object = 1};
  while (object_conn) {
    next = object_conn->next;
    client_conn = object_conn->client_conn;
    send_client_reply(client_conn, &reply);
    event_loop_remove_timer(client_conn->manager_state->loop,
                            object_conn->timer);
    remove_object_connection(client_conn, object_conn);
    object_conn = next;
  }
  /* Remove the request buffer used for reading this object's data. */
  LL_DELETE(conn->transfer_queue, buf);
  free(buf);
  /* Switch to listening for requests from this socket, instead of reading
   * object data. */
  event_loop_remove_file(loop, data_sock);
  event_loop_add_file(loop, data_sock, EVENT_LOOP_READ, process_message, conn);
}

client_connection *get_manager_connection(plasma_manager_state *state,
                                          const char *ip_addr,
                                          int port) {
  /* TODO(swang): Should probably check whether ip_addr and port belong to us.
   */
  UT_string *ip_addr_port;
  utstring_new(ip_addr_port);
  utstring_printf(ip_addr_port, "%s:%d", ip_addr, port);
  client_connection *manager_conn;
  HASH_FIND(manager_hh, state->manager_connections, utstring_body(ip_addr_port),
            utstring_len(ip_addr_port), manager_conn);
  LOG_DEBUG("Getting manager connection to %s on DB client %d",
            utstring_body(ip_addr_port), get_client_id(state->db));
  if (!manager_conn) {
    /* If we don't already have a connection to this manager, start one. */
    int fd = plasma_manager_connect(ip_addr, port);
    /* TODO(swang): Handle the case when connection to this manager was
     * unsuccessful. */
    CHECK(fd >= 0);
    manager_conn = malloc(sizeof(client_connection));
    manager_conn->fd = fd;
    manager_conn->manager_state = state;
    manager_conn->transfer_queue = NULL;
    manager_conn->cursor = 0;
    manager_conn->ip_addr_port = strdup(utstring_body(ip_addr_port));
    HASH_ADD_KEYPTR(manager_hh,
                    manager_conn->manager_state->manager_connections,
                    manager_conn->ip_addr_port,
                    strlen(manager_conn->ip_addr_port), manager_conn);
  }
  utstring_free(ip_addr_port);
  return manager_conn;
}

void process_transfer_request(event_loop *loop,
                              object_id object_id,
                              uint8_t addr[4],
                              int port,
                              client_connection *conn) {
  uint8_t *data;
  int64_t data_size;
  uint8_t *metadata;
  int64_t metadata_size;
  /* TODO(swang): A non-blocking plasma_get, or else we could block here
   * forever if we don't end up sealing this object. */
  /* The corresponding call to plasma_release will happen in
   * write_object_chunk. */
  plasma_get(conn->manager_state->plasma_conn, object_id, &data_size, &data,
             &metadata_size, &metadata);
  assert(metadata == data + data_size);
  plasma_request_buffer *buf = malloc(sizeof(plasma_request_buffer));
  buf->type = PLASMA_DATA;
  buf->object_id = object_id;
  buf->data = data; /* We treat this as a pointer to the
                       concatenated data and metadata. */
  buf->data_size = data_size;
  buf->metadata_size = metadata_size;

  UT_string *ip_addr;
  utstring_new(ip_addr);
  utstring_printf(ip_addr, "%d.%d.%d.%d", addr[0], addr[1], addr[2], addr[3]);
  client_connection *manager_conn =
      get_manager_connection(conn->manager_state, utstring_body(ip_addr), port);
  utstring_free(ip_addr);

  if (manager_conn->transfer_queue == NULL) {
    /* If we already have a connection to this manager and its inactive,
     * (re)register it with the event loop again. */
    event_loop_add_file(loop, manager_conn->fd, EVENT_LOOP_WRITE,
                        send_queued_request, manager_conn);
  }
  /* Add this transfer request to this connection's transfer queue. */
  LL_APPEND(manager_conn->transfer_queue, buf);
}

void process_data_request(event_loop *loop,
                          int client_sock,
                          object_id object_id,
                          int64_t data_size,
                          int64_t metadata_size,
                          client_connection *conn) {
  plasma_request_buffer *buf = malloc(sizeof(plasma_request_buffer));
  buf->object_id = object_id;
  buf->data_size = data_size;
  buf->metadata_size = metadata_size;

  /* The corresponding call to plasma_release should happen in
   * process_data_chunk. */
  plasma_create(conn->manager_state->plasma_conn, object_id, data_size, NULL,
                metadata_size, &(buf->data));
  LL_APPEND(conn->transfer_queue, buf);
  CHECK(conn->cursor == 0);

  /* Switch to reading the data from this socket, instead of listening for
   * other requests. */
  event_loop_remove_file(loop, client_sock);
  event_loop_add_file(loop, client_sock, EVENT_LOOP_READ, process_data_chunk,
                      conn);
}

/**
 * Request a transfer for the given object ID from the next manager believed to
 * have a copy. Adds the request for this object ID to the queue of outgoing
 * requests to the manager we want to try.
 *
 * @param client_conn The context for the connection to this client.
 * @param object_id The object ID we want to request a transfer of.
 * @returns Void.
 */
void request_transfer_from(client_connection *client_conn,
                           object_id object_id) {
  client_object_connection *object_conn =
      get_object_connection(client_conn, object_id);
  CHECK(object_conn);
  CHECK(object_conn->manager_count > 0);
  CHECK(object_conn->next_manager >= 0 &&
        object_conn->next_manager < object_conn->manager_count);
  char addr[16];
  int port;
  parse_ip_addr_port(object_conn->manager_vector[object_conn->next_manager],
                     addr, &port);

  client_connection *manager_conn =
      get_manager_connection(client_conn->manager_state, addr, port);
  plasma_request_buffer *transfer_request =
      malloc(sizeof(plasma_request_buffer));
  transfer_request->type = PLASMA_TRANSFER;
  transfer_request->object_id = object_conn->object_id;

  if (manager_conn->transfer_queue == NULL) {
    /* If we already have a connection to this manager and its inactive,
     * (re)register it with the event loop. */
    event_loop_add_file(client_conn->manager_state->loop, manager_conn->fd,
                        EVENT_LOOP_WRITE, send_queued_request, manager_conn);
  }
  /* Add this transfer request to this connection's transfer queue. */
  LL_APPEND(manager_conn->transfer_queue, transfer_request);
  /* On the next attempt, try the next manager in manager_vector. */
  ++object_conn->next_manager;
  object_conn->next_manager %= object_conn->manager_count;
}

int manager_timeout_handler(event_loop *loop, timer_id id, void *context) {
  client_object_connection *object_conn = context;
  client_connection *client_conn = object_conn->client_conn;
  LOG_DEBUG("Timer went off, %d tries left", object_conn->num_retries);
  if (object_conn->num_retries > 0) {
    request_transfer_from(client_conn, object_conn->object_id);
    object_conn->num_retries--;
    return MANAGER_TIMEOUT;
  }
  plasma_reply reply = {.object_id = object_conn->object_id, .has_object = 0};
  send_client_reply(client_conn, &reply);
  remove_object_connection(client_conn, object_conn);
  return AE_NOMORE;
}

/* TODO(swang): Consolidate transfer requests for same object
 * from different client IDs by passing in manager state, not
 * client context. */
void request_transfer(object_id object_id,
                      int manager_count,
                      const char *manager_vector[],
                      void *context) {
  client_connection *client_conn = (client_connection *) context;
  client_object_connection *object_conn =
      get_object_connection(client_conn, object_id);
  CHECK(object_conn);
  LOG_DEBUG("Object is on %d managers", manager_count);
  if (manager_count == 0) {
    /* TODO(swang): Instead of immediately counting this as a failure, maybe
     * register a Redis callback for changes to this object table entry. */
    free(manager_vector);
    plasma_reply reply = {.object_id = object_conn->object_id, .has_object = 0};
    send_client_reply(client_conn, &reply);
    remove_object_connection(client_conn, object_conn);
    return;
  }
  /* Pick a different manager to request a transfer from on every attempt. */
  object_conn->manager_count = manager_count;
  object_conn->manager_vector = malloc(manager_count * sizeof(char *));
  memset(object_conn->manager_vector, 0, manager_count * sizeof(char *));
  for (int i = 0; i < manager_count; ++i) {
    int len = strlen(manager_vector[i]);
    object_conn->manager_vector[i] = malloc(len + 1);
    strncpy(object_conn->manager_vector[i], manager_vector[i], len);
    object_conn->manager_vector[i][len] = '\0';
  }
  free(manager_vector);
  /* Wait for the object data for the default number of retries, which timeout
   * after a default interval. */
  object_conn->num_retries = NUM_RETRIES;
  object_conn->timer =
      event_loop_add_timer(client_conn->manager_state->loop, MANAGER_TIMEOUT,
                           manager_timeout_handler, object_conn);
  request_transfer_from(client_conn, object_id);
}

void process_fetch_request(client_connection *client_conn,
                           object_id object_id) {
  plasma_reply reply = {.object_id = object_id};
  if (client_conn->manager_state->db == NULL) {
    reply.has_object = 0;
    send_client_reply(client_conn, &reply);
    return;
  }
  /* Return success immediately if we already have this object. */
  int is_local = 0;
  plasma_contains(client_conn->manager_state->plasma_conn, object_id,
                  &is_local);
  if (is_local) {
    reply.has_object = 1;
    send_client_reply(client_conn, &reply);
    return;
  }
  /* Register the new context with the current client connection. */
  /* TODO(swang): If there is already an outstanding fetch request for this
   * object, exit now. */
  client_object_connection *object_conn =
      add_object_connection(client_conn, object_id);
  if (!object_conn) {
    LOG_DEBUG("Unable to allocate memory for object context.");
    reply.has_object = 0;
    send_client_reply(client_conn, &reply);
  }
  /* Request a transfer from a plasma manager that has this object. */
  object_table_lookup(client_conn->manager_state->db, object_id,
                      request_transfer, client_conn);
}

void process_fetch_requests(client_connection *client_conn,
                            int num_object_ids,
                            object_id object_ids[]) {
  for (int i = 0; i < num_object_ids; ++i) {
    ++client_conn->num_return_objects;
    process_fetch_request(client_conn, object_ids[i]);
  }
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  client_connection *conn = (client_connection *) context;

  int64_t type;
  int64_t length;
  plasma_request *req;
  read_message(client_sock, &type, &length, (uint8_t **) &req);

  switch (type) {
  case PLASMA_TRANSFER:
    process_transfer_request(loop, req->object_ids[0], req->addr, req->port,
                             conn);
    break;
  case PLASMA_DATA:
    LOG_DEBUG("Starting to stream data");
    process_data_request(loop, client_sock, req->object_ids[0], req->data_size,
                         req->metadata_size, conn);
    break;
  case PLASMA_FETCH:
    LOG_DEBUG("Processing fetch");
    process_fetch_requests(conn, req->num_object_ids, req->object_ids);
    break;
  case PLASMA_SEAL:
    LOG_DEBUG("Publishing to object table from DB client %d.",
              get_client_id(conn->manager_state->db));
    object_table_add(conn->manager_state->db, req->object_ids[0]);
    break;
  case DISCONNECT_CLIENT: {
    LOG_INFO("Disconnecting client on fd %d", client_sock);
    /* TODO(swang): Check if this connection was to a plasma manager. If so,
     * delete it. */
    event_loop_remove_file(loop, client_sock);
    close(client_sock);
    free(conn);
  } break;
  default:
    LOG_ERR("invalid request %" PRId64, type);
    exit(-1);
  }

  free(req);
}

client_connection *new_client_connection(event_loop *loop,
                                         int listener_sock,
                                         void *context,
                                         int events) {
  int new_socket = accept_client(listener_sock);
  /* Create a new data connection context per client. */
  client_connection *conn = malloc(sizeof(client_connection));
  conn->manager_state = (plasma_manager_state *) context;
  conn->cursor = 0;
  conn->transfer_queue = NULL;
  conn->fd = new_socket;
  conn->active_objects = NULL;
  conn->num_return_objects = 0;
  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message, conn);
  LOG_DEBUG("New plasma manager connection with fd %d", new_socket);
  return conn;
}

void handle_new_client(event_loop *loop,
                       int listener_sock,
                       void *context,
                       int events) {
  (void) new_client_connection(loop, listener_sock, context, events);
}

int get_client_sock(client_connection *conn) {
  return conn->fd;
}

void start_server(const char *store_socket_name,
                  const char *master_addr,
                  int port,
                  const char *db_addr,
                  int db_port) {
  g_manager_state = init_plasma_manager_state(store_socket_name, master_addr,
                                              port, db_addr, db_port);
  CHECK(g_manager_state);

  int sock = bind_inet_sock(port);
  CHECKM(sock >= 0, "Unable to bind to manager port");

  LOG_DEBUG("Started server connected to store %s, listening on port %d",
            store_socket_name, port);
  event_loop_add_file(g_manager_state->loop, sock, EVENT_LOOP_READ,
                      handle_new_client, g_manager_state);
  event_loop_run(g_manager_state->loop);
}

/* Report "success" to valgrind. */
void signal_handler(int signal) {
  if (signal == SIGTERM) {
    if (g_manager_state) {
      db_disconnect(g_manager_state->db);
    }
    exit(0);
  }
}

/* Only declare the main function if we are not in testing mode, since the test
 * suite has its own declaration of main. */
#ifndef PLASMA_TEST
int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* Socket name of the plasma store this manager is connected to. */
  char *store_socket_name = NULL;
  /* IP address of this node. */
  char *master_addr = NULL;
  /* Port number the manager should use. */
  int port;
  /* IP address and port of state database. */
  char *db_host = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:m:p:d:")) != -1) {
    switch (c) {
    case 's':
      store_socket_name = optarg;
      break;
    case 'm':
      master_addr = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'd':
      db_host = optarg;
      break;
    default:
      LOG_ERR("unknown option %c", c);
      exit(-1);
    }
  }
  if (!store_socket_name) {
    LOG_ERR(
        "please specify socket for connecting to the plasma store with -s "
        "switch");
    exit(-1);
  }
  if (!master_addr) {
    LOG_ERR(
        "please specify ip address of the current host in the format "
        "123.456.789.10 with -m switch");
    exit(-1);
  }
  char db_addr[16];
  int db_port;
  if (db_host) {
    parse_ip_addr_port(db_host, db_addr, &db_port);
    start_server(store_socket_name, master_addr, port, db_addr, db_port);
  } else {
    start_server(store_socket_name, master_addr, port, NULL, 0);
  }
}
#endif
