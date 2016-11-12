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
#include <stdbool.h>
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
#include "net.h"
#include "event_loop.h"
#include "plasma.h"
#include "plasma_client.h"
#include "plasma_manager.h"
#include "state/db.h"
#include "state/object_table.h"

void wait_object_lookup_callback(object_id object_id,
                                 int manager_count,
                                 const char *manager_vector[],
                                 void *context);

/**
 * Process either the fetch or the status request.
 *
 * @param client_conn Client connection.
 * @param object_id ID of the object for which we process this request.
 * @param fetc If true we process a fetch request, and if false
 *             we process a status request.
 * @return Void.
 */
void process_fetch_or_status_request(client_connection *client_conn,
                                     object_id object_id,
                                     bool fetch);
/**
 * Request a transfer for the given object ID from the next manager believed to
 * have a copy. Adds the request for this object ID to the queue of outgoing
 * requests to the manager we want to try.
 *
 * requst_trasnfer_from() will lead to thic Plasma manager, call it S,
 * (1) sending a PLASMA_TRANFER request for object_id to the other
 *     end-point, R, of the client_conn. R is a remote Plasma manager
 *     which is expected to store object_id.
 * (2) upon receiving this request, R will invoke process_transfer_request()
 *     which will send a PLASMA_DATA request containing object_id back to
 *     S.
 * (3) Upen receiving the PLASMA_DATA request, S, will invoke
 *     process_data_request() (via process_data_chunk()) to read object_id.
 * Note that all requests that are exchanged between S and R are via FIFO
 * queues.
 *
 * @param client_conn The context for the connection to this client.
 * @param object_id The object ID we want to request a transfer of.
 * @returns Void.
 */
void request_transfer_from(client_connection *client_conn, object_id object_id);

/**
 * Request the transfer from a remote node or get the status of
 * a given object. This is called for an object that is stored at
 * a remote Plasma Store.
 *
 * @param object_id ID of the object to transfer or to get its status.
 * @param manager_cont Number of remote nodes object_id is stored at.
 * @param manager_vector Array containing the Plasma Managers
 *                       running at the nodes where object_id is stored.
 * @param context Client connection.
 * @param fetch If true, this was triggered by a fetc operation. If not.
 *              we request its status.
 * @return Status of object_id as defined in plasma.h
 */
int request_fetch_or_status(object_id object_id,
                            int manager_count,
                            const char *manager_vector[],
                            void *context,
                            bool fetch);

/**
 * Send requested object_id back to the Plasma Manager identified
 * by (addr, port) which requested it. This is done via a
 * PLASMA_DATA message.
 *
 * @param loop
 * @param object_id The ID of the object being transferred to (addr, port).
 * @param addr The address of the Plasma Manager object_id is sent to.
 * @param port The port number of the Plasma Manager object_id is sent to.
 * @param conn The client connection object.
 */
void process_transfer_request(event_loop *loop,
                              object_id object_id,
                              uint8_t addr[4],
                              int port,
                              client_connection *conn);

/**
 * Receive object_id requested by this Plamsa Manager from the remote Plasma
 * Manager identified by client_sock. The object_id is sent via the PLASMA_DATA
 * message.
 *
 * @param loop The event data structure.
 * @param client_sock The sender's socket.
 * @param object_id ID of the object being received.
 * @param data_size Size of the data of object_id.
 * @param metadata_size Size of the metadata of object_id.
 * @param conn The connection object.
 */
void process_data_request(event_loop *loop,
                          int client_sock,
                          object_id object_id,
                          int64_t data_size,
                          int64_t metadata_size,
                          client_connection *conn);

/** Entry of the hashtable of objects that are available locally. */
typedef struct {
  /** Object id of this object. */
  object_id object_id;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} available_object;

typedef struct {
  /** The ID of the object we are fetching or waiting for. */
  object_id object_id;
  /** The plasma manager state. */
  plasma_manager_state *manager_state;
  /** The ID for the timer that will time out the current request to the state
   *  database or another plasma manager. */
  int64_t timer;
  /** Pointer to the array containing the manager locations of this object. This
   *  struct owns and must free each entry. */
  char **manager_vector;
  /** The number of manager locations in the array manager_vector. */
  int manager_count;
  /** The next manager we should try to contact. This is set to an index in
   *  manager_vector in the retry handler, in case the current attempt fails to
   *  contact a manager. */
  int next_manager;
  /** Handle for the uthash table in the manager state that keeps track of
   *  outstanding fetch requests. */
  UT_hash_handle hh;
} fetch_request2;

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
  client_object_request *fetch_requests;
  /** Hash table of outstanding fetch requests. The key is the object ID. The
   *  value is the data needed to perform the fetch. */
  fetch_request2 *fetch_requests2;
  /** Initialize an empty hash map for the cache of local available object. */
  available_object *local_available_objects;
};

plasma_manager_state *g_manager_state = NULL;

/* The context for fetch and wait requests. These are per client, per object. */
struct client_object_request {
  /** The ID of the object we are fetching or waiting for. */
  object_id object_id;
  /** The client connection context, shared between other
   *  client_object_requests for the same client. */
  client_connection *client_conn;
  /** The ID for the timer that will time out the current request to the state
   *  database or another plasma manager. */
  int64_t timer;
  /** How many retries we have left for the request. Decremented on every
   *  timeout. */
  int num_retries;
  /** Handle for a linked list. */
  client_object_request *next;
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
  /** Buffer used to receive transfers (data fetches) we want to ignore */
  plasma_request_buffer *ignore_buffer;
  /** File descriptor for the socket connected to the other
   *  plasma manager. */
  int fd;
  /** Timer id for timing out wait (or fetch). */
  int64_t timer_id;
  /** True if this client is in a "wait" and false if it is in a "fetch". */
  bool is_wait;
  /** True if we use new version of wait. */
  bool wait1;
  /** True if we use the new version of fetch. */
  bool fetch1;
  /** If this client is processing a wait, this contains the object ids that
   *  are already available. */
  plasma_reply *wait_reply;
  /** The objects that we are waiting for and their callback
   *  contexts, for either a fetch or a wait operation. */
  client_object_request *active_objects;
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

void free_client_object_request(client_object_request *object_req) {
  for (int i = 0; i < object_req->manager_count; ++i) {
    free(object_req->manager_vector[i]);
  }
  free(object_req->manager_vector);
  free(object_req);
}

void send_client_reply(client_connection *conn, plasma_reply *reply) {
  CHECK(conn->num_return_objects >= 0);
  --conn->num_return_objects;
  CHECK(plasma_send_reply(conn->fd, reply) >= 0);
}

void send_client_failure_reply(object_id object_id, client_connection *conn) {
  plasma_reply reply = plasma_make_reply(object_id);
  reply.has_object = 0;
  send_client_reply(conn, &reply);
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
client_object_request *get_object_request(client_connection *client_conn,
                                          object_id object_id) {
  client_object_request *object_req;
  HASH_FIND(active_hh, client_conn->active_objects, &object_id,
            sizeof(object_id), object_req);
  return object_req;
}

client_object_request *add_object_request(client_connection *client_conn,
                                          object_id object_id) {
  CHECK(client_conn);
  /* Create a new context for this client connection and object. */
  client_object_request *object_req = malloc(sizeof(client_object_request));
  CHECK(object_req);
  object_req->object_id = object_id;
  object_req->client_conn = client_conn;
  /* The timer ID returned by event_loop_add_timer is positive, so we can check
   * if the timer is -1 to see if a timer has been added. */
  object_req->timer = -1;
  object_req->manager_count = 0;
  object_req->manager_vector = NULL;
  object_req->next_manager = 0;
  /* Register the object context with the client context. */
  client_object_request *temp_object_conn = NULL;
  HASH_FIND(active_hh, client_conn->active_objects, &object_id,
            sizeof(object_id), temp_object_conn);
  CHECKM(temp_object_conn == NULL,
         "The hash table already has an object connection for this object ID.");
  HASH_ADD(active_hh, client_conn->active_objects, object_id, sizeof(object_id),
           object_req);
  /* Register the object context with the manager state. */
  client_object_request *fetch_requests;
  HASH_FIND(fetch_hh, client_conn->manager_state->fetch_requests, &object_id,
            sizeof(object_id), fetch_requests);
  LOG_DEBUG("Registering fd %d for fetch.", client_conn->fd);
  if (!fetch_requests) {
    fetch_requests = NULL;
    LL_APPEND(fetch_requests, object_req);
    HASH_ADD(fetch_hh, client_conn->manager_state->fetch_requests, object_id,
             sizeof(object_id), fetch_requests);
  } else {
    LL_APPEND(fetch_requests, object_req);
  }
  return object_req;
}

void remove_object_request(client_connection *client_conn,
                           client_object_request *object_req) {
  /* Deregister the object context with the client context. */
  /* TODO(rkn): Check that object_conn is actually in the hash table. */
  HASH_DELETE(active_hh, client_conn->active_objects, object_req);
  /* Deregister the object context with the manager state. */
  client_object_request *object_reqs;
  HASH_FIND(fetch_hh, client_conn->manager_state->fetch_requests,
            &(object_req->object_id), sizeof(object_req->object_id),
            object_reqs);
  CHECK(object_reqs);
  int len;
  client_object_request *tmp;
  LL_COUNT(object_reqs, tmp, len);
  if (len == 1) {
    HASH_DELETE(fetch_hh, client_conn->manager_state->fetch_requests,
                object_reqs);
  }
  LL_DELETE(object_reqs, object_req);
  /* remove_object_request() is not always called from the request's timer
   * handle, so we remove the request's timer explicitly here. If
   * remove_object_request() is called from the the request's timer handle, the
   * code will still work correctly. While the timer handle returning
   * EVENT_LOOP_TIMER_DONE will trigger another call for removing the request's
   * timer, that's ok as event_loop_remove_timer() is idempotent. */
  if (object_req->timer != -1) {
    event_loop_remove_timer(client_conn->manager_state->loop,
                            object_req->timer);
  }
  /* Free the object. */
  free_client_object_request(object_req);
}

void remove_fetch_request(plasma_manager_state *manager_state,
                          fetch_request2 *fetch_req) {
  /* Remove the fetch request from the table of fetch requests. */
  HASH_DELETE(hh, manager_state->fetch_requests2, fetch_req);
  /* Remove the timer associated with this fetch request. */
  if (fetch_req->timer != -1) {
    CHECK(event_loop_remove_timer(manager_state->loop, fetch_req->timer) ==
          AE_OK);
  }
  /* Free the fetch request and everything in it. */
  for (int i = 0; i < fetch_req->manager_count; ++i) {
    free(fetch_req->manager_vector[i]);
  }
  if (fetch_req->manager_vector != NULL) {
    free(fetch_req->manager_vector);
  }
  free(fetch_req);
}

plasma_manager_state *init_plasma_manager_state(const char *store_socket_name,
                                                const char *manager_addr,
                                                int manager_port,
                                                const char *db_addr,
                                                int db_port) {
  plasma_manager_state *state = malloc(sizeof(plasma_manager_state));
  state->loop = event_loop_create();
  state->plasma_conn =
      plasma_connect(store_socket_name, NULL, PLASMA_DEFAULT_RELEASE_DELAY);
  state->manager_connections = NULL;
  state->fetch_requests = NULL;
  state->fetch_requests2 = NULL;
  if (db_addr) {
    state->db = db_connect(db_addr, db_port, "plasma_manager", manager_addr,
                           manager_port);
    db_attach(state->db, state->loop, false);
  } else {
    state->db = NULL;
    LOG_DEBUG("No db connection specified");
  }
  sscanf(manager_addr, "%hhu.%hhu.%hhu.%hhu", &state->addr[0], &state->addr[1],
         &state->addr[2], &state->addr[3]);
  state->port = manager_port;
  /* Initialize an empty hash map for the cache of local available objects. */
  state->local_available_objects = NULL;
  /* Subscribe to notifications about sealed objects. */
  int plasma_fd = plasma_subscribe(state->plasma_conn);
  /* Add the callback that processes the notification to the event loop. */
  event_loop_add_file(state->loop, plasma_fd, EVENT_LOOP_READ,
                      process_object_notification, state);
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

  if (state->fetch_requests != NULL) {
    LOG_DEBUG("There were outstanding fetch requests.");
    client_object_request *object_req, *tmp;
    HASH_ITER(fetch_hh, state->fetch_requests, object_req, tmp) {
      remove_object_request(object_req->client_conn, object_req);
    }
  }

  if (state->fetch_requests2 != NULL) {
    fetch_request2 *fetch_req, *tmp;
    HASH_ITER(hh, state->fetch_requests2, fetch_req, tmp) {
      remove_fetch_request(fetch_req->manager_state, fetch_req);
    }
  }

  plasma_disconnect(state->plasma_conn);
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
      LOG_ERROR("partial write on fd %d", conn->fd);
    } else {
      /* TODO(swang): This should not be a fatal error, since connections can
       * close at any time. */
      LOG_FATAL("write error");
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
  plasma_request manager_req = plasma_make_request(buf->object_id);
  switch (buf->type) {
  case PLASMA_TRANSFER:
    memcpy(manager_req.addr, conn->manager_state->addr,
           sizeof(manager_req.addr));
    manager_req.port = conn->manager_state->port;
    CHECK(plasma_send_request(conn->fd, PLASMA_TRANSFER, &manager_req) >= 0);
    break;
  case PLASMA_DATA:
    LOG_DEBUG("Transferring object to manager");
    if (conn->cursor == 0) {
      /* If the cursor is zero, we haven't sent any requests for this object
       * yet,
       * so send the initial PLASMA_DATA request. */
      manager_req.data_size = buf->data_size;
      manager_req.metadata_size = buf->metadata_size;
      CHECK(plasma_send_request(conn->fd, PLASMA_DATA, &manager_req) >= 0);
    }
    write_object_chunk(conn, buf);
    break;
  default:
    LOG_FATAL("Buffered request has unknown type.");
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
    LOG_ERROR("read error");
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
  /* The following seal also triggers notification of clients for fetch or
   * wait requests, see process_object_notification. */
  plasma_seal(conn->manager_state->plasma_conn, buf->object_id);
  plasma_release(conn->manager_state->plasma_conn, buf->object_id);
  /* Remove the request buffer used for reading this object's data. */
  LL_DELETE(conn->transfer_queue, buf);
  free(buf);
  /* Switch to listening for requests from this socket, instead of reading
   * object data. */
  event_loop_remove_file(loop, data_sock);
  event_loop_add_file(loop, data_sock, EVENT_LOOP_READ, process_message, conn);
}

void ignore_data_chunk(event_loop *loop,
                       int data_sock,
                       void *context,
                       int events) {
  /* Read the object chunk. */
  client_connection *conn = (client_connection *) context;
  plasma_request_buffer *buf = conn->ignore_buffer;

  /* Just read the transferred data into ignore_buf and then drop (free) it. */
  int done = read_object_chunk(conn, buf);
  if (!done) {
    return;
  }

  free(buf->data);
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

/**
 * Receive object_id requested by this Plamsa Manager from the remote Plasma
 * Manager identified by client_sock. The object_id is sent via the PLASMA_DATA
 * message.
 *
 * @param loop The event data structure.
 * @param client_sock The sender's socket.
 * @param object_id ID of the object being received.
 * @param data_size Size of the data of object_id.
 * @param metadata_size Size of the metadata of object_id.
 * @param conn The connection object.
 * @return Void.
 */
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
  bool success_create =
      plasma_create(conn->manager_state->plasma_conn, object_id, data_size,
                    NULL, metadata_size, &(buf->data));
  /* If success_create == true, a new object has been created.
   * If success_create == false the object creation has failed, possibly
   * due to an object with the same ID already existing in the Plasma Store. */
  if (success_create) {
    /* Add buffer where the fetched data is to be stored to
     * conn->transfer_queue. */
    LL_APPEND(conn->transfer_queue, buf);
  }
  CHECK(conn->cursor == 0);

  /* Switch to reading the data from this socket, instead of listening for
   * other requests. */
  event_loop_remove_file(loop, client_sock);
  if (success_create) {
    event_loop_add_file(loop, client_sock, EVENT_LOOP_READ, process_data_chunk,
                        conn);
  } else {
    /* Since plasma_create() has failed, we ignore the data transfer. We will
     * receive this transfer in g_ignore_buf and then drop it. Allocate memory
     * for data and metadata, if needed. All memory associated with
     * buf/g_ignore_buf will be freed in ignore_data_chunkc(). */
    conn->ignore_buffer = buf;
    buf->data = (uint8_t *) malloc(buf->data_size + buf->metadata_size);
    event_loop_add_file(loop, client_sock, EVENT_LOOP_READ, ignore_data_chunk,
                        conn);
  }
}

void request_transfer_from(client_connection *client_conn,
                           object_id object_id) {
  client_object_request *object_req =
      get_object_request(client_conn, object_id);
  CHECK(object_req);
  CHECK(object_req->manager_count > 0);
  CHECK(object_req->next_manager >= 0 &&
        object_req->next_manager < object_req->manager_count);
  char addr[16];
  int port;
  parse_ip_addr_port(object_req->manager_vector[object_req->next_manager], addr,
                     &port);

  client_connection *manager_conn =
      get_manager_connection(client_conn->manager_state, addr, port);
  plasma_request_buffer *transfer_request =
      malloc(sizeof(plasma_request_buffer));
  transfer_request->type = PLASMA_TRANSFER;
  transfer_request->object_id = object_req->object_id;

  if (manager_conn->transfer_queue == NULL) {
    /* If we already have a connection to this manager and its inactive,
     * (re)register it with the event loop. */
    event_loop_add_file(client_conn->manager_state->loop, manager_conn->fd,
                        EVENT_LOOP_WRITE, send_queued_request, manager_conn);
  }
  /* Add this transfer request to this connection's transfer queue. */
  LL_APPEND(manager_conn->transfer_queue, transfer_request);
  /* On the next attempt, try the next manager in manager_vector. */
  object_req->next_manager += 1;
  object_req->next_manager %= object_req->manager_count;
}

void request_transfer_from2(plasma_manager_state *manager_state,
                            object_id object_id) {
  fetch_request2 *fetch_req;
  HASH_FIND(hh, manager_state->fetch_requests2, &object_id, sizeof(object_id),
            fetch_req);
  /* TODO(rkn): This probably can be NULL so we should remove this check, and
   * instead return in the case where there is no fetch request. */
  CHECK(fetch_req != NULL);

  CHECK(fetch_req->manager_count > 0);
  CHECK(fetch_req->next_manager >= 0 &&
        fetch_req->next_manager < fetch_req->manager_count);
  char addr[16];
  int port;
  parse_ip_addr_port(fetch_req->manager_vector[fetch_req->next_manager], addr,
                     &port);

  client_connection *manager_conn =
      get_manager_connection(manager_state, addr, port);
  plasma_request_buffer *transfer_request =
      malloc(sizeof(plasma_request_buffer));
  transfer_request->type = PLASMA_TRANSFER;
  transfer_request->object_id = fetch_req->object_id;

  if (manager_conn->transfer_queue == NULL) {
    /* If we already have a connection to this manager and its inactive,
     * (re)register it with the event loop. */
    event_loop_add_file(manager_state->loop, manager_conn->fd, EVENT_LOOP_WRITE,
                        send_queued_request, manager_conn);
  }
  /* Add this transfer request to this connection's transfer queue. */
  LL_APPEND(manager_conn->transfer_queue, transfer_request);
  /* On the next attempt, try the next manager in manager_vector. */
  fetch_req->next_manager += 1;
  fetch_req->next_manager %= fetch_req->manager_count;
}

int manager_timeout_handler(event_loop *loop, timer_id id, void *context) {
  client_object_request *object_req = context;
  client_connection *client_conn = object_req->client_conn;
  LOG_DEBUG("Timer went off, %d tries left", object_req->num_retries);
  if (object_req->num_retries > 0) {
    request_transfer_from(client_conn, object_req->object_id);
    object_req->num_retries--;
    return MANAGER_TIMEOUT;
  }
  plasma_reply reply = plasma_make_reply(object_req->object_id);
  reply.has_object = 0;
  send_client_reply(client_conn, &reply);
  remove_object_request(client_conn, object_req);
  return EVENT_LOOP_TIMER_DONE;
}

int manager_timeout_handler2(event_loop *loop, timer_id id, void *context) {
  fetch_request2 *fetch_req = context;
  plasma_manager_state *manager_state = fetch_req->manager_state;
  request_transfer_from2(manager_state, fetch_req->object_id);
  return MANAGER_TIMEOUT;
}

bool is_object_local(plasma_manager_state *state, object_id object_id) {
  available_object *entry;
  HASH_FIND(hh, state->local_available_objects, &object_id, sizeof(object_id),
            entry);
  return entry != NULL;
}

/* TODO(swang): Consolidate transfer requests for same object
 * from different client IDs by passing in manager state, not
 * client context. */
void request_transfer(object_id object_id,
                      int manager_count,
                      const char *manager_vector[],
                      void *context) {
  client_connection *client_conn = (client_connection *) context;
  client_object_request *object_req =
      get_object_request(client_conn, object_id);
  /* If there's already an outstanding fetch for this object for this client,
   * let the outstanding request finish the work. */
  if (object_req) {
    return;
  }
  /* If the object isn't on any managers, report a failure to the client. */
  LOG_DEBUG("Object is on %d managers", manager_count);
  if (manager_count == 0) {
    /* TODO(swang): Instead of immediately counting this as a failure, maybe
     * register a Redis callback for changes to this object table entry. */
    plasma_reply reply = plasma_make_reply(object_id);
    reply.object_status = PLASMA_OBJECT_NONEXISTENT;
    CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
    return;
  }
  /* Register the new outstanding fetch with the current client connection. */
  object_req = add_object_request(client_conn, object_id);
  if (!object_req) {
    LOG_DEBUG("Unable to allocate memory for object context.");
    send_client_failure_reply(object_id, client_conn);
  }
  /* Pick a different manager to request a transfer from on every attempt. */
  object_req->manager_count = manager_count;
  object_req->manager_vector = malloc(manager_count * sizeof(char *));
  memset(object_req->manager_vector, 0, manager_count * sizeof(char *));
  for (int i = 0; i < manager_count; ++i) {
    int len = strlen(manager_vector[i]);
    object_req->manager_vector[i] = malloc(len + 1);
    strncpy(object_req->manager_vector[i], manager_vector[i], len);
    object_req->manager_vector[i][len] = '\0';
  }
  /* Wait for the object data for the default number of retries, which timeout
   * after a default interval. */
  object_req->num_retries = NUM_RETRIES;
  object_req->timer =
      event_loop_add_timer(client_conn->manager_state->loop, MANAGER_TIMEOUT,
                           manager_timeout_handler, object_req);
  request_transfer_from(client_conn, object_id);
}

void request_transfer2(object_id object_id,
                       int manager_count,
                       const char *manager_vector[],
                       void *context) {
  plasma_manager_state *manager_state = (plasma_manager_state *) context;
  /* This callback is called from object_table_subscribe, which guarantees that
   * the manager vector contains at least one element. */
  CHECK(manager_count >= 1);
  fetch_request2 *fetch_req;
  HASH_FIND(hh, manager_state->fetch_requests2, &object_id, sizeof(object_id),
            fetch_req);

  if (is_object_local(manager_state, object_id)) {
    /* If the object is already here, then the fetch request should have been
     * removed. */
    CHECK(fetch_req == NULL);
    return;
  }

  /* If the object is not present, then the fetch request should still be here.
   * TODO(rkn): We actually have to remove this check to handle the rare
   * scenario where the object is transferred here and then evicted before this
   * callback gets called. */
  CHECK(fetch_req != NULL);

  /* This method may be run multiple times, so if we are updating the manager
   * vector, we need to free the previous manager vector. */
  if (fetch_req->manager_count != 0) {
    for (int i = 0; i < fetch_req->manager_count; ++i) {
      free(fetch_req->manager_vector[i]);
    }
    free(fetch_req->manager_vector);
  }
  /* Update the manager vector. */
  fetch_req->manager_count = manager_count;
  fetch_req->manager_vector = malloc(manager_count * sizeof(char *));
  fetch_req->next_manager = 0;
  memset(fetch_req->manager_vector, 0, manager_count * sizeof(char *));
  for (int i = 0; i < manager_count; ++i) {
    int len = strlen(manager_vector[i]);
    fetch_req->manager_vector[i] = malloc(len + 1);
    strncpy(fetch_req->manager_vector[i], manager_vector[i], len);
    fetch_req->manager_vector[i][len] = '\0';
  }
  /* Wait for the object data for the default number of retries, which timeout
   * after a default interval. */
  request_transfer_from2(manager_state, object_id);
  /* It is possible for this method to be called multiple times, but we only
   * need to create a timer once. */
  if (fetch_req->timer == -1) {
    fetch_req->timer =
        event_loop_add_timer(manager_state->loop, MANAGER_TIMEOUT,
                             manager_timeout_handler2, fetch_req);
  }
}

void process_fetch_request(client_connection *client_conn,
                           object_id object_id) {
  client_conn->is_wait = false;
  client_conn->fetch1 = false;
  client_conn->wait_reply = NULL;
  plasma_reply reply = plasma_make_reply(object_id);
  if (client_conn->manager_state->db == NULL) {
    reply.has_object = 0;
    send_client_reply(client_conn, &reply);
    return;
  }
  /* Return success immediately if we already have this object. */
  if (is_object_local(client_conn->manager_state, object_id)) {
    reply.has_object = 1;
    send_client_reply(client_conn, &reply);
    return;
  }
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = MANAGER_TIMEOUT,
      .fail_callback = (table_fail_callback) send_client_failure_reply,
  };
  /* Request a transfer from a plasma manager that has this object. */
  object_table_lookup(client_conn->manager_state->db, object_id, &retry,
                      request_transfer, client_conn);
}

void process_fetch_requests(client_connection *client_conn,
                            int num_object_ids,
                            object_request object_requests[]) {
  for (int i = 0; i < num_object_ids; ++i) {
    ++client_conn->num_return_objects;
    process_fetch_request(client_conn, object_requests[i].object_id);
  }
}

void fatal_table_callback(object_id id, void *user_context, void *user_data) {
  CHECK(0);
}

void process_fetch_requests2(client_connection *client_conn,
                             int num_object_ids,
                             object_request object_requests[]) {
  plasma_manager_state *manager_state = client_conn->manager_state;
  for (int i = 0; i < num_object_ids; ++i) {
    object_id obj_id = object_requests[i].object_id;

    /* Check if this object is already present locally. If so, do nothing. */
    if (is_object_local(manager_state, obj_id)) {
      continue;
    }

    /* Check if this object is already being fetched. If so, do nothing. */
    fetch_request2 *entry;
    HASH_FIND(hh, manager_state->fetch_requests2, &obj_id, sizeof(obj_id),
              entry);
    if (entry != NULL) {
      continue;
    }

    /* Add an entry to the fetch requests data structure to indidate that the
     * object is being fetched. */
    entry = malloc(sizeof(fetch_request2));
    entry->manager_state = manager_state;
    entry->object_id = obj_id;
    entry->timer = -1;
    entry->manager_count = 0;
    entry->manager_vector = NULL;
    HASH_ADD(hh, manager_state->fetch_requests2, object_id,
             sizeof(entry->object_id), entry);

    /* Get a list of Plasma Managers that have this object from the object
     * table. If the list of Plasma Managers is non-empty, the callback should
     * initiate a transfer. */
    /* TODO(rkn): Make sure this also handles the case where the list is
     * initially empty. */
    retry_info retry;
    memset(&retry, 0, sizeof(retry));
    retry.num_retries = NUM_RETRIES;
    retry.timeout = MANAGER_TIMEOUT;
    retry.fail_callback = fatal_table_callback;
    object_table_subscribe(manager_state->db, obj_id, request_transfer2,
                           manager_state, &retry, NULL, NULL);
  }
}

void return_from_wait(client_connection *client_conn) {
  CHECK(client_conn->is_wait);
  /* TODO: check for wait1. */
  client_conn->wait_reply->num_objects_returned =
      client_conn->wait_reply->num_object_ids - client_conn->num_return_objects;
  CHECK(plasma_send_reply(client_conn->fd, client_conn->wait_reply) >= 0);
  plasma_free_reply(client_conn->wait_reply);
  /* Clean the remaining object connections. */
  client_object_request *object_req, *tmp;
  HASH_ITER(active_hh, client_conn->active_objects, object_req, tmp) {
    remove_object_request(client_conn, object_req);
  }
}

int wait_timeout_handler(event_loop *loop, timer_id id, void *context) {
  client_connection *client_conn = context;
  CHECK(client_conn->timer_id == id);
  return_from_wait(client_conn);
  return EVENT_LOOP_TIMER_DONE;
}

void process_wait_request(client_connection *client_conn,
                          int num_object_ids,
                          object_request object_requests[],
                          uint64_t timeout,
                          int num_ready_objects) {
  plasma_manager_state *manager_state = client_conn->manager_state;
  client_conn->num_return_objects = num_ready_objects;
  client_conn->is_wait = true;
  client_conn->wait1 = false; /* old wait */
  client_conn->fetch1 = false;
  client_conn->timer_id = event_loop_add_timer(
      manager_state->loop, timeout, wait_timeout_handler, client_conn);
  client_conn->wait_reply = plasma_alloc_reply(num_ready_objects);
  for (int i = 0; i < num_object_ids; ++i) {
    available_object *entry;
    HASH_FIND(hh, manager_state->local_available_objects,
              &(object_requests[i].object_id),
              sizeof(object_requests[i].object_id), entry);
    if (entry) {
      /* If an object id occurs twice in object_ids, this will count them twice.
       * This might not be desirable behavior. */
      client_conn->num_return_objects -= 1;
      client_conn->wait_reply->object_requests[client_conn->num_return_objects]
          .object_id = entry->object_id;
      if (client_conn->num_return_objects == 0) {
        event_loop_remove_timer(manager_state->loop, client_conn->timer_id);
        return_from_wait(client_conn);
        return;
      }
    } else {
      add_object_request(client_conn, object_requests[i].object_id);
    }
  }
}

/** === START - ALTERNATE PLASMA CLIENT API === */

void return_from_wait1(client_connection *client_conn) {
  CHECK(client_conn->is_wait);
  CHECK(client_conn->wait1);

  CHECK(plasma_send_reply(client_conn->fd, client_conn->wait_reply) >= 0);
  free(client_conn->wait_reply);

  /* Clean the remaining object connections. TODO(istoica): Check with Philipp.
   */
  client_object_request *object_req, *tmp;
  HASH_ITER(active_hh, client_conn->active_objects, object_req, tmp) {
    remove_object_request(client_conn, object_req);
  }
}

int wait_timeout_handler1(event_loop *loop, timer_id id, void *context) {
  client_connection *client_conn = context;
  CHECK(client_conn->timer_id == id);
  return_from_wait1(client_conn);
  return EVENT_LOOP_TIMER_DONE;
}

void process_wait_request1(client_connection *client_conn,
                           int num_object_requests,
                           object_request object_requests[],
                           uint64_t timeout,
                           int num_ready_objects) {
  CHECK(client_conn != NULL);

  plasma_manager_state *manager_state = client_conn->manager_state;
  client_conn->num_return_objects = num_ready_objects;

  /* We can only run a command at a time on any given client connection
   * (client_conn) so set up is_wait so callback() can check whether we are
   * still in wait(). */
  client_conn->is_wait = true;
  client_conn->wait1 = true; /* new wait request */
  client_conn->fetch1 = false;

  client_conn->wait_reply = plasma_alloc_reply(num_object_requests);
  object_requests_copy(num_object_requests,
                       client_conn->wait_reply->object_requests,
                       object_requests);
  object_requests_set_status_all(num_object_requests,
                                 client_conn->wait_reply->object_requests,
                                 PLASMA_OBJECT_NONEXISTENT);
  /* We will just return back the same object_requests list after setting the
   * status of the requests. */
  client_conn->wait_reply->num_object_ids = num_object_requests;

  /* Add timer callback. If timeout expires, it invokes wait_timeout_handler().
   * If we get num_ready_objects before timeout expires, we remove the timer. */
  client_conn->timer_id = event_loop_add_timer(
      manager_state->loop, timeout, wait_timeout_handler1, client_conn);

  /* Now check whether objects are in the Local Object store, and if not, check
   * whether they are remote. */
  for (int i = 0; i < num_object_requests; ++i) {
    if (is_object_local(manager_state, object_requests[i].object_id)) {
      /* If an object ID occurs twice in object_requests, this will count them
       * twice. This might not be desirable behavior. */
      client_conn->num_return_objects -= 1;
      client_conn->wait_reply->object_requests[i].status = PLASMA_OBJECT_LOCAL;
      if (client_conn->num_return_objects == 0) {
        /* We got num_return_objects in the local Object Store, so return. */
        event_loop_remove_timer(manager_state->loop, client_conn->timer_id);
        return_from_wait1(client_conn);
        return;
      }
    } else {
      object_request *object_request =
          &client_conn->wait_reply->object_requests[i];

      if (object_request->status == PLASMA_OBJECT_NONEXISTENT) {
        if (get_object_request(client_conn, object_request->object_id)) {
          /* This object is in transfer, which means that it is stored on a
           * remote node. */
          client_conn->wait_reply->object_requests[i].status =
              PLASMA_OBJECT_REMOTE;
          if (client_conn->wait_reply->object_requests[i].type ==
              PLASMA_QUERY_ANYWHERE) {
            client_conn->num_return_objects -= 1;
            if (client_conn->num_return_objects == 0) {
              /* We got num_return_objects in the local Object Store, so return.
               */
              event_loop_remove_timer(manager_state->loop,
                                      client_conn->timer_id);
              return_from_wait1(client_conn);
              return;
            }
          }
        }
        /* Subscribe to hear when object becomes available. */
        retry_info retry_subscribe = {
            .num_retries = 0, .timeout = 0, .fail_callback = NULL,
        };
        /* TODO(istoica): We should really cache the results here. */
        object_table_subscribe(
            g_manager_state->db,
            client_conn->wait_reply->object_requests[i].object_id,
            wait_object_available_callback, (void *) client_conn,
            &retry_subscribe, NULL, NULL);
        /* TODO(istoica): Since the existing subscribe doesn't return when the
         * object already exists in the Object Table, do a lookup as well. */
        retry_info retry_lookup = {
            .num_retries = NUM_RETRIES,
            .timeout = MANAGER_TIMEOUT,
            .fail_callback = NULL,
        };

        object_table_lookup(
            client_conn->manager_state->db,
            client_conn->wait_reply->object_requests[i].object_id,
            &retry_lookup, wait_object_lookup_callback, client_conn);
      }
    }
  }
}

/* TODO(pcm): unify with wait_object_available_callback. */
void wait_object_lookup_callback(object_id object_id,
                                 int manager_count,
                                 const char *manager_vector[],
                                 void *context) {
  if (manager_count > 0) {
    wait_object_available_callback(object_id, manager_count, manager_vector,
                                   context);
  }
}

void wait_object_available_callback(object_id object_id,
                                    int manager_count,
                                    const char *manager_vector[],
                                    void *user_context) {
  client_connection *client_conn = (client_connection *) user_context;
  CHECK(client_conn != NULL);
  plasma_manager_state *manager_state = client_conn->manager_state;
  CHECK(manager_state);

  if ((!client_conn->is_wait) || (!client_conn->wait1)) {
    return;
  }

  plasma_reply *wait_reply = client_conn->wait_reply;
  object_request *object_request;
  object_request = object_requests_get_object(
      object_id, wait_reply->num_object_ids, wait_reply->object_requests);
  if (object_request == NULL) {
    /* Maybe this is from a previous wait call, so ignore it. */
    return;
  }

  /* Check first whether object is avilable in the local Plasma Store. */
  if (is_object_local(manager_state, object_id)) {
    client_conn->num_return_objects -= 1;
    object_request->status = PLASMA_OBJECT_LOCAL;
  } else {
    object_request->status = PLASMA_OBJECT_REMOTE;
    if (object_request->type == PLASMA_QUERY_ANYWHERE) {
      client_conn->num_return_objects -= 1;
    }
  }

  if (client_conn->num_return_objects == 0) {
    /* We got num_return_objects in the local Object Store, so return. */
    event_loop_remove_timer(manager_state->loop, client_conn->timer_id);
    return_from_wait1(client_conn);
  }
}

void wait_process_object_available_local(client_connection *client_conn,
                                         object_id object_id) {
  CHECK(client_conn != NULL);
  if (!client_conn->is_wait) {
    return;
  }

  plasma_reply *wait_reply = client_conn->wait_reply;
  object_request *object_request;
  object_request = object_requests_get_object(
      object_id, wait_reply->num_object_ids, wait_reply->object_requests);
  if (object_request) {
    client_conn->num_return_objects -= 1;
    object_request->status = PLASMA_OBJECT_LOCAL;
  }
}

/**
 * Handler handling the timeout experiation of a transfer request.
 *
 * @param loop Event loop.
 * @param timer_id ID of the timer which has expired.
 * @param contect Client connection.
 * @return Void.
 */
int fetch_timeout_handler(event_loop *loop, timer_id id, void *context) {
  CHECK(loop);
  CHECK(context);

  client_object_request *object_req = context;
  client_connection *client_conn = object_req->client_conn;

  LOG_DEBUG("Timer went off, %d tries left", object_req->num_retries);

  if (object_req->num_retries > 0) {
    request_transfer_from(client_conn, object_req->object_id);
    object_req->num_retries--;
    return MANAGER_TIMEOUT;
  }
  plasma_reply reply = plasma_make_reply(object_req->object_id);
  reply.object_status = PLASMA_OBJECT_NONEXISTENT;
  CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);

  remove_object_request(client_conn, object_req);
  return EVENT_LOOP_TIMER_DONE;
}

/**
 * Request the transfer from a remote node.
 *
 * @param object_id ID of the object to transfer.
 * @param manager_cont Number of remote nodes object_id is stored at.
 * @param manager_vector Array containing the Plasma Managers running at the
 *        nodes where object_id is stored.
 * @param context Client connection.
 * @return Void.
 */
void request_fetch_initiate(object_id object_id,
                            int manager_count,
                            const char *manager_vector[],
                            void *context) {
  client_connection *client_conn = (client_connection *) context;
  int status = request_fetch_or_status(object_id, manager_count, manager_vector,
                                       context, true);
  plasma_reply reply = plasma_make_reply(object_id);
  reply.object_status = status;
  CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
}

/**
 * Check whether a non-local object is stored on any remot enote or not.
 *
 * @param object_id ID of the object whose status we require.
 * @param manager_cont Number of remote nodes object_id is stored at. If
 *        manager_count > 0, then object_id exists on a remote node an its
 *        status is PLASMA_OBJECT_REMOTE. Otherwise, if manager_count == 0, the
 *        object doesn't exist in the system and its status is
 *        PLASMA_OBJECT_NONEXISTENT.
 * @param manager_vector Array containing the Plasma Managers running at the
 *        nodes where object_id is stored. Not used; it will be eventually
 *        deallocated.
 * @param context Client connection.
 * @return Void.
 */
void request_status_done(object_id object_id,
                         int manager_count,
                         const char *manager_vector[],
                         void *context) {
  client_connection *client_conn = (client_connection *) context;
  int status = request_fetch_or_status(object_id, manager_count, manager_vector,
                                       context, false);
  plasma_reply reply = plasma_make_reply(object_id);
  reply.object_status = status;
  CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
}

int request_fetch_or_status(object_id object_id,
                            int manager_count,
                            const char *manager_vector[],
                            void *context,
                            bool fetch) {
  client_connection *client_conn = (client_connection *) context;
  client_object_request *object_req =
      get_object_request(client_conn, object_id);

  /* Return success immediately if we already have this object. */
  if (is_object_local(client_conn->manager_state, object_id)) {
    return PLASMA_OBJECT_LOCAL;
  }

  /* Check wether there's already an outstanding fetch for this object for this
   * client, and if yes let the outstanding request finish the work. Note that
   * we have already checked for this in process_fetch_or_status_request(), but
   * we need to check again here as the object could hav been evicted since
   * then. */
  if (object_req) {
    return PLASMA_OBJECT_IN_TRANSFER;
  }

  /* If the object isn't on any managers, report a failure to the client. */
  LOG_DEBUG("Object is on %d managers", manager_count);
  if (manager_count == 0) {
    if (object_req) {
      remove_object_request(client_conn, object_req);
    }
    return PLASMA_OBJECT_NONEXISTENT;
  }

  if (fetch) {
    /* Register the new outstanding fetch with the current client connection. */
    object_req = add_object_request(client_conn, object_id);
    CHECKM(object_req != NULL, "Unable to allocate memory for object context.");

    /* Pick a different manager to request a transfer from on every attempt. */
    object_req->manager_count = manager_count;
    object_req->manager_vector = malloc(manager_count * sizeof(char *));
    memset(object_req->manager_vector, 0, manager_count * sizeof(char *));
    for (int i = 0; i < manager_count; ++i) {
      int len = strlen(manager_vector[i]);
      object_req->manager_vector[i] = malloc(len + 1);
      strncpy(object_req->manager_vector[i], manager_vector[i], len);
      object_req->manager_vector[i][len] = '\0';
    }
    /* Wait for the object data for the default number of retries, which timeout
     * after a default interval. */
    object_req->num_retries = NUM_RETRIES;
    object_req->object_id = object_id;
    object_req->timer =
        event_loop_add_timer(client_conn->manager_state->loop, MANAGER_TIMEOUT,
                             fetch_timeout_handler, object_req);
    request_transfer_from(client_conn, object_id);
    /* Let scheduling the fetch request proceded and return. */
  };

  /* Since object is not stored at the local locally, manager_count > 0 means
   * that the object is stored at another remote object. Otherwise, if
   * manager_count == 0, the object is not stored anywhere. */
  return (manager_count > 0 ? PLASMA_OBJECT_REMOTE : PLASMA_OBJECT_NONEXISTENT);
}

void object_table_lookup_fail_callback(object_id object_id,
                                       void *user_context,
                                       void *user_data) {
  /* Fail for now. Later, we may want to send a PLASMA_OBJECT_NONEXISTENT to the
   * client. */
  CHECK(0);
}

void process_fetch_or_status_request(client_connection *client_conn,
                                     object_id object_id,
                                     bool fetch) {
  client_conn->is_wait = false;
  client_conn->fetch1 = true;
  client_conn->wait_reply = NULL;

  /* Return success immediately if we already have this object. */
  if (is_object_local(client_conn->manager_state, object_id)) {
    plasma_reply reply = plasma_make_reply(object_id);
    reply.object_status = PLASMA_OBJECT_LOCAL;
    CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
    return;
  }

  /* Check whether a transfer request for this object is already pending. */
  if (get_object_request(client_conn, object_id)) {
    plasma_reply reply = plasma_make_reply(object_id);
    reply.object_status = PLASMA_OBJECT_IN_TRANSFER;
    CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
    return;
  }

  if (client_conn->manager_state->db == NULL) {
    plasma_reply reply = plasma_make_reply(object_id);
    reply.object_status = PLASMA_OBJECT_NONEXISTENT;
    CHECK(plasma_send_reply(client_conn->fd, &reply) >= 0);
    return;
  }

  /* The object is not local, so check whether it is stored remotely. */
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = MANAGER_TIMEOUT,
      .fail_callback = object_table_lookup_fail_callback,
  };

  if (fetch) {
    /* Request a transfer from a plasma manager that has this object, if any. */
    object_table_lookup(client_conn->manager_state->db, object_id, &retry,
                        request_fetch_initiate, client_conn);
  } else {
    object_table_lookup(client_conn->manager_state->db, object_id, &retry,
                        request_status_done, client_conn);
  }
}

/* === END - ALTERNATE PLASMA CLIENT API === */

void process_object_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  plasma_manager_state *state = context;
  object_id obj_id;
  object_info object_info;
  retry_info retry = {
      .num_retries = NUM_RETRIES,
      .timeout = MANAGER_TIMEOUT,
      .fail_callback = NULL,
  };
  /* Read the notification from Plasma. */
  int error =
      read_bytes(client_sock, (uint8_t *) &object_info, sizeof(object_info));
  if (error < 0) {
    /* The store has closed the socket. */
    LOG_DEBUG(
        "The plasma store has closed the object notification socket, or some "
        "other error has occurred.");
    event_loop_remove_file(loop, client_sock);
    close(client_sock);
    return;
  }
  obj_id = object_info.obj_id;
  /* Add object to locally available object. */
  /* TODO(pcm): Where is this deallocated? */
  available_object *entry =
      (available_object *) malloc(sizeof(available_object));
  entry->object_id = obj_id;
  HASH_ADD(hh, state->local_available_objects, object_id, sizeof(object_id),
           entry);

  /* Add this object to the (redis) object table. */
  if (state->db) {
    /* TODO(swang): Log the error if we fail to add the object, and possibly
     * retry later? */
    object_table_add(state->db, obj_id, obj_id_notification.digest, &retry,
                     NULL, NULL);
  }

  /* If we were trying to fetch this object, finish up the fetch request. */
  fetch_request2 *fetch_req;
  HASH_FIND(hh, state->fetch_requests2, &obj_id, sizeof(obj_id), fetch_req);
  if (fetch_req != NULL) {
    remove_fetch_request(state, fetch_req);
    /* TODO(rkn): We also really should unsubscribe from the object table. */
  }
  /* Notify any clients who were waiting on a fetch to this object and tick
   * off objects we are waiting for. */
  client_object_request *object_req, *next;
  client_connection *client_conn;
  HASH_FIND(fetch_hh, state->fetch_requests, &obj_id, sizeof(object_id),
            object_req);
  plasma_reply reply = plasma_make_reply(obj_id);
  reply.has_object = 1;
  while (object_req) {
    next = object_req->next;
    client_conn = object_req->client_conn;
    if (!client_conn->is_wait) {
      event_loop_remove_timer(state->loop, object_req->timer);
      if (!client_conn->fetch1) {
        send_client_reply(client_conn, &reply);
      }
    } else {
      if (client_conn->wait1) {
        wait_process_object_available_local(client_conn, obj_id);
      } else {
        client_conn->num_return_objects -= 1;
        client_conn->wait_reply
            ->object_requests[client_conn->num_return_objects]
            .object_id = obj_id;
      }
      if (client_conn->num_return_objects == 0) {
        event_loop_remove_timer(loop, client_conn->timer_id);
        if (client_conn->wait1) {
          return_from_wait1(client_conn);
        } else {
          return_from_wait(client_conn);
        }
        object_req = next;
        continue;
      }
    }
    remove_object_request(client_conn, object_req);
    object_req = next;
  }
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  client_connection *conn = (client_connection *) context;

  int64_t type;
  plasma_request *req;
  CHECK(plasma_receive_request(client_sock, &type, &req) >= 0);

  switch (type) {
  case PLASMA_TRANSFER:
    DCHECK(req->num_object_ids == 1);
    process_transfer_request(loop, req->object_requests[0].object_id, req->addr,
                             req->port, conn);
    break;
  case PLASMA_DATA:
    LOG_DEBUG("Starting to stream data");
    DCHECK(req->num_object_ids == 1);
    process_data_request(loop, client_sock, req->object_requests[0].object_id,
                         req->data_size, req->metadata_size, conn);
    break;
  case PLASMA_FETCH:
    LOG_DEBUG("Processing fetch");
    process_fetch_requests(conn, req->num_object_ids, req->object_requests);
    break;
  case PLASMA_FETCH_REMOTE:
    LOG_DEBUG("Processing fetch remote");
    DCHECK(req->num_object_ids == 1);
    process_fetch_or_status_request(conn, req->object_requests[0].object_id,
                                    true);
    break;
  case PLASMA_FETCH2:
    LOG_DEBUG("Processing fetch remote");
    process_fetch_requests2(conn, req->num_object_ids, req->object_requests);
    break;
  case PLASMA_WAIT:
    LOG_DEBUG("Processing wait");
    process_wait_request(conn, req->num_object_ids, req->object_requests,
                         req->timeout, req->num_ready_objects);
    break;
  case PLASMA_WAIT1:
    LOG_DEBUG("Processing wait1");
    process_wait_request1(conn, req->num_object_ids, req->object_requests,
                          req->timeout, req->num_ready_objects);
    break;
  case PLASMA_STATUS:
    LOG_DEBUG("Processing status");
    DCHECK(req->num_object_ids == 1);
    process_fetch_or_status_request(conn, req->object_requests[0].object_id,
                                    false);
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
    LOG_FATAL("invalid request %" PRId64, type);
  }

  free(req);
}

/* TODO(pcm): Split this into two methods: new_worker_connection
 * and new_manager_connection and also split client_connection
 * into two structs, one for workers and one for other plasma managers. */
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
  LOG_DEBUG("New client connection with fd %d", new_socket);
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
                  const char *manager_socket_name,
                  const char *master_addr,
                  int port,
                  const char *db_addr,
                  int db_port) {
  /* Bind the sockets before we try to connect to the plasma store.
   * In case the bind does not succeed, we want to be able to exit
   * without breaking the pipe to the store. */
  int remote_sock = bind_inet_sock(port, false);
  if (remote_sock < 0) {
    exit(EXIT_COULD_NOT_BIND_PORT);
  }

  int local_sock = bind_ipc_sock(manager_socket_name, false);
  CHECKM(local_sock >= 0, "Unable to bind local manager socket");

  g_manager_state = init_plasma_manager_state(store_socket_name, master_addr,
                                              port, db_addr, db_port);
  CHECK(g_manager_state);

  CHECK(listen(remote_sock, 5) != -1);
  CHECK(listen(local_sock, 5) != -1);

  LOG_DEBUG("Started server connected to store %s, listening on port %d",
            store_socket_name, port);
  event_loop_add_file(g_manager_state->loop, local_sock, EVENT_LOOP_READ,
                      handle_new_client, g_manager_state);
  event_loop_add_file(g_manager_state->loop, remote_sock, EVENT_LOOP_READ,
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
  /* Socket name this manager will bind to. */
  char *manager_socket_name = NULL;
  /* IP address of this node. */
  char *master_addr = NULL;
  /* Port number the manager should use. */
  int port = -1;
  /* IP address and port of state database. */
  char *db_host = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:m:h:p:r:")) != -1) {
    switch (c) {
    case 's':
      store_socket_name = optarg;
      break;
    case 'm':
      manager_socket_name = optarg;
      break;
    case 'h':
      master_addr = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'r':
      db_host = optarg;
      break;
    default:
      LOG_FATAL("unknown option %c", c);
    }
  }
  if (!store_socket_name) {
    LOG_FATAL(
        "please specify socket for connecting to the plasma store with -s "
        "switch");
  }
  if (!manager_socket_name) {
    LOG_FATAL(
        "please specify socket name of the manager's local socket with -m "
        "switch");
  }
  if (!master_addr) {
    LOG_FATAL(
        "please specify ip address of the current host in the format "
        "123.456.789.10 with -h switch");
  }
  if (port == -1) {
    LOG_FATAL(
        "please specify port the plasma manager shall listen to in the"
        "format 12345 with -p switch");
  }
  char db_addr[16];
  int db_port;
  if (db_host) {
    parse_ip_addr_port(db_host, db_addr, &db_port);
    start_server(store_socket_name, manager_socket_name, master_addr, port,
                 db_addr, db_port);
  } else {
    start_server(store_socket_name, manager_socket_name, master_addr, port,
                 NULL, 0);
  }
}
#endif
