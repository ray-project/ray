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

/* C++ includes. */
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common_protocol.h"
#include "io.h"
#include "net.h"
#include "event_loop.h"
#include "common.h"
#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/protocol.h"
#include "plasma/client.h"
#include "plasma_manager.h"
#include "state/db.h"
#include "state/object_table.h"
#include "state/error_table.h"
#include "state/task_table.h"
#include "state/db_client_table.h"
#include "ray/gcs/client.h"

int handle_sigpipe(plasma::Status s, int fd) {
  if (s.ok()) {
    return 0;
  }

  int err = errno;

  switch (err) {
  case EPIPE: {
    ARROW_LOG(WARNING)
        << "Received EPIPE when sending a message to client on fd " << fd
        << ". The client on the other end may have hung up.";
  } break;
  case EBADF: {
    ARROW_LOG(WARNING)
        << "Received EBADF when sending a message to client on fd " << fd
        << ". The client on the other end may have hung up.";
  } break;
  case ECONNRESET: {
    ARROW_LOG(WARNING)
        << "Received ECONNRESET when sending a message to client on fd " << fd
        << ". The client on the other end may have hung up.";
  } break;
  case EPROTOTYPE: {
    /* TODO(rkn): What triggers this case? */
    ARROW_LOG(WARNING)
        << "Received EPROTOTYPE when sending a message to client on fd " << fd
        << ". The client on the other end may have hung up.";
  } break;
  default:
    /* This code should be unreachable. */
    RAY_CHECK(0);
    RAY_LOG(FATAL) << "Failed to write message to client on fd " << fd;
  }

  return err;
}

/**
 * Process either the fetch or the status request.
 *
 * @param client_conn Client connection.
 * @param object_id ID of the object for which we process this request.
 * @return Void.
 */
void process_status_request(ClientConnection *client_conn, ObjectID object_id);

/**
 * Request the transfer from a remote node or get the status of
 * a given object. This is called for an object that is stored at
 * a remote Plasma Store.
 *
 * @param object_id ID of the object to transfer or to get its status.
 * @param manager_vector Array containing the Plasma Managers running at the
 *        nodes where object_id is stored.
 * @param context Client connection.
 * @return Status of object_id as defined in plasma.h
 */
int request_status(ObjectID object_id,
                   const std::vector<DBClientID> &manager_vector,
                   void *context);

/**
 * Send requested object_id back to the Plasma Manager identified
 * by (addr, port) which requested it. This is done via a
 * data Request message.
 *
 * @param loop
 * @param object_id The ID of the object being transferred to (addr, port).
 * @param addr The address of the Plasma Manager object_id is sent to.
 * @param port The port number of the Plasma Manager object_id is sent to.
 * @param conn The client connection object.
 */
void process_transfer_request(event_loop *loop,
                              ObjectID object_id,
                              const char *addr,
                              int port,
                              ClientConnection *conn);

/**
 * Receive object_id requested by this Plamsa Manager from the remote Plasma
 * Manager identified by client_sock. The object_id is sent via the data request
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
                          ObjectID object_id,
                          int64_t data_size,
                          int64_t metadata_size,
                          ClientConnection *conn);

typedef struct {
  /** The ID of the object we are fetching or waiting for. */
  ObjectID object_id;
  /** Vector of the addresses of the managers containing this object. */
  std::vector<std::string> manager_vector;
  /** The next manager we should try to contact. This is set to an index in
   *  manager_vector in the retry handler, in case the current attempt fails to
   *  contact a manager. */
  int next_manager;
} FetchRequest;

/**
 * There are fundamentally two data structures used for handling wait requests.
 * There is the "wait_request" struct and an unordered map.
 * WaitRequest keeps track of all of the object IDs that a WaitRequest is
 * waiting for. The unordered map keeps track of all of the
 * WaitRequest structs that are waiting for a particular object ID. The
 * PlasmaManagerState owns and manages the unordered maps.
 *
 * These data structures are updated by several methods:
 *   - add_wait_request_for_object adds a WaitRequest to the unordered map
 *     corresponding to a particular object ID. This is called when a client
 *     calls plasma_wait.
 *   - remove_wait_request_for_object removes a WaitRequest from an unordered
 *     map. When a wait request returns, this method is called for all of the
 *     object IDs involved in that WaitRequest.
 *   - update_object_wait_requests removes a vector of wait requests from the
 *     unordered map and does some processing for each WaitRequest involved in
 *     the vector.
 */
struct WaitRequest {
  WaitRequest(ClientConnection *client_conn,
              int64_t timer,
              int64_t num_object_requests,
              plasma::ObjectRequestMap &&object_requests,
              int64_t num_objects_to_wait_for,
              int64_t num_satisfied)
      : client_conn(client_conn),
        timer(timer),
        num_object_requests(num_object_requests),
        object_requests(object_requests),
        num_objects_to_wait_for(num_objects_to_wait_for),
        num_satisfied(num_satisfied) {}

  /** The client connection that called wait. */
  ClientConnection *client_conn;
  /** The ID of the timer that will time out and cause this wait to return to
   *  the client if it hasn't already returned. */
  int64_t timer;
  /** The number of objects in this wait request. */
  int64_t num_object_requests;
  /** The object requests for this wait request. Each object request has a
   *  status field which is either PLASMA_QUERY_LOCAL or PLASMA_QUERY_ANYWHERE.
   */
  plasma::ObjectRequestMap object_requests;
  /** The minimum number of objects to wait for in this request. */
  int64_t num_objects_to_wait_for;
  /** The number of object requests in this wait request that are already
   *  satisfied. */
  int64_t num_satisfied;
};

struct PlasmaManagerState {
  /** Event loop. */
  event_loop *loop;
  /** Connection to the local plasma store for reading or writing data. */
  plasma::PlasmaClient *plasma_conn;
  /** Hash table of all contexts for active connections to
   *  other plasma managers. These are used for writing data to
   *  other plasma stores. */
  std::unordered_map<std::string, ClientConnection *> manager_connections;
  DBHandle *db;
  /** The handle to the GCS (modern version of the above). */
  ray::gcs::AsyncGcsClient gcs_client;
  /** Our address. */
  const char *addr;
  /** Our port. */
  int port;
  /** Unordered map of outstanding fetch requests. The key is the object ID. The
   *  value is the data needed to perform the fetch. */
  std::unordered_map<ObjectID, FetchRequest *> fetch_requests;
  /** Unordered map of outstanding wait requests. The key is the object ID. The
   *  value is the vector of wait requests that are waiting for the object to
   *  arrive locally. */
  std::unordered_map<ObjectID, std::vector<WaitRequest *>>
      object_wait_requests_local;
  /** Unordered map of outstanding wait requests. The key is the object ID. The
   *  value is the vector of wait requests that are waiting for the object to
   *  be available somewhere in the system. */
  std::unordered_map<ObjectID, std::vector<WaitRequest *>>
      object_wait_requests_remote;
  /** Initialize an empty unordered set for the cache of local available object.
   */
  std::unordered_set<ObjectID> local_available_objects;
  /** The time (in milliseconds since the Unix epoch) when the most recent
   *  heartbeat was sent. */
  int64_t previous_heartbeat_time;
  /** This is the set of ObjectIDs currently being transferred to this manager.
   *  An ObjectID is added to this set if a shared buffer is
   *  successfully created for the corresponding object.
   *  The ObjectID is removed in process_add_object_notification, which is
   *  triggered by the corresponding notification from the plasma store.
   *  If an object transfer fails, only the ObjectID of the corresponding
   *  object is removed. If object transfers between managers is parallelized,
   *  then all objects being received from a remote manager will need to be
   *  removed if the connection to the remote manager fails. */
  std::unordered_set<ObjectID> receives_in_progress;
};

PlasmaManagerState *g_manager_state = NULL;

/* Context for a client connection to another plasma manager. */
struct ClientConnection {
  /** Current state for this plasma manager. This is shared
   *  between all client connections to the plasma manager. */
  PlasmaManagerState *manager_state;
  /** Current position in the buffer. */
  int64_t cursor;
  /** Linked list of buffers to read or write. */
  /* TODO(swang): Split into two queues, data transfers and data requests. */
  std::list<PlasmaRequestBuffer *> transfer_queue;
  /* A set of object IDs which are queued in the transfer_queue and waiting to
   * be sent. This is used to avoid sending the same object ID to the same
   * manager multiple times. */
  std::unordered_map<ObjectID, PlasmaRequestBuffer *> pending_object_transfers;
  /** Buffer used to receive transfers (data fetches) we want to ignore */
  PlasmaRequestBuffer *ignore_buffer;
  /** File descriptor for the socket connected to the other
   *  plasma manager. */
  int fd;
  /** Timer id for timing out wait (or fetch). */
  int64_t timer_id;
  /** The number of objects that we have left to return for
   *  this fetch or wait operation. */
  int num_return_objects;
  /** Fields specific to connections to plasma managers.  Key that uniquely
   * identifies the plasma manager that we're connected to. We will use the
   * string <address>:<port> as an identifier. */
  std::string ip_addr_port;
};

/**
 * Initializes the state for a plasma client connection.
 *
 * @param state The plasma manager state.
 * @param client_sock The socket that we use to communicate with the client.
 * @param client_key A string uniquely identifying the client. If the client is
 *        another plasma manager, this is the manager's IP address and port.
 *        Else, the client is the string of the client's socket.
 * @return A pointer to the initialized client state.
 */
ClientConnection *ClientConnection_init(PlasmaManagerState *state,
                                        int client_sock,
                                        std::string const &client_key);

/**
 * Destroys a plasma client and its connection.
 *
 * @param client_conn The client's state.
 * @return Void.
 */
void ClientConnection_free(ClientConnection *client_conn);

void ClientConnection_start_request(ClientConnection *client_conn) {
  client_conn->cursor = 0;
}

void ClientConnection_finish_request(ClientConnection *client_conn) {
  client_conn->cursor = -1;
}

bool ClientConnection_request_finished(ClientConnection *client_conn) {
  return client_conn->cursor == -1;
}

std::unordered_map<ObjectID, std::vector<WaitRequest *>> &
object_wait_requests_from_type(PlasmaManagerState *manager_state, int type) {
  /* We use different types of hash tables for different requests. */
  RAY_CHECK(type == plasma::PLASMA_QUERY_LOCAL ||
            type == plasma::PLASMA_QUERY_ANYWHERE);
  if (type == plasma::PLASMA_QUERY_LOCAL) {
    return manager_state->object_wait_requests_local;
  } else {
    return manager_state->object_wait_requests_remote;
  }
}

void add_wait_request_for_object(PlasmaManagerState *manager_state,
                                 ObjectID object_id,
                                 int type,
                                 WaitRequest *wait_req) {
  auto &object_wait_requests =
      object_wait_requests_from_type(manager_state, type);

  /* Add this wait request to the vector of wait requests involving this object
   * ID. Creates a vector of wait requests if none exist involving the object
   * ID. */
  object_wait_requests[object_id].push_back(wait_req);
}

void remove_wait_request_for_object(PlasmaManagerState *manager_state,
                                    ObjectID object_id,
                                    int type,
                                    WaitRequest *wait_req) {
  auto &object_wait_requests =
      object_wait_requests_from_type(manager_state, type);
  auto object_wait_requests_it = object_wait_requests.find(object_id);
  /* If there is a vector of wait requests for this object ID, and if this
   * vector contains the wait request, then remove the wait request from the
   * vector. */
  if (object_wait_requests_it != object_wait_requests.end()) {
    std::vector<WaitRequest *> &wait_requests = object_wait_requests_it->second;
    for (size_t i = 0; i < wait_requests.size(); ++i) {
      if (wait_requests[i] == wait_req) {
        /* Remove the wait request from the array. */
        wait_requests.erase(wait_requests.begin() + i);
        break;
      }
    }
  }
}

void remove_wait_request(PlasmaManagerState *manager_state,
                         WaitRequest *wait_req) {
  if (wait_req->timer != -1) {
    RAY_CHECK(event_loop_remove_timer(manager_state->loop, wait_req->timer) ==
              AE_OK);
  }
  delete wait_req;
}

void return_from_wait(PlasmaManagerState *manager_state,
                      WaitRequest *wait_req) {
  /* Send the reply to the client. */
  handle_sigpipe(plasma::SendWaitReply(wait_req->client_conn->fd,
                                       wait_req->object_requests,
                                       wait_req->num_object_requests),
                 wait_req->client_conn->fd);
  /* Iterate over all object IDs requested as part of this wait request.
   * Remove the wait request from each of the relevant object_wait_requests maps
   * if it is present there. */
  for (const auto &entry : wait_req->object_requests) {
    remove_wait_request_for_object(manager_state, entry.second.object_id,
                                   entry.second.type, wait_req);
  }
  /* Remove the wait request. */
  remove_wait_request(manager_state, wait_req);
}

void update_object_wait_requests(PlasmaManagerState *manager_state,
                                 ObjectID obj_id,
                                 int type,
                                 int status) {
  auto &object_wait_requests =
      object_wait_requests_from_type(manager_state, type);
  /* Update the in-progress wait requests in the specified table. */
  auto object_wait_requests_it = object_wait_requests.find(obj_id);
  if (object_wait_requests_it != object_wait_requests.end()) {
    /* We compute the number of requests first because the length of the vector
     * will change as we iterate over it (because each call to return_from_wait
     * will remove one element). */
    std::vector<WaitRequest *> &wait_requests = object_wait_requests_it->second;
    int num_requests = wait_requests.size();
    /* The argument index is the index of the current element of the vector
     * that we are processing. It may differ from the counter i when elements
     * are removed from the array. */
    int index = 0;
    for (int i = 0; i < num_requests; ++i) {
      WaitRequest *wait_req = wait_requests[index];
      wait_req->num_satisfied += 1;
      /* Mark the object as present in the wait request. */
      auto object_request =
          wait_req->object_requests.find(obj_id.to_plasma_id());
      /* Check that we found the object. */
      RAY_CHECK(object_request != wait_req->object_requests.end());
      /* Check that the object found was not previously known to us. */
      RAY_CHECK(object_request->second.status == ObjectStatus_Nonexistent);
      /* Update the found object's status to a known status. */
      object_request->second.status = status;

      /* If this wait request is done, reply to the client. */
      if (wait_req->num_satisfied == wait_req->num_objects_to_wait_for) {
        return_from_wait(manager_state, wait_req);
      } else {
        /* The call to return_from_wait will remove the current element in the
         * array, so we only increment the counter in the else branch. */
        index += 1;
      }
    }
    RAY_CHECK(static_cast<size_t>(index) == wait_requests.size());
    /* Remove the array of wait requests for this object, since no one should be
     * waiting for this object anymore. */
    object_wait_requests.erase(object_wait_requests_it);
  }
}

FetchRequest *create_fetch_request(PlasmaManagerState *manager_state,
                                   ObjectID object_id) {
  FetchRequest *fetch_req = new FetchRequest();
  fetch_req->object_id = object_id;
  return fetch_req;
}

/**
 * Remove a fetch request from the table of fetch requests.
 *
 * @param manager_state The state of the manager.
 * @param fetch_req The fetch request to remove.
 * @return Void.
 */
void remove_fetch_request(PlasmaManagerState *manager_state,
                          FetchRequest *fetch_req) {
  /* Remove the fetch request from the table of fetch requests. */
  manager_state->fetch_requests.erase(fetch_req->object_id);
  /* Free the fetch request. */
  delete fetch_req;
}

PlasmaManagerState *PlasmaManagerState_init(const char *store_socket_name,
                                            const char *manager_socket_name,
                                            const char *manager_addr,
                                            int manager_port,
                                            const char *redis_primary_addr,
                                            int redis_primary_port) {
  PlasmaManagerState *state = new PlasmaManagerState();
  state->loop = event_loop_create();
  state->plasma_conn = new plasma::PlasmaClient();
  ARROW_CHECK_OK(state->plasma_conn->Connect(
      store_socket_name, "", plasma::kPlasmaDefaultReleaseDelay));
  if (redis_primary_addr) {
    /* Get the manager port as a string. */
    std::string manager_address_str =
        std::string(manager_addr) + ":" + std::to_string(manager_port);

    std::vector<std::string> db_connect_args;
    db_connect_args.push_back("store_socket_name");
    db_connect_args.push_back(store_socket_name);
    db_connect_args.push_back("manager_socket_name");
    db_connect_args.push_back(manager_socket_name);
    db_connect_args.push_back("manager_address");
    db_connect_args.push_back(manager_address_str);
    state->db = db_connect(std::string(redis_primary_addr), redis_primary_port,
                           "plasma_manager", manager_addr, db_connect_args);
    db_attach(state->db, state->loop, false);

    RAY_CHECK_OK(state->gcs_client.Connect(std::string(redis_primary_addr),
                                           redis_primary_port));
    RAY_CHECK_OK(state->gcs_client.context()->AttachToEventLoop(state->loop));
  } else {
    state->db = NULL;
    RAY_LOG(DEBUG) << "No db connection specified";
  }
  state->addr = manager_addr;
  state->port = manager_port;
  /* Subscribe to notifications about sealed objects. */
  int plasma_fd;
  ARROW_CHECK_OK(state->plasma_conn->Subscribe(&plasma_fd));
  /* Add the callback that processes the notification to the event loop. */
  event_loop_add_file(state->loop, plasma_fd, EVENT_LOOP_READ,
                      process_object_notification, state);
  /* Initialize the time at which the previous heartbeat was sent. */
  state->previous_heartbeat_time = current_time_ms();
  return state;
}

void PlasmaManagerState_free(PlasmaManagerState *state) {
  /* Reset the SIGTERM handler to default behavior, so we try to clean up the
   * plasma manager at most once. */
  signal(SIGTERM, SIG_DFL);
  if (state->db != NULL) {
    db_disconnect(state->db);
    state->db = NULL;
  }

  /* We have to be careful here because ClientConnection_free modifies
   * state->manager_connections in place. */
  auto cc_it = state->manager_connections.begin();
  while (cc_it != state->manager_connections.end()) {
    auto next_it = std::next(cc_it, 1);
    ClientConnection_free(cc_it->second);
    cc_it = next_it;
  }

  /* We have to be careful here because remove_fetch_request modifies
   * state->fetch_requests in place. */
  auto it = state->fetch_requests.begin();
  while (it != state->fetch_requests.end()) {
    auto next_it = std::next(it, 1);
    remove_fetch_request(state, it->second);
    it = next_it;
  }

  ARROW_CHECK_OK(state->plasma_conn->Disconnect());
  delete state->plasma_conn;

  /* Destroy the event loop. */
  destroy_outstanding_callbacks(state->loop);
  event_loop_destroy(state->loop);
  state->loop = NULL;

  delete state;
}

bool is_receiving_or_received(const PlasmaManagerState *state,
                              const ObjectID &object_id) {
  return state->local_available_objects.count(object_id) > 0 ||
         state->receives_in_progress.count(object_id) > 0;
}

event_loop *get_event_loop(PlasmaManagerState *state) {
  return state->loop;
}

/* Handle a command request that came in through a socket (transfering data,
 * or accepting incoming data). */
void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events);

int write_object_chunk(ClientConnection *conn, PlasmaRequestBuffer *buf) {
  ssize_t r, s;
  /* Try to write one buf_size at a time. */
  s = buf->data_size + buf->metadata_size - conn->cursor;
  if (s > RayConfig::instance().buf_size()) {
    s = RayConfig::instance().buf_size();
  }
  r = write(conn->fd, buf->data + conn->cursor, s);

  int err;
  if (r <= 0) {
    RAY_LOG(ERROR) << "Write error";
    err = errno;
  } else {
    conn->cursor += r;
    RAY_CHECK(conn->cursor <= buf->data_size + buf->metadata_size);
    /* If we've finished writing this buffer, reset the cursor. */
    if (conn->cursor == buf->data_size + buf->metadata_size) {
      RAY_LOG(DEBUG) << "writing on channel " << conn->fd << " finished";
      ClientConnection_finish_request(conn);
    }
    err = 0;
  }
  return err;
}

void send_queued_request(event_loop *loop,
                         int data_sock,
                         void *context,
                         int events) {
  ClientConnection *conn = (ClientConnection *) context;
  PlasmaManagerState *state = conn->manager_state;

  if (conn->transfer_queue.size() == 0) {
    /* If there are no objects to transfer, temporarily remove this connection
     * from the event loop. It will be reawoken when we receive another
     * data request. */
    event_loop_remove_file(loop, conn->fd);
    return;
  }

  PlasmaRequestBuffer *buf = conn->transfer_queue.front();
  int err = 0;
  switch (buf->type) {
  case MessageType_PlasmaDataRequest:
    err = handle_sigpipe(
        plasma::SendDataRequest(conn->fd, buf->object_id.to_plasma_id(),
                                state->addr, state->port),
        conn->fd);
    break;
  case MessageType_PlasmaDataReply:
    RAY_LOG(DEBUG) << "Transferring object to manager";
    if (ClientConnection_request_finished(conn)) {
      /* If the cursor is not set, we haven't sent any requests for this object
       * yet, so send the initial data request. */
      err = handle_sigpipe(
          plasma::SendDataReply(conn->fd, buf->object_id.to_plasma_id(),
                                buf->data_size, buf->metadata_size),
          conn->fd);
      ClientConnection_start_request(conn);
    }
    if (err == 0) {
      err = write_object_chunk(conn, buf);
    }
    break;
  default:
    RAY_LOG(FATAL) << "Buffered request has unknown type.";
  }

  /* If the other side hung up, stop sending to this manager. */
  if (err != 0) {
    if (buf->type == MessageType_PlasmaDataReply) {
      /* We errored while sending the object, so release it before removing the
       * connection. The corresponding call to plasma_get occurred in
       * process_transfer_request. */
      ARROW_CHECK_OK(conn->manager_state->plasma_conn->Release(
          buf->object_id.to_plasma_id()));
    }
    event_loop_remove_file(loop, conn->fd);
    ClientConnection_free(conn);
  } else if (ClientConnection_request_finished(conn)) {
    /* If we are done with this request, remove it from the transfer queue. */
    if (buf->type == MessageType_PlasmaDataReply) {
      /* We are done sending the object, so release it. The corresponding call
       * to plasma_get occurred in process_transfer_request. */
      ARROW_CHECK_OK(conn->manager_state->plasma_conn->Release(
          buf->object_id.to_plasma_id()));
      /* Remove the object from the hash table of pending transfer requests. */
      conn->pending_object_transfers.erase(buf->object_id);
    }
    conn->transfer_queue.pop_front();
    delete buf;
  }
}

int read_object_chunk(ClientConnection *conn, PlasmaRequestBuffer *buf) {
  ssize_t r, s;
  RAY_CHECK(buf != NULL);
  /* Try to read one buf_size at a time. */
  s = buf->data_size + buf->metadata_size - conn->cursor;
  if (s > RayConfig::instance().buf_size()) {
    s = RayConfig::instance().buf_size();
  }
  r = read(conn->fd, buf->data + conn->cursor, s);

  int err;
  if (r <= 0) {
    RAY_LOG(ERROR) << "Read error";
    err = errno;
  } else {
    conn->cursor += r;
    RAY_CHECK(conn->cursor <= buf->data_size + buf->metadata_size);
    /* If the cursor is equal to the full object size, reset the cursor and
     * we're done. */
    if (conn->cursor == buf->data_size + buf->metadata_size) {
      ClientConnection_finish_request(conn);
    }
    err = 0;
  }
  return err;
}

void process_data_chunk(event_loop *loop,
                        int data_sock,
                        void *context,
                        int events) {
  /* Read the object chunk. */
  ClientConnection *conn = (ClientConnection *) context;
  PlasmaRequestBuffer *buf = conn->transfer_queue.front();
  int err = read_object_chunk(conn, buf);
  auto plasma_conn = conn->manager_state->plasma_conn;
  if (err != 0) {
    // Remove the object from the receives_in_progress set so that
    // retries are processed.
    // TODO(hme): Remove all ObjectIDs associated with this manager if we
    // allow parallel object transfers.
    conn->manager_state->receives_in_progress.erase(buf->object_id);
    /* Abort the object that we were trying to read from the remote plasma
     * manager. */
    ARROW_CHECK_OK(plasma_conn->Release(buf->object_id.to_plasma_id()));
    ARROW_CHECK_OK(plasma_conn->Abort(buf->object_id.to_plasma_id()));
    /* Remove the bad connection. */
    event_loop_remove_file(loop, data_sock);
    ClientConnection_free(conn);
  } else if (ClientConnection_request_finished(conn)) {
    /* If we're done receiving the object, seal the object and release it. The
     * release corresponds to the call to plasma_create that occurred in
     * process_data_request. */
    RAY_LOG(DEBUG) << "reading on channel " << data_sock << " finished";
    /* The following seal also triggers notification of clients for fetch or
     * wait requests, see process_object_notification. */
    ARROW_CHECK_OK(plasma_conn->Seal(buf->object_id.to_plasma_id()));
    ARROW_CHECK_OK(plasma_conn->Release(buf->object_id.to_plasma_id()));
    /* Remove the request buffer used for reading this object's data. */
    conn->transfer_queue.pop_front();
    delete buf;
    /* Switch to listening for requests from this socket, instead of reading
     * object data. */
    event_loop_remove_file(loop, data_sock);
    bool success = event_loop_add_file(loop, data_sock, EVENT_LOOP_READ,
                                       process_message, conn);
    if (!success) {
      ClientConnection_free(conn);
    }
  }
}

void ignore_data_chunk(event_loop *loop,
                       int data_sock,
                       void *context,
                       int events) {
  /* Read the object chunk. */
  ClientConnection *conn = (ClientConnection *) context;
  PlasmaRequestBuffer *buf = conn->ignore_buffer;

  /* Just read the transferred data into ignore_buf and then drop (free) it. */
  int err = read_object_chunk(conn, buf);
  if (err != 0) {
    event_loop_remove_file(loop, data_sock);
    ClientConnection_free(conn);
  } else if (ClientConnection_request_finished(conn)) {
    free(buf->data);
    delete buf;
    /* Switch to listening for requests from this socket, instead of reading
     * object data. */
    event_loop_remove_file(loop, data_sock);
    bool success = event_loop_add_file(loop, data_sock, EVENT_LOOP_READ,
                                       process_message, conn);
    if (!success) {
      ClientConnection_free(conn);
    }
  }
}

ClientConnection *get_manager_connection(PlasmaManagerState *state,
                                         const char *ip_addr,
                                         int port) {
  /* TODO(swang): Should probably check whether ip_addr and port belong to us.
   */
  std::string ip_addr_port = std::string(ip_addr) + ":" + std::to_string(port);
  ClientConnection *manager_conn;
  auto cc_it = state->manager_connections.find(ip_addr_port);
  if (cc_it == state->manager_connections.end()) {
    /* If we don't already have a connection to this manager, start one. */
    int fd = connect_inet_sock(ip_addr, port);
    if (fd < 0) {
      return NULL;
    }

    manager_conn = ClientConnection_init(state, fd, ip_addr_port);
  } else {
    manager_conn = cc_it->second;
  }
  return manager_conn;
}

void process_transfer_request(event_loop *loop,
                              ObjectID obj_id,
                              const char *addr,
                              int port,
                              ClientConnection *conn) {
  ClientConnection *manager_conn =
      get_manager_connection(conn->manager_state, addr, port);
  if (manager_conn == NULL) {
    return;
  }

  /* If there is already a request in the transfer queue with the same object
   * ID, do not add the transfer request. */
  auto pending_it = manager_conn->pending_object_transfers.find(obj_id);
  if (pending_it != manager_conn->pending_object_transfers.end()) {
    return;
  }

  /* Allocate and append the request to the transfer queue. */
  plasma::ObjectBuffer object_buffer;
  plasma::ObjectID object_id = obj_id.to_plasma_id();
  /* We pass in 0 to indicate that the command should return immediately. */
  ARROW_CHECK_OK(
      conn->manager_state->plasma_conn->Get(&object_id, 1, 0, &object_buffer));
  if (object_buffer.data == nullptr) {
    /* If the object wasn't locally available, exit immediately. If the object
     * later appears locally, the requesting plasma manager should request the
     * transfer again. */
    RAY_LOG(WARNING) << "Unable to transfer object to requesting plasma "
                     << "manager, object not local.";
    return;
  }

  /* If we already have a connection to this manager and its inactive,
   * (re)register it with the event loop again. */
  if (manager_conn->transfer_queue.size() == 0) {
    bool success = event_loop_add_file(loop, manager_conn->fd, EVENT_LOOP_WRITE,
                                       send_queued_request, manager_conn);
    if (!success) {
      ClientConnection_free(manager_conn);
      return;
    }
  }

  RAY_CHECK(object_buffer.metadata->data() ==
            object_buffer.data->data() + object_buffer.data->size());
  PlasmaRequestBuffer *buf = new PlasmaRequestBuffer();
  buf->type = MessageType_PlasmaDataReply;
  buf->object_id = obj_id;
  /* We treat buf->data as a pointer to the concatenated data and metadata, so
   * we don't actually use buf->metadata. */
  buf->data = const_cast<uint8_t *>(object_buffer.data->data());
  buf->data_size = object_buffer.data->size();
  buf->metadata_size = object_buffer.metadata->size();

  manager_conn->transfer_queue.push_back(buf);
  manager_conn->pending_object_transfers[object_id] = buf;
}

/**
 * Receive object_id requested by this Plamsa Manager from the remote Plasma
 * Manager identified by client_sock. The object_id is sent via the data requst
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
                          ObjectID object_id,
                          int64_t data_size,
                          int64_t metadata_size,
                          ClientConnection *conn) {
  PlasmaRequestBuffer *buf = new PlasmaRequestBuffer();
  buf->object_id = object_id;
  buf->data_size = data_size;
  buf->metadata_size = metadata_size;

  /* The corresponding call to plasma_release should happen in
   * process_data_chunk. */
  std::shared_ptr<Buffer> data;
  plasma::Status s = conn->manager_state->plasma_conn->Create(
      object_id.to_plasma_id(), data_size, NULL, metadata_size, &data);

  /* If success_create == true, a new object has been created.
   * If success_create == false the object creation has failed, possibly
   * due to an object with the same ID already existing in the Plasma Store. */
  if (s.ok()) {
    /* Add buffer where the fetched data is to be stored to
     * conn->transfer_queue. */
    conn->transfer_queue.push_back(buf);
  }
  RAY_CHECK(ClientConnection_request_finished(conn));
  ClientConnection_start_request(conn);

  /* Switch to reading the data from this socket, instead of listening for
   * other requests. */
  event_loop_remove_file(loop, client_sock);
  event_loop_file_handler data_chunk_handler;
  if (s.ok()) {
    // Monitor objects that are in progress of being received.
    // If a read fails while receiving this object, its
    // ObjectID will be removed. If the object is successfully
    // received, its ObjectID is removed by process_add_object_notification.
    // If a shared buffer for the object cannot be created,
    // then the receive is ignored, and the corresponding ObjectID
    // is not inserted into receives_in_progress.
    conn->manager_state->receives_in_progress.insert(object_id);
    buf->data = data->mutable_data();
    data_chunk_handler = process_data_chunk;
  } else {
    /* Since plasma_create() has failed, we ignore the data transfer. We will
     * receive this transfer in g_ignore_buf and then drop it. Allocate memory
     * for data and metadata, if needed. All memory associated with
     * buf/g_ignore_buf will be freed in ignore_data_chunkc(). */
    conn->ignore_buffer = buf;
    buf->data = (uint8_t *) malloc(buf->data_size + buf->metadata_size);
    data_chunk_handler = ignore_data_chunk;
  }

  bool success = event_loop_add_file(loop, client_sock, EVENT_LOOP_READ,
                                     data_chunk_handler, conn);
  if (!success) {
    ClientConnection_free(conn);
  }
}

void request_transfer_from(PlasmaManagerState *manager_state,
                           FetchRequest *fetch_req) {
  RAY_CHECK(fetch_req->manager_vector.size() > 0);
  RAY_CHECK(fetch_req->next_manager >= 0 &&
            static_cast<size_t>(fetch_req->next_manager) <
                fetch_req->manager_vector.size());
  char addr[16];
  int port;
  parse_ip_addr_port(fetch_req->manager_vector[fetch_req->next_manager].c_str(),
                     addr, &port);

  ClientConnection *manager_conn =
      get_manager_connection(manager_state, addr, port);
  if (manager_conn != NULL) {
    /* Check that this manager isn't trying to request an object from itself.
     * TODO(rkn): Later this should not be fatal. */
    uint8_t temp_addr[4];
    sscanf(addr, "%hhu.%hhu.%hhu.%hhu", &temp_addr[0], &temp_addr[1],
           &temp_addr[2], &temp_addr[3]);
    if (memcmp(temp_addr, manager_state->addr, 4) == 0 &&
        port == manager_state->port) {
      RAY_LOG(FATAL) << "This manager is attempting to request a transfer from "
                     << "itself.";
    }

    PlasmaRequestBuffer *transfer_request = new PlasmaRequestBuffer();
    transfer_request->type = MessageType_PlasmaDataRequest;
    transfer_request->object_id = fetch_req->object_id;

    if (manager_conn->transfer_queue.size() == 0) {
      /* If we already have a connection to this manager and it's inactive,
       * (re)register it with the event loop. */
      event_loop_add_file(manager_state->loop, manager_conn->fd,
                          EVENT_LOOP_WRITE, send_queued_request, manager_conn);
    }
    /* Add this transfer request to this connection's transfer queue. */
    manager_conn->transfer_queue.push_back(transfer_request);
  }

  /* On the next attempt, try the next manager in manager_vector. */
  fetch_req->next_manager += 1;
  fetch_req->next_manager %= fetch_req->manager_vector.size();
}

int fetch_timeout_handler(event_loop *loop, timer_id id, void *context) {
  PlasmaManagerState *manager_state = (PlasmaManagerState *) context;

  /* Allocate a vector of object IDs to resend requests for location
   * notifications. */
  int num_object_ids_to_request = 0;
  int num_object_ids = manager_state->fetch_requests.size();
  /* This is allocating more space than necessary, but we do not know the exact
   * number of object IDs to request notifications for yet. */
  ObjectID *object_ids_to_request =
      (ObjectID *) malloc(num_object_ids * sizeof(ObjectID));

  /* Loop over the fetch requests and reissue requests for objects whose
   * locations we know. */
  for (auto it = manager_state->fetch_requests.begin();
       it != manager_state->fetch_requests.end(); it++) {
    FetchRequest *fetch_req = it->second;
    if (fetch_req->manager_vector.size() > 0) {
      if (is_receiving_or_received(manager_state, fetch_req->object_id)) {
        // Do nothing if the object transfer is in progress or if the object
        // has already been received.
        RAY_LOG(DEBUG) << "fetch_timeout_handler: Object in progress or "
                       << "received. " << fetch_req->object_id;
        continue;
      }
      RAY_LOG(DEBUG) << "fetch_timeout_handler: Object missing. "
                     << fetch_req->object_id;
      request_transfer_from(manager_state, fetch_req);
      /* If we've tried all of the managers that we know about for this object,
       * add this object to the list to resend requests for. */
      if (fetch_req->next_manager == 0) {
        object_ids_to_request[num_object_ids_to_request] = fetch_req->object_id;
        ++num_object_ids_to_request;
      }
    }
  }

  /* Resend requests for notifications on these objects' locations. */
  if (num_object_ids_to_request > 0 && manager_state->db != NULL) {
    object_table_request_notifications(manager_state->db,
                                       num_object_ids_to_request,
                                       object_ids_to_request, NULL);
  }
  free(object_ids_to_request);

  /* Wait at least manager_timeout_milliseconds before running this timeout
   * handler again. But if we're waiting for a large number of objects, wait
   * longer (e.g., 10 seconds for one million objects) so that we don't
   * overwhelm other components like Redis with too many requests (and so that
   * we don't overwhelm this manager with responses). */
  return std::max(RayConfig::instance().manager_timeout_milliseconds(),
                  int64_t(0.01 * num_object_ids));
}

bool is_object_local(PlasmaManagerState *state, ObjectID object_id) {
  return state->local_available_objects.count(object_id) > 0;
}

void request_transfer(ObjectID object_id,
                      const std::vector<std::string> &manager_vector,
                      void *context) {
  PlasmaManagerState *manager_state = (PlasmaManagerState *) context;
  /* This callback is called from object_table_subscribe, which guarantees that
   * the manager vector contains at least one element. */
  RAY_CHECK(manager_vector.size() >= 1);
  auto it = manager_state->fetch_requests.find(object_id);

  if (is_object_local(manager_state, object_id)) {
    /* If the object is already here, then the fetch request should have been
     * removed. */
    RAY_CHECK(it == manager_state->fetch_requests.end());
    return;
  }
  FetchRequest *fetch_req = it->second;

  /* If the object is not present, then the fetch request should still be here.
   * TODO(rkn): We actually have to remove this check to handle the rare
   * scenario where the object is transferred here and then evicted before this
   * callback gets called. */
  RAY_CHECK(fetch_req != NULL);

  /* Update the manager vector. */
  fetch_req->manager_vector = manager_vector;
  fetch_req->next_manager = 0;
  /* Wait for the object data for the default number of retries, which timeout
   * after a default interval. */

  if (!is_receiving_or_received(manager_state, object_id)) {
    // Request object if it's not already being received,
    // or if it has not already been received.
    request_transfer_from(manager_state, fetch_req);
  }
}

/* This method is only called from the tests. */
void call_request_transfer(ObjectID object_id,
                           const std::vector<std::string> &manager_vector,
                           void *context) {
  PlasmaManagerState *manager_state = (PlasmaManagerState *) context;
  /* Check that there isn't already a fetch request for this object. */
  auto it = manager_state->fetch_requests.find(object_id);
  RAY_CHECK(it == manager_state->fetch_requests.end());
  /* Create a fetch request. */
  FetchRequest *fetch_req = create_fetch_request(manager_state, object_id);
  manager_state->fetch_requests[object_id] = fetch_req;
  request_transfer(object_id, manager_vector, context);
}

void fatal_table_callback(ObjectID id, void *user_context, void *user_data) {
  RAY_CHECK(0);
}

/* This callback is used by both fetch and wait. Therefore, it may have to
 * handle outstanding fetch and wait requests. */
void object_table_subscribe_callback(ObjectID object_id,
                                     int64_t data_size,
                                     const std::vector<DBClientID> &manager_ids,
                                     void *context) {
  PlasmaManagerState *manager_state = (PlasmaManagerState *) context;
  const std::vector<std::string> managers =
      db_client_table_get_ip_addresses(manager_state->db, manager_ids);
  /* Run the callback for fetch requests if there is a fetch request. */
  auto it = manager_state->fetch_requests.find(object_id);

  if (it != manager_state->fetch_requests.end()) {
    request_transfer(object_id, managers, context);
  }
  /* Run the callback for wait requests. */
  update_object_wait_requests(manager_state, object_id,
                              plasma::PLASMA_QUERY_ANYWHERE,
                              ObjectStatus_Remote);
}

void process_fetch_requests(ClientConnection *client_conn,
                            int num_object_ids,
                            plasma::ObjectID object_ids[]) {
  PlasmaManagerState *manager_state = client_conn->manager_state;

  int num_object_ids_to_request = 0;
  /* This is allocating more space than necessary, but we do not know the exact
   * number of object IDs to request notifications for yet. */
  ObjectID *object_ids_to_request =
      (ObjectID *) malloc(num_object_ids * sizeof(ObjectID));

  for (int i = 0; i < num_object_ids; ++i) {
    ObjectID obj_id = object_ids[i];

    /* Check if this object is already present locally. If so, do nothing. */
    if (is_object_local(manager_state, obj_id)) {
      continue;
    }

    /* Check if this object is already being fetched. If so, do nothing. */
    auto it = manager_state->fetch_requests.find(obj_id);
    if (it != manager_state->fetch_requests.end()) {
      continue;
    }

    /* Add an entry to the fetch requests data structure to indidate that the
     * object is being fetched. */
    FetchRequest *entry = create_fetch_request(manager_state, obj_id);
    manager_state->fetch_requests[obj_id] = entry;
    /* Add this object ID to the list of object IDs to request notifications for
     * from the object table. */
    object_ids_to_request[num_object_ids_to_request] = obj_id;
    num_object_ids_to_request += 1;
  }
  if (num_object_ids_to_request > 0) {
    /* Request notifications from the object table when these object IDs become
     * available. The notifications will call the callback that was passed to
     * object_table_subscribe_to_notifications, which will initiate a transfer
     * of the object to this plasma manager. */
    object_table_request_notifications(manager_state->db,
                                       num_object_ids_to_request,
                                       object_ids_to_request, NULL);
  }
  free(object_ids_to_request);
}

int wait_timeout_handler(event_loop *loop, timer_id id, void *context) {
  WaitRequest *wait_req = (WaitRequest *) context;
  return_from_wait(wait_req->client_conn->manager_state, wait_req);
  return EVENT_LOOP_TIMER_DONE;
}

void process_wait_request(ClientConnection *client_conn,
                          plasma::ObjectRequestMap &&object_requests,
                          uint64_t timeout_ms,
                          int num_ready_objects) {
  RAY_CHECK(client_conn != NULL);
  PlasmaManagerState *manager_state = client_conn->manager_state;
  int num_object_requests = object_requests.size();

  /* Create a wait request for this object. */
  WaitRequest *wait_req =
      new WaitRequest(client_conn, -1, num_object_requests,
                      std::move(object_requests), num_ready_objects, 0);

  int num_object_ids_to_request = 0;
  /* This is allocating more space than necessary, but we do not know the exact
   * number of object IDs to request notifications for yet. */
  ObjectID *object_ids_to_request =
      (ObjectID *) malloc(num_object_requests * sizeof(ObjectID));

  for (auto &entry : wait_req->object_requests) {
    auto &object_request = entry.second;
    ObjectID obj_id = object_request.object_id;

    /* Check if this object is already present locally. If so, mark the object
     * as present. */
    if (is_object_local(manager_state, obj_id)) {
      object_request.status = ObjectStatus_Local;
      wait_req->num_satisfied += 1;
      continue;
    }

    /* Add the wait request to the relevant data structures. */
    add_wait_request_for_object(manager_state, obj_id, object_request.type,
                                wait_req);

    if (object_request.type == plasma::PLASMA_QUERY_LOCAL) {
      /* TODO(rkn): If desired, we could issue a fetch command here to retrieve
       * the object. */
    } else if (object_request.type == plasma::PLASMA_QUERY_ANYWHERE) {
      /* Add this object ID to the list of object IDs to request notifications
       * for from the object table. */
      object_ids_to_request[num_object_ids_to_request] = obj_id;
      num_object_ids_to_request += 1;
    } else {
      /* This code should be unreachable. */
      RAY_CHECK(0);
    }
  }

  /* If enough of the wait requests have already been satisfied, return to the
   * client. */
  if (wait_req->num_satisfied >= wait_req->num_objects_to_wait_for) {
    return_from_wait(manager_state, wait_req);
  } else {
    if (num_object_ids_to_request > 0) {
      /* Request notifications from the object table when these object IDs
       * become available. The notifications will call the callback that was
       * passed to object_table_subscribe_to_notifications, which will update
       * the wait request. */
      object_table_request_notifications(manager_state->db,
                                         num_object_ids_to_request,
                                         object_ids_to_request, NULL);
    }

    /* Set a timer that will cause the wait request to return to the client. */
    wait_req->timer = event_loop_add_timer(manager_state->loop, timeout_ms,
                                           wait_timeout_handler, wait_req);
  }
  free(object_ids_to_request);
}

/**
 * Check whether a non-local object is stored on any remot enote or not.
 *
 * @param object_id ID of the object whose status we require.
 * @param never_created True if the object has not been created yet and false
 *        otherwise.
 * @param manager_vector Vector containing the addresses of the Plasma Managers
 *        that have the object.
 * @param context Client connection.
 * @return Void.
 */
void request_status_done(ObjectID object_id,
                         bool never_created,
                         const std::vector<DBClientID> &manager_vector,
                         void *context) {
  ClientConnection *client_conn = (ClientConnection *) context;
  int status = request_status(object_id, manager_vector, context);
  plasma::ObjectID object_id_copy = object_id.to_plasma_id();
  handle_sigpipe(
      plasma::SendStatusReply(client_conn->fd, &object_id_copy, &status, 1),
      client_conn->fd);
}

int request_status(ObjectID object_id,
                   const std::vector<DBClientID> &manager_vector,
                   void *context) {
  ClientConnection *client_conn = (ClientConnection *) context;

  /* Return success immediately if we already have this object. */
  if (is_object_local(client_conn->manager_state, object_id)) {
    return ObjectStatus_Local;
  }

  /* Since object is not stored at the local locally, manager_vector.size() > 0
   * means that the object is stored at another remote object. Otherwise, if
   * manager_vector.size() == 0, the object is not stored anywhere. */
  return (manager_vector.size() > 0 ? ObjectStatus_Remote
                                    : ObjectStatus_Nonexistent);
}

void object_table_lookup_fail_callback(ObjectID object_id,
                                       void *user_context,
                                       void *user_data) {
  /* Fail for now. Later, we may want to send a ObjectStatus_Nonexistent to the
   * client. */
  RAY_CHECK(0);
}

void process_status_request(ClientConnection *client_conn,
                            plasma::ObjectID object_id) {
  /* Return success immediately if we already have this object. */
  if (is_object_local(client_conn->manager_state, object_id)) {
    int status = ObjectStatus_Local;
    handle_sigpipe(
        plasma::SendStatusReply(client_conn->fd, &object_id, &status, 1),
        client_conn->fd);
    return;
  }

  if (client_conn->manager_state->db == NULL) {
    int status = ObjectStatus_Nonexistent;
    handle_sigpipe(
        plasma::SendStatusReply(client_conn->fd, &object_id, &status, 1),
        client_conn->fd);
    return;
  }

  /* The object is not local, so check whether it is stored remotely. */
  object_table_lookup(client_conn->manager_state->db, object_id, NULL,
                      request_status_done, client_conn);
}

void process_delete_object_notification(PlasmaManagerState *state,
                                        ObjectID object_id) {
  state->local_available_objects.erase(object_id);

  /* Remove this object from the (redis) object table. */
  if (state->db) {
    object_table_remove(state->db, object_id, NULL, NULL, NULL, NULL);
  }

  /* NOTE: There could be pending wait requests for this object that will now
   * return when the object is not actually available. For simplicity, we allow
   * this scenario rather than try to keep the wait request statuses exactly
   * up-to-date. */
}

void log_object_hash_mismatch_error_task_callback(Task *task,
                                                  void *user_context) {
  RAY_CHECK(task != NULL);
  PlasmaManagerState *state = (PlasmaManagerState *) user_context;
  TaskSpec *spec = Task_task_execution_spec(task)->Spec();
  /* Push the error to the Python driver that caused the nondeterministic task
   * to be submitted. */
  std::ostringstream error_message;
  error_message << "An object created by the task with ID "
                << TaskSpec_task_id(spec) << " was created with a different "
                << "hash. This may mean that a non-deterministic task was "
                << "reexecuted.";
  push_error(state->db, TaskSpec_driver_id(spec),
             ErrorIndex::OBJECT_HASH_MISMATCH, error_message.str());
}

void log_object_hash_mismatch_error_result_callback(ObjectID object_id,
                                                    TaskID task_id,
                                                    bool is_put,
                                                    void *user_context) {
  RAY_CHECK(!task_id.is_nil());
  PlasmaManagerState *state = (PlasmaManagerState *) user_context;
/* Get the specification for the nondeterministic task. */
#if !RAY_USE_NEW_GCS
  task_table_get_task(state->db, task_id, NULL,
                      log_object_hash_mismatch_error_task_callback, state);
#else
  RAY_CHECK_OK(state->gcs_client.task_table().Lookup(
      ray::JobID::nil(), task_id,
      [user_context](gcs::AsyncGcsClient *, const TaskID &,
                     const TaskTableDataT &t) {
        Task *task = Task_alloc(t.task_info.data(), t.task_info.size(),
                                static_cast<TaskStatus>(t.scheduling_state),
                                DBClientID::from_binary(t.scheduler_id),
                                std::vector<ObjectID>());
        log_object_hash_mismatch_error_task_callback(task, user_context);
        Task_free(task);
      },
      [](gcs::AsyncGcsClient *, const TaskID &) {
        // TODO(pcmoritz): Handle failure.
      }));
#endif
}

void log_object_hash_mismatch_error_object_callback(ObjectID object_id,
                                                    bool success,
                                                    void *user_context) {
  if (success) {
    /* The object was added successfully. */
    return;
  }

  /* The object was added, but there was an object hash mismatch. In this case,
   * look up the task that created the object so we can notify the Python
   * driver that the task is nondeterministic. */
  PlasmaManagerState *state = (PlasmaManagerState *) user_context;
  result_table_lookup(state->db, object_id, NULL,
                      log_object_hash_mismatch_error_result_callback, state);
}

void process_add_object_notification(PlasmaManagerState *state,
                                     ObjectID object_id,
                                     int64_t data_size,
                                     int64_t metadata_size,
                                     unsigned char *digest) {
  state->local_available_objects.insert(object_id);
  if (state->receives_in_progress.count(object_id) > 0) {
    // This object is now locally available, so remove it from the
    // receives_in_progress set.
    state->receives_in_progress.erase(object_id);
  }

  /* Add this object to the (redis) object table. */
  if (state->db) {
    object_table_add(state->db, object_id, data_size + metadata_size, digest,
                     NULL, log_object_hash_mismatch_error_object_callback,
                     (void *) state);
  }

  /* If we were trying to fetch this object, finish up the fetch request. */
  auto it = state->fetch_requests.find(object_id);
  if (it != state->fetch_requests.end()) {
    remove_fetch_request(state, it->second);
    /* TODO(rkn): We also really should unsubscribe from the object table. */
  }

  /* Update the in-progress local and remote wait requests. */
  update_object_wait_requests(state, object_id, plasma::PLASMA_QUERY_LOCAL,
                              ObjectStatus_Local);
  update_object_wait_requests(state, object_id, plasma::PLASMA_QUERY_ANYWHERE,
                              ObjectStatus_Local);
}

void process_object_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events) {
  PlasmaManagerState *state = (PlasmaManagerState *) context;
  uint8_t *notification = read_message_async(loop, client_sock);
  if (notification == NULL) {
    PlasmaManagerState_free(state);
    RAY_LOG(FATAL) << "Lost connection to the plasma store, plasma manager is "
                   << "exiting!";
  }
  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification);
  /* Add object to locally available object. */
  ObjectID object_id = from_flatbuf(*object_info->object_id());
  if (object_info->is_deletion()) {
    process_delete_object_notification(state, object_id);
  } else {
    process_add_object_notification(
        state, object_id, object_info->data_size(),
        object_info->metadata_size(),
        (unsigned char *) object_info->digest()->data());
  }
  free(notification);
}

/* TODO(pcm): Split this into two methods: new_worker_connection
 * and new_manager_connection and also split ClientConnection
 * into two structs, one for workers and one for other plasma managers. */
ClientConnection *ClientConnection_init(PlasmaManagerState *state,
                                        int client_sock,
                                        std::string const &client_key) {
  /* Create a new data connection context per client. */
  ClientConnection *conn = new ClientConnection();
  conn->manager_state = state;
  ClientConnection_finish_request(conn);
  conn->fd = client_sock;
  conn->num_return_objects = 0;

  conn->ip_addr_port = client_key;
  state->manager_connections[client_key] = conn;
  return conn;
}

ClientConnection *ClientConnection_listen(event_loop *loop,
                                          int listener_sock,
                                          void *context,
                                          int events) {
  PlasmaManagerState *state = (PlasmaManagerState *) context;
  int new_socket = accept_client(listener_sock);
  char client_key[8];
  snprintf(client_key, sizeof(client_key), "%d", new_socket);
  ClientConnection *conn = ClientConnection_init(state, new_socket, client_key);

  event_loop_add_file(loop, new_socket, EVENT_LOOP_READ, process_message, conn);
  RAY_LOG(DEBUG) << "New client connection with fd " << new_socket;
  return conn;
}

void ClientConnection_free(ClientConnection *client_conn) {
  PlasmaManagerState *state = client_conn->manager_state;
  state->manager_connections.erase(client_conn->ip_addr_port);

  client_conn->pending_object_transfers.clear();

  /* Free the transfer queue. */
  while (client_conn->transfer_queue.size()) {
    delete client_conn->transfer_queue.front();
    client_conn->transfer_queue.pop_front();
  }
  /* Close the manager connection and free the remaining state. */
  close(client_conn->fd);
  delete client_conn;
}

void handle_new_client(event_loop *loop,
                       int listener_sock,
                       void *context,
                       int events) {
  (void) ClientConnection_listen(loop, listener_sock, context, events);
}

int get_client_sock(ClientConnection *conn) {
  return conn->fd;
}

void process_message(event_loop *loop,
                     int client_sock,
                     void *context,
                     int events) {
  int64_t start_time = current_time_ms();

  ClientConnection *conn = (ClientConnection *) context;

  int64_t length;
  int64_t type;
  uint8_t *data;
  read_message(client_sock, &type, &length, &data);

  switch (type) {
  case MessageType_PlasmaDataRequest: {
    RAY_LOG(DEBUG) << "Processing data request";
    plasma::ObjectID object_id;
    char *address;
    int port;
    ARROW_CHECK_OK(
        plasma::ReadDataRequest(data, length, &object_id, &address, &port));
    process_transfer_request(loop, object_id, address, port, conn);
    free(address);
  } break;
  case MessageType_PlasmaDataReply: {
    RAY_LOG(DEBUG) << "Processing data reply";
    plasma::ObjectID object_id;
    int64_t object_size;
    int64_t metadata_size;
    ARROW_CHECK_OK(plasma::ReadDataReply(data, length, &object_id, &object_size,
                                         &metadata_size));
    process_data_request(loop, client_sock, object_id, object_size,
                         metadata_size, conn);
  } break;
  case MessageType_PlasmaFetchRequest: {
    RAY_LOG(DEBUG) << "Processing fetch remote";
    std::vector<plasma::ObjectID> object_ids_to_fetch;
    /* TODO(pcm): process_fetch_requests allocates an array of num_objects
     * object_ids too so these should be shared in the future. */
    ARROW_CHECK_OK(plasma::ReadFetchRequest(data, length, object_ids_to_fetch));
    process_fetch_requests(conn, object_ids_to_fetch.size(),
                           object_ids_to_fetch.data());
  } break;
  case MessageType_PlasmaWaitRequest: {
    RAY_LOG(DEBUG) << "Processing wait";
    plasma::ObjectRequestMap object_requests;
    int64_t timeout_ms;
    int num_ready_objects;
    ARROW_CHECK_OK(plasma::ReadWaitRequest(data, length, object_requests,
                                           &timeout_ms, &num_ready_objects));
    process_wait_request(conn, std::move(object_requests), timeout_ms,
                         num_ready_objects);
  } break;
  case MessageType_PlasmaStatusRequest: {
    RAY_LOG(DEBUG) << "Processing status";
    plasma::ObjectID object_id;
    ARROW_CHECK_OK(plasma::ReadStatusRequest(data, length, &object_id, 1));
    process_status_request(conn, object_id);
  } break;
  case static_cast<int64_t>(CommonMessageType::DISCONNECT_CLIENT): {
    RAY_LOG(DEBUG) << "Disconnecting client on fd " << client_sock;
    event_loop_remove_file(loop, client_sock);
    ClientConnection_free(conn);
  } break;
  default:
    RAY_LOG(FATAL) << "invalid request " << type;
  }
  free(data);

  /* Print a warning if this method took too long. */
  int64_t end_time = current_time_ms();
  if (end_time - start_time >
      RayConfig::instance().max_time_for_handler_milliseconds()) {
    RAY_LOG(WARNING) << "process_message of type " << type << " took "
                     << end_time - start_time << " milliseconds.";
  }
}

int heartbeat_handler(event_loop *loop, timer_id id, void *context) {
  PlasmaManagerState *state = (PlasmaManagerState *) context;

  /* Check that the last heartbeat was not sent too long ago. */
  int64_t current_time = current_time_ms();
  RAY_CHECK(current_time >= state->previous_heartbeat_time);
  if (current_time - state->previous_heartbeat_time >
      RayConfig::instance().num_heartbeats_timeout() *
          RayConfig::instance().heartbeat_timeout_milliseconds()) {
    RAY_LOG(FATAL) << "The last heartbeat was sent "
                   << current_time - state->previous_heartbeat_time
                   << " milliseconds ago.";
  }
  state->previous_heartbeat_time = current_time;

  plasma_manager_send_heartbeat(state->db);
  return RayConfig::instance().heartbeat_timeout_milliseconds();
}

void start_server(const char *store_socket_name,
                  const char *manager_socket_name,
                  const char *master_addr,
                  int port,
                  const char *redis_primary_addr,
                  int redis_primary_port) {
  /* Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
   * to a client that has already died, the manager could die. */
  signal(SIGPIPE, SIG_IGN);
  /* Bind the sockets before we try to connect to the plasma store.
   * In case the bind does not succeed, we want to be able to exit
   * without breaking the pipe to the store. */
  int remote_sock = bind_inet_sock(port, false);
  if (remote_sock < 0) {
    exit(EXIT_COULD_NOT_BIND_PORT);
  }

  int local_sock = bind_ipc_sock(manager_socket_name, false);
  RAY_CHECK(local_sock >= 0) << "Unable to bind local manager socket";

  g_manager_state = PlasmaManagerState_init(
      store_socket_name, manager_socket_name, master_addr, port,
      redis_primary_addr, redis_primary_port);
  RAY_CHECK(g_manager_state);

  RAY_CHECK(listen(remote_sock, 128) != -1);
  RAY_CHECK(listen(local_sock, 128) != -1);

  RAY_LOG(DEBUG) << "Started server connected to store " << store_socket_name
                 << ", listening on port " << port;
  event_loop_add_file(g_manager_state->loop, local_sock, EVENT_LOOP_READ,
                      handle_new_client, g_manager_state);
  event_loop_add_file(g_manager_state->loop, remote_sock, EVENT_LOOP_READ,
                      handle_new_client, g_manager_state);
  /* Set up a client-specific channel to receive notifications from the object
   * table. */
  object_table_subscribe_to_notifications(g_manager_state->db, false,
                                          object_table_subscribe_callback,
                                          g_manager_state, NULL, NULL, NULL);
  /* Set up a recurring timer that will loop through the outstanding fetch
   * requests and reissue requests for transfers of those objects. */
  event_loop_add_timer(g_manager_state->loop,
                       RayConfig::instance().manager_timeout_milliseconds(),
                       fetch_timeout_handler, g_manager_state);
  /* Publish the heartbeats to all subscribers of the plasma manager table. */
  event_loop_add_timer(g_manager_state->loop,
                       RayConfig::instance().heartbeat_timeout_milliseconds(),
                       heartbeat_handler, g_manager_state);
  /* Run the event loop. */
  event_loop_run(g_manager_state->loop);
}

/* Report "success" to valgrind. */
void signal_handler(int signal) {
  RAY_LOG(DEBUG) << "Signal was " << signal;
  if (signal == SIGTERM) {
    if (g_manager_state) {
      PlasmaManagerState_free(g_manager_state);
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
  /* IP address and port of the primary redis instance. */
  char *redis_primary_addr_port = NULL;
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
      redis_primary_addr_port = optarg;
      break;
    default:
      RAY_LOG(FATAL) << "unknown option " << c;
    }
  }
  if (!store_socket_name) {
    RAY_LOG(FATAL) << "please specify socket for connecting to the plasma "
                   << "store with -s switch";
  }
  if (!manager_socket_name) {
    RAY_LOG(FATAL) << "please specify socket name of the manager's local "
                   << "socket with -m switch";
  }
  if (!master_addr) {
    RAY_LOG(FATAL) << "please specify ip address of the current host in the "
                   << "format 123.456.789.10 with -h switch";
  }
  if (port == -1) {
    RAY_LOG(FATAL) << "please specify port the plasma manager shall listen to "
                   << "in the format 12345 with -p switch";
  }
  char redis_primary_addr[16];
  int redis_primary_port = -1;
  if (!redis_primary_addr_port ||
      parse_ip_addr_port(redis_primary_addr_port, redis_primary_addr,
                         &redis_primary_port) == -1) {
    RAY_LOG(FATAL) << "specify the primary redis address like 127.0.0.1:6379 "
                   << "with the -r switch";
  }
  start_server(store_socket_name, manager_socket_name, master_addr, port,
               redis_primary_addr, redis_primary_port);
}
#endif
