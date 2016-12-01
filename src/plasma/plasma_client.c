/* PLASMA CLIENT: Client library for using the plasma store and manager */

#ifdef _WIN32
#include <Win32_Interop/win32_types.h>
#endif

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <netdb.h>
#include <poll.h>

#include "common.h"
#include "io.h"
#include "plasma.h"
#include "plasma_client.h"
#include "fling.h"
#include "uthash.h"
#include "utringbuffer.h"

/* Number of times we try connecting to a socket. */
#define NUM_CONNECT_ATTEMPTS 50
#define CONNECT_TIMEOUT 100

#ifndef _WIN32
/* This function is actually not declared in standard POSIX, so declare it. */
extern int usleep(useconds_t usec);
#endif

typedef struct {
  /** Key that uniquely identifies the  memory mapped file. In practice, we
   *  take the numerical value of the file descriptor in the object store. */
  int key;
  /** The result of mmap for this file descriptor. */
  uint8_t *pointer;
  /** The length of the memory-mapped file. */
  size_t length;
  /** The number of objects in this memory-mapped file that are currently being
   *  used by the client. When this count reaches zeros, we unmap the file. */
  int count;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} client_mmap_table_entry;

typedef struct {
  /** The ID of the object. This is used as the key in the hash table. */
  object_id object_id;
  /** The file descriptor of the memory-mapped file that contains the object. */
  int fd;
  /** A count of the number of times this client has called plasma_create or
   *  plasma_get on this object ID minus the number of calls to plasma_release.
   *  When this count reaches zero, we remove the entry from the objects_in_use
   *  and decrement a count in the relevant client_mmap_table_entry. */
  int count;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} object_in_use_entry;

/** Configuration options for the plasma client. */
typedef struct {
  /** Number of release calls we wait until the object is actually released.
   *  This allows us to avoid invalidating the cpu cache on workers if objects
   *  are reused accross tasks. */
  int release_delay;
} plasma_client_config;

/** Information about a connection between a Plasma Client and Plasma Store.
 *  This is used to avoid mapping the same files into memory multiple times. */
struct plasma_connection {
  /** File descriptor of the Unix domain socket that connects to the store. */
  int store_conn;
  /** File descriptor of the Unix domain socket that connects to the manager. */
  int manager_conn;
  /** File descriptor of the Unix domain socket on which client receives event
   *  notifications for the objects it subscribes for when these objects are
   *  sealed either locally or remotely. */
  int manager_conn_subscribe;
  /** Table of dlmalloc buffer files that have been memory mapped so far. This
   *  is a hash table mapping a file descriptor to a struct containing the
   *  address of the corresponding memory-mapped file. */
  client_mmap_table_entry *mmap_table;
  /** A hash table of the object IDs that are currently being used by this
   * client. */
  object_in_use_entry *objects_in_use;
  /** Object IDs of the last few release calls. This is used to delay releasing
   *  objects to see if they can be reused by subsequent tasks so we do not
   *  unneccessarily invalidate cpu caches. TODO(pcm): replace this with a
   *  proper lru cache of size sizeof(L3 cache). */
  UT_ringbuffer *release_history;
  /** Configuration options for the plasma client. */
  plasma_client_config config;
};

/* If the file descriptor fd has been mmapped in this client process before,
 * return the pointer that was returned by mmap, otherwise mmap it and store the
 * pointer in a hash table. */
uint8_t *lookup_or_mmap(plasma_connection *conn,
                        int fd,
                        int store_fd_val,
                        int64_t map_size) {
  client_mmap_table_entry *entry;
  HASH_FIND_INT(conn->mmap_table, &store_fd_val, entry);
  if (entry) {
    close(fd);
    return entry->pointer;
  } else {
    uint8_t *result =
        mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (result == MAP_FAILED) {
      LOG_FATAL("mmap failed");
    }
    close(fd);
    entry = malloc(sizeof(client_mmap_table_entry));
    entry->key = store_fd_val;
    entry->pointer = result;
    entry->length = map_size;
    entry->count = 0;
    HASH_ADD_INT(conn->mmap_table, key, entry);
    return result;
  }
}

void increment_object_count(plasma_connection *conn,
                            object_id object_id,
                            int fd) {
  /* Increment the count of the object to track the fact that it is being used.
   * The corresponding decrement should happen in plasma_release. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &object_id, sizeof(object_id),
            object_entry);
  if (object_entry == NULL) {
    /* Add this object ID to the hash table of object IDs in use. The
     * corresponding call to free happens in plasma_release. */
    object_entry = malloc(sizeof(object_in_use_entry));
    object_entry->object_id = object_id;
    object_entry->fd = fd;
    object_entry->count = 0;
    HASH_ADD(hh, conn->objects_in_use, object_id, sizeof(object_id),
             object_entry);
    /* Increment the count of the number of objects in the memory-mapped file
     * that are being used. The corresponding decrement should happen in
     * plasma_release. */
    client_mmap_table_entry *entry;
    HASH_FIND_INT(conn->mmap_table, &object_entry->fd, entry);
    CHECK(entry != NULL);
    CHECK(entry->count >= 0);
    entry->count += 1;
  } else {
    CHECK(object_entry->count > 0);
  }
  /* Increment the count of the number of instances of this object that are
   * being used by this client. The corresponding decrement should happen in
   * plasma_release. */
  object_entry->count += 1;
}

bool plasma_create(plasma_connection *conn,
                   object_id object_id,
                   int64_t data_size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data) {
  LOG_DEBUG("called plasma_create on conn %d with size %" PRId64
            " and metadata size %" PRId64,
            conn->store_conn, data_size, metadata_size);
  plasma_request req = plasma_make_request(object_id);
  req.data_size = data_size;
  req.metadata_size = metadata_size;
  CHECK(plasma_send_request(conn->store_conn, PLASMA_CREATE, &req) >= 0);
  plasma_reply reply;
  CHECK(plasma_receive_reply(conn->store_conn, sizeof(reply), &reply) >= 0);
  int fd = recv_fd(conn->store_conn);
  CHECKM(fd >= 0, "recv not successful");
  if (reply.error_code == PLASMA_REPLY_OBJECT_ALREADY_EXISTS) {
    LOG_DEBUG("returned from plasma_create with error %d", reply.error_code);
    return false;
  }
  plasma_object *object = &reply.object;
  CHECK(object->data_size == data_size);
  CHECK(object->metadata_size == metadata_size);
  /* The metadata should come right after the data. */
  CHECK(object->metadata_offset == object->data_offset + data_size);
  *data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                         object->handle.mmap_size) +
          object->data_offset;
  /* If plasma_create is being called from a transfer, then we will not copy the
   * metadata here. The metadata will be written along with the data streamed
   * from the transfer. */
  if (metadata != NULL) {
    /* Copy the metadata to the buffer. */
    memcpy(*data + object->data_size, metadata, metadata_size);
  }
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. */
  increment_object_count(conn, object_id, object->handle.store_fd);
  return true;
}

/* This method is used to get both the data and the metadata. */
void plasma_get(plasma_connection *conn,
                object_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata) {
  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->store_conn, PLASMA_GET, &req) >= 0);
  plasma_reply reply;
  CHECK(plasma_receive_reply(conn->store_conn, sizeof(reply), &reply) >= 0);
  int fd = recv_fd(conn->store_conn);
  CHECK(fd >= 0);
  plasma_object *object = &reply.object;
  *data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                         object->handle.mmap_size) +
          object->data_offset;
  *size = object->data_size;
  /* If requested, return the metadata as well. */
  if (metadata != NULL) {
    *metadata = *data + object->data_size;
    *metadata_size = object->metadata_size;
  }
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. */
  increment_object_count(conn, object_id, object->handle.store_fd);
}

void plasma_perform_release(plasma_connection *conn, object_id object_id) {
  /* Decrement the count of the number of instances of this object that are
   * being used by this client. The corresponding increment should have happened
   * in plasma_get. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &object_id, sizeof(object_id),
            object_entry);
  CHECK(object_entry != NULL);
  object_entry->count -= 1;
  CHECK(object_entry->count >= 0);
  /* Check if the client is no longer using this object. */
  if (object_entry->count == 0) {
    /* Decrement the count of the number of objects in this memory-mapped file
     * that the client is using. The corresponding increment should have
     * happened in plasma_get. */
    client_mmap_table_entry *entry;
    HASH_FIND_INT(conn->mmap_table, &object_entry->fd, entry);
    CHECK(entry != NULL);
    entry->count -= 1;
    CHECK(entry->count >= 0);
    /* If none are being used then unmap the file. */
    if (entry->count == 0) {
      munmap(entry->pointer, entry->length);
      /* Remove the corresponding entry from the hash table. */
      HASH_DELETE(hh, conn->mmap_table, entry);
      free(entry);
    }
    /* Tell the store that the client no longer needs the object. */
    plasma_request req = plasma_make_request(object_id);
    CHECK(plasma_send_request(conn->store_conn, PLASMA_RELEASE, &req) >= 0);
    /* Remove the entry from the hash table of objects currently in use. */
    HASH_DELETE(hh, conn->objects_in_use, object_entry);
    free(object_entry);
  }
}

void plasma_release(plasma_connection *conn, object_id obj_id) {
  /* If no ringbuffer is used, don't delay the release. */
  if (conn->config.release_delay == 0) {
    plasma_perform_release(conn, obj_id);
  } else if (!utringbuffer_full(conn->release_history)) {
    /* Delay the release by storing new releases into a ringbuffer and only
     * popping them off and actually releasing if the buffer is full. This is
     * so consecutive tasks don't release and map again objects and invalidate
     * the cpu cache this way. */
    utringbuffer_push_back(conn->release_history, &obj_id);
  } else {
    object_id object_id_to_release =
        *(object_id *) utringbuffer_front(conn->release_history);
    utringbuffer_push_back(conn->release_history, &obj_id);
    plasma_perform_release(conn, object_id_to_release);
  }
}

/* This method is used to query whether the plasma store contains an object. */
void plasma_contains(plasma_connection *conn,
                     object_id object_id,
                     int *has_object) {
  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->store_conn, PLASMA_CONTAINS, &req) >= 0);
  plasma_reply reply;
  CHECK(plasma_receive_reply(conn->store_conn, sizeof(reply), &reply) >= 0);
  *has_object = reply.has_object;
}

void plasma_seal(plasma_connection *conn, object_id object_id) {
  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->store_conn, PLASMA_SEAL, &req) >= 0);
  if (conn->manager_conn >= 0) {
    CHECK(plasma_send_request(conn->manager_conn, PLASMA_SEAL, &req) >= 0);
  }
}

void plasma_delete(plasma_connection *conn, object_id object_id) {
  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->store_conn, PLASMA_DELETE, &req) >= 0);
}

int64_t plasma_evict(plasma_connection *conn, int64_t num_bytes) {
  /* Send a request to the store to evict objects. */
  plasma_request req;
  memset(&req, 0, sizeof(req));
  req.num_bytes = num_bytes;
  CHECK(plasma_send_request(conn->store_conn, PLASMA_EVICT, &req) >= 0);
  /* Wait for a response with the number of bytes actually evicted. */
  plasma_reply reply;
  CHECK(plasma_receive_reply(conn->store_conn, sizeof(reply), &reply) >= 0);
  return reply.num_bytes;
}

int plasma_subscribe(plasma_connection *conn) {
  int fd[2];
  /* TODO: Just create 1 socket, bind it to port 0 to find a free port, and
   * send the port number instead, and let the client connect. */
  /* Create a non-blocking socket pair. This will only be used to send
   * notifications from the Plasma store to the client. */
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  /* Make the socket non-blocking. */
  int flags = fcntl(fd[1], F_GETFL, 0);
  CHECK(fcntl(fd[1], F_SETFL, flags | O_NONBLOCK) == 0);
  /* Tell the Plasma store about the subscription. */
  plasma_request req = {0};
  CHECK(plasma_send_request(conn->store_conn, PLASMA_SUBSCRIBE, &req) >= 0);
  /* Send the file descriptor that the Plasma store should use to push
   * notifications about sealed objects to this client. */
  CHECK(send_fd(conn->store_conn, fd[1]) >= 0);
  close(fd[1]);
  /* Return the file descriptor that the client should use to read notifications
   * about sealed objects. */
  return fd[0];
}

int socket_connect_retry(const char *socket_name,
                         int num_retries,
                         int64_t timeout) {
  CHECK(socket_name);
  int fd = -1;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    fd = connect_ipc_sock(socket_name);
    if (fd >= 0) {
      break;
    }
    /* Sleep for timeout milliseconds. */
    usleep(timeout * 1000);
  }
  /* If we could not connect to the socket, exit. */
  if (fd == -1) {
    LOG_FATAL("could not connect to socket %s", socket_name);
  }
  return fd;
}

plasma_connection *plasma_connect(const char *store_socket_name,
                                  const char *manager_socket_name,
                                  int release_delay) {
  /* Initialize the store connection struct */
  plasma_connection *result = malloc(sizeof(plasma_connection));
  result->store_conn = socket_connect_retry(
      store_socket_name, NUM_CONNECT_ATTEMPTS, CONNECT_TIMEOUT);
  if (manager_socket_name != NULL) {
    result->manager_conn = socket_connect_retry(
        manager_socket_name, NUM_CONNECT_ATTEMPTS, CONNECT_TIMEOUT);
  } else {
    result->manager_conn = -1;
  }
  result->mmap_table = NULL;
  result->objects_in_use = NULL;
  result->config.release_delay = release_delay;
  utringbuffer_new(result->release_history, release_delay, &object_id_icd);
  return result;
}

void plasma_disconnect(plasma_connection *conn) {
  object_id *id = NULL;
  while ((id = (object_id *) utringbuffer_next(conn->release_history, id))) {
    plasma_perform_release(conn, *id);
  }
  utringbuffer_free(conn->release_history);
  close(conn->store_conn);
  if (conn->manager_conn >= 0) {
    close(conn->manager_conn);
  }
  free(conn);
}

bool plasma_manager_is_connected(plasma_connection *conn) {
  return conn->manager_conn >= 0;
}

#define h_addr h_addr_list[0]

int plasma_manager_try_connect(const char *ip_addr, int port) {
  int fd = socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  struct hostent *manager = gethostbyname(ip_addr); /* TODO(pcm): cache this */
  if (!manager) {
    return -1;
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  memcpy(&addr.sin_addr.s_addr, manager->h_addr, manager->h_length);
  addr.sin_port = htons(port);

  int r = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
  if (r < 0) {
    return -1;
  }
  return fd;
}

int plasma_manager_connect(const char *ip_addr, int port) {
  /* Try to connect to the Plasma manager. If unsuccessful, retry several times.
   */
  int fd = -1;
  for (int num_attempts = 0; num_attempts < NUM_CONNECT_ATTEMPTS;
       ++num_attempts) {
    fd = plasma_manager_try_connect(ip_addr, port);
    if (fd >= 0) {
      break;
    }
    /* Sleep for 100 milliseconds. */
    usleep(100000);
  }
  if (fd < 0) {
    LOG_WARN("Unable to connect to plasma manager at %s:%d", ip_addr, port);
  }
  return fd;
}

void plasma_transfer(plasma_connection *conn,
                     const char *addr,
                     int port,
                     object_id object_id) {
  plasma_request req = plasma_make_request(object_id);
  req.port = port;
  char *end = NULL;
  for (int i = 0; i < 4; ++i) {
    req.addr[i] = strtol(end ? end : addr, &end, 10);
    /* skip the '.' */
    end += 1;
  }
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_TRANSFER, &req) >= 0);
}

void plasma_fetch(plasma_connection *conn,
                  int num_object_ids,
                  object_id object_ids[],
                  int is_fetched[]) {
  CHECK(conn->manager_conn >= 0);
  /* Make sure that there are no duplicated object IDs. TODO(rkn): we should
   * allow this case in the future. */
  CHECK(plasma_object_ids_distinct(num_object_ids, object_ids));
  plasma_request *req = plasma_alloc_request(num_object_ids, object_ids);
  LOG_DEBUG("Requesting fetch");
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_FETCH, req) >= 0);
  free(req);

  plasma_reply reply;
  int success;
  for (int received = 0; received < num_object_ids; ++received) {
    CHECK(plasma_receive_reply(conn->manager_conn, sizeof(reply), &reply) >= 0);
    CHECK(reply.num_object_ids == 1);
    success = reply.has_object;
    /* Update the correct index in is_fetched. */
    int i = 0;
    for (; i < num_object_ids; ++i) {
      if (object_ids_equal(object_ids[i], reply.object_ids[0]) &&
          !is_fetched[i]) {
        is_fetched[i] = success;
        break;
      }
    }
    CHECKM(i != num_object_ids,
           "Received an unexpected object ID from manager during fetch or the "
           "object ID was received multiple times.");
  }
}

int plasma_wait(plasma_connection *conn,
                int num_object_ids,
                object_id object_ids[],
                uint64_t timeout,
                int num_returns,
                object_id return_object_ids[]) {
  CHECK(conn->manager_conn >= 0);
  plasma_request *req = plasma_alloc_request(num_object_ids, object_ids);
  req->num_ready_objects = num_returns;
  req->timeout = timeout;
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_WAIT, req) >= 0);
  plasma_free_request(req);
  int64_t return_size = plasma_reply_size(num_returns);
  plasma_reply *reply = malloc(return_size);
  CHECK(plasma_receive_reply(conn->manager_conn, return_size, reply) >= 0);
  memcpy(return_object_ids, reply->object_ids, num_returns * sizeof(object_id));
  int num_objects_returned = reply->num_objects_returned;
  free(reply);
  return num_objects_returned;
}

int get_manager_fd(plasma_connection *conn) {
  return conn->manager_conn;
}

/** === ALTERNATE PLASMA CLIENT API ===

 * This API simplifies the previous one in two ways. First if factors out
 * object (re)construction from the Plasma Manager. Second, except for
 * plasma_wait_for_objects() all other functions are non-blocking.
 *
 * TODO:
 * - plasma_info() not implemented yet, but not needed at this point.
 * - assume new implementation of object_table_subscribe() which returns
 *   if object is in the Local Store (check with jpm).
 * - need to phase out old API and drope *1 from the names of the functions
 *   once the old ones are dropped.
 */

bool plasma_get_local(plasma_connection *conn,
                      object_id object_id,
                      object_buffer *object_buffer) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->store_conn, PLASMA_GET_LOCAL, &req) >= 0);

  plasma_reply reply;
  CHECK(plasma_receive_reply(conn->store_conn, sizeof(reply), &reply) >= 0);
  int fd = recv_fd(conn->store_conn);
  CHECKM(fd >= 0, "recv_fd not successful");

  if (reply.has_object) {
    plasma_object *object = &reply.object;
    object_buffer->data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                                         object->handle.mmap_size) +
                          object->data_offset;
    object_buffer->data_size = object->data_size;
    object_buffer->metadata = object_buffer->data + object->data_size;
    object_buffer->metadata_size = object->metadata_size;

    /* Increment the count of the number of instances of this object that this
     * client is using. A call to plasma_release is required to decrement this
     * count. */
    increment_object_count(conn, object_id, object->handle.store_fd);
    return true;
  }
  /* The object is either (1) not available in the local Plasma store, or (2) it
   * is not sealed yet. */
  return false;
}

int plasma_fetch_remote(plasma_connection *conn, object_id object_id) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_FETCH_REMOTE, &req) >=
        0);

  plasma_reply reply;
  int nbytes;

  nbytes =
      recv(conn->manager_conn, (uint8_t *) &reply, sizeof(reply), MSG_WAITALL);
  if (nbytes < 0) {
    LOG_ERROR("Error while waiting for manager response in fetch.");
  } else if (nbytes == 0) {
    LOG_ERROR("Error as Plasma Manager has closed the socket.");
  } else {
    CHECK(nbytes == sizeof(reply));
  }
  return reply.object_status;
}

int plasma_status(plasma_connection *conn, object_id object_id) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  plasma_request req = plasma_make_request(object_id);
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_STATUS, &req) >= 0);

  plasma_reply reply;

  int nbytes =
      recv(conn->manager_conn, (uint8_t *) &reply, sizeof(reply), MSG_WAITALL);
  if (nbytes < 0) {
    LOG_ERROR("Error while waiting for manager response in fetch.");
  } else if (nbytes == 0) {
    LOG_ERROR("Error as Plasma Manager has closed the socket.");
  } else {
    CHECK(nbytes == sizeof(reply));
  }

  return reply.object_status;
}

int plasma_wait_for_objects(plasma_connection *conn,
                            int num_object_requests,
                            object_request object_requests[],
                            int num_ready_objects,
                            uint64_t timeout_ms) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);
  CHECK(num_object_requests > 0);

  plasma_request *req =
      plasma_alloc_request2(num_object_requests, object_requests);
  req->num_ready_objects = num_ready_objects;
  req->timeout = timeout_ms;
  CHECK(plasma_send_request(conn->manager_conn, PLASMA_WAIT1, req) >= 0);
  free(req);

  plasma_reply *reply = plasma_alloc_reply2(num_object_requests);
  CHECK(plasma_receive_reply(conn->manager_conn,
                             plasma_reply_size2(num_object_requests),
                             reply) >= 0);
  int num_objects_ready = 0;
  for (int i = 0; i < num_object_requests; ++i) {
    int type, status;
    object_requests[i].object_id = reply->object_requests[i].object_id;
    type = reply->object_requests[i].type;
    object_requests[i].type = type;
    status = reply->object_requests[i].status;
    object_requests[i].status = status;

    if (type == PLASMA_QUERY_LOCAL) {
      if (status == PLASMA_OBJECT_LOCAL) {
        num_objects_ready += 1;
      }
    } else {
      CHECK(type == PLASMA_QUERY_ANYWHERE);
      if (status == PLASMA_OBJECT_LOCAL || status == PLASMA_OBJECT_REMOTE) {
        num_objects_ready += 1;
      }
    }
  }
  free(reply);
  return num_objects_ready;
}

/*
 *  TODO: maybe move the plasma_client_* functions in another file.
 *
 *  plasma_client_* represent functions implemented by client; so probably
 *  need to be in a different file.
 */

void plasma_client_get(plasma_connection *conn,
                       object_id object_id,
                       object_buffer *object_buffer) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  object_request request;
  request.object_id = object_id;

  while (true) {
    if (plasma_get_local(conn, object_id, object_buffer)) {
      /* Object is in the local Plasma Store, and it is sealed. */
      return;
    }

    switch (plasma_fetch_remote(conn, object_id)) {
    case PLASMA_OBJECT_LOCAL:
      /* Object has finished being transfered just after calling
       * plasma_get_local(), and it is now in the local Plasma Store. Loop again
       * to call plasma_get_local() and eventually return. */
      continue;
    case PLASMA_OBJECT_REMOTE:
      /* A fetch request has been already scheduled for object_id, so wait for
       * it to complete. */
      request.type = PLASMA_QUERY_LOCAL;
      break;
    case PLASMA_OBJECT_NONEXISTENT:
      /* Object doesn’t exist in the system so ask local scheduler to create it.
       */
      /* TODO: scheduler_create_object(object_id); */
      /* Wait for the object to be (re)constructed and sealed either in the
       * local Plasma Store or remotely. */
      request.type = PLASMA_QUERY_ANYWHERE;
      break;
    default:
      CHECKM(0, "Unrecognizable object status.")
    }

/*
 * Wait for object_id to (1) be transferred and sealed in the local
 * Plasma Store, if available remotely, or (2) be (re)constructued either
 * locally or remotely, if object_id didn't exist in the system.
 * - if timeout, next iteration will retry plasma_fetch() or
 *   scheduler_create_object()
 * - if request.status == PLASMA_OBJECT_LOCAL, next iteration
 *     will get object and return
 * - if request.status == PLASMA_OBJECT_REMOTE, next iteration
 *     will call plasma_fetch()
 * - if request.status == PLASMA_OBJECT_NONEXISTENT, next iteration
 *     will call scheduler_create_object()
 */
#define TIMEOUT_WAIT_MS 200
    plasma_wait_for_objects(conn, 1, &request, 1, TIMEOUT_WAIT_MS);
  }
}

int plasma_client_wait(plasma_connection *conn,
                       int num_object_ids,
                       object_id object_ids[],
                       uint64_t timeout,
                       int num_returns,
                       object_id return_object_ids[]) {
  CHECK(conn->manager_conn >= 0);
  CHECK(num_object_ids >= num_returns);

  object_request requests[num_object_ids];

  /* Initialize array of object requests. We only care for the objects to be
   * present in the system, not necessary in the local Plasma Store. Thus, we
   * set the request type to PLASMA_QUERY_ANYWHERE. */
  for (int i = 0; i < num_object_ids; ++i) {
    requests[i].object_id = object_ids[i];
    requests[i].type = PLASMA_QUERY_ANYWHERE;
  }

  /* Loop until we get num_returns objects stored in the system either in the
   * local Plasma Store or remotely. */
  uint64_t remaining_timeout = timeout;
  while (true) {
    struct timeval start, end;
    gettimeofday(&start, NULL);

    int n = plasma_wait_for_objects(conn, num_object_ids, requests, num_returns,
                                    MIN(remaining_timeout, TIMEOUT_WAIT_MS));

    gettimeofday(&end, NULL);
    float diff_ms = (end.tv_sec - start.tv_sec);
    diff_ms = (((diff_ms * 1000000.) + end.tv_usec) - (start.tv_usec)) / 1000.;
    remaining_timeout =
        (remaining_timeout >= diff_ms ? remaining_timeout - diff_ms : 0);

    if (n >= num_returns || remaining_timeout == 0) {
      /* Either (1) num_returns requests are satisfied or (2) timeout expired.
       * In both cases we return. */
      int idx_returns = 0;

      for (int i = 0; i < num_returns; ++i) {
        if (requests[i].status == PLASMA_OBJECT_LOCAL ||
            requests[i].status == PLASMA_OBJECT_REMOTE) {
          return_object_ids[idx_returns] = requests[i].object_id;
          idx_returns += 1;
        }
      }
      return idx_returns;
    }
    /* The timeout hasn't expired and we got less than num_returns in the
     * system. Trigger reconstruction of the missing objects. */
    for (int i = 0; i < num_returns; ++i) {
      if (requests[i].status == PLASMA_OBJECT_NONEXISTENT) {
        /* Object doesn’t exist in the system so ask local scheduler to create
         * object with ID requests[i].object_id. */
        /* TODO: scheduler_create_object(object_id); */
        printf("XXX Need to schedule object -- not implemented yet!\n");
        /* Subscribe to hear back when object_id is sealed. */
      }
    }
  }
}

void plasma_client_multiget(plasma_connection *conn,
                            int num_object_ids,
                            object_id object_ids[],
                            object_buffer object_buffers[]) {
  object_request requests[num_object_ids];

  /* Set all request types to PLASMA_OBJECT_LOCAL, as we want to get all objects
   * into the local Plasma Store. */
  for (int i = 0; i < num_object_ids; ++i) {
    requests[i].object_id = object_ids[i];
    requests[i].type = PLASMA_QUERY_LOCAL;
  }

  while (true) {
    int n;

    /* Wait to get all objects in the system. The reason we call
     * plasma_wait_for_objects() here instead of iterating over
     * plasma_client_get() is to increase concurrency as plasma_client_get() is
     * blocking. */
    n = plasma_wait_for_objects(conn, num_object_ids, requests, num_object_ids,
                                TIMEOUT_WAIT_MS);

    if (n == num_object_ids) {
      /* All objects are in the system either on the local or a remote Plasma
       * store, so we are done. */
      break;
    }

    for (int i = 0; i < num_object_ids; ++i) {
      if (requests[i].status == PLASMA_OBJECT_REMOTE) {
        plasma_fetch_remote(conn, requests[i].object_id);
      } else {
        if (requests[i].status == PLASMA_OBJECT_NONEXISTENT) {
          /* Object doesn’t exist so ask local scheduler to create it. */
          /* TODO: scheduler_create_object(requests[i].object_id); */
          printf("XXX Need to schedule object -- not implemented yet!\n");
        }
      }
    }
  }

  /* Now get the data for every object. */
  for (int i = 0; i < num_object_ids; ++i) {
    plasma_client_get(conn, object_ids[i], &object_buffers[i]);
  }
}

/**
 * TODO: maybe move object_requests_* functions in another file.
 * The object_request data structure is defined in plasma.h since
 * it is used by plasma_request and plasma_reply, but there is no
 * plasma.c file.
 */
void object_requests_copy(int num_object_requests,
                          object_request object_requests_dst[],
                          object_request object_requests_src[]) {
  for (int i = 0; i < num_object_requests; ++i) {
    object_requests_dst[i].object_id = object_requests_src[i].object_id;
    object_requests_dst[i].type = object_requests_src[i].type;
    object_requests_dst[i].status = object_requests_src[i].type;
  }
}

object_request *object_requests_get_object(object_id object_id,
                                           int num_object_requests,
                                           object_request object_requests[]) {
  for (int i = 0; i < num_object_requests; ++i) {
    if (object_ids_equal(object_requests[i].object_id, object_id)) {
      return &object_requests[i];
    }
  }
  return NULL;
}

void object_requests_set_status_all(int num_object_requests,
                                    object_request object_requests[],
                                    int status) {
  for (int i = 0; i < num_object_requests; ++i) {
    object_requests[i].status = status;
  }
}

void object_id_print(object_id obj_id) {
  for (int i = 0; i < sizeof(object_id); ++i) {
    printf("%u.", obj_id.id[i]);
    if (i < sizeof(object_id) - 1) {
      printf(".");
    }
  }
}

void object_requests_print(int num_object_requests,
                           object_request object_requests[]) {
  for (int i = 0; i < num_object_requests; ++i) {
    printf("[");
    for (int j = 0; j < sizeof(object_id); ++j) {
      object_id_print(object_requests[i].object_id);
    }
    printf(" | %d | %d], ", object_requests[i].type, object_requests[i].status);
  }
  printf("\n");
}
