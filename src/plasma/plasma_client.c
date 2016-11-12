/* PLASMA CLIENT: Client library for using the plasma store and manager */

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <netinet/in.h>
#include <netdb.h>

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
  /** Table of dlmalloc buffer files that have been memory mapped so far. This
   *  is a hash table mapping a file descriptor to a struct containing the
   *  address of the corresponding memory-mapped file. */
  client_mmap_table_entry *mmap_table;
  /** A hash table of the object IDs that are currently being used by this
   * client. */
  object_in_use_entry *objects_in_use;
  /** Object IDs of the last few release calls. This is used to delay
   *  releasing objects to see if they can be reused by subsequent tasks so we
   *  do not unneccessarily invalidate cpu caches. TODO(pcm): replace this with
   *  a proper lru cache of size sizeof(L3 cache). */
  UT_ringbuffer *release_history;
  /** Configuration options for the plasma client. */
  plasma_client_config config;
};

int plasma_request_size(int num_object_ids) {
  int object_ids_size = (num_object_ids - 1) * sizeof(object_id);
  return sizeof(plasma_request) + object_ids_size;
}

void plasma_send_request(int fd, int type, plasma_request *req) {
  int req_size = plasma_request_size(req->num_object_ids);
  int error = write_message(fd, type, req_size, (uint8_t *) req);
  /* TODO(swang): Actually handle the write error. */
  CHECK(!error);
}

plasma_request make_plasma_request(object_id object_id) {
  plasma_request req = {.num_object_ids = 1, .object_ids = {object_id}};
  return req;
}

plasma_request *make_plasma_multiple_request(int num_object_ids,
                                             object_id object_ids[]) {
  int req_size = plasma_request_size(num_object_ids);
  plasma_request *req = malloc(req_size);
  req->num_object_ids = num_object_ids;
  memcpy(&req->object_ids, object_ids, num_object_ids * sizeof(object_id));
  return req;
}

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

void plasma_create(plasma_connection *conn,
                   object_id object_id,
                   int64_t data_size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data) {
  LOG_DEBUG("called plasma_create on conn %d with size %" PRId64
            " and metadata size "
            "%" PRId64,
            conn->store_conn, data_size, metadata_size);
  plasma_request req = make_plasma_request(object_id);
  req.data_size = data_size;
  req.metadata_size = metadata_size;
  plasma_send_request(conn->store_conn, PLASMA_CREATE, &req);
  plasma_reply reply;
  int fd = recv_fd(conn->store_conn, (char *) &reply, sizeof(plasma_reply));
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
}

/* This method is used to get both the data and the metadata. */
void plasma_get(plasma_connection *conn,
                object_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata) {
  plasma_request req = make_plasma_request(object_id);
  plasma_send_request(conn->store_conn, PLASMA_GET, &req);
  plasma_reply reply;
  int fd = recv_fd(conn->store_conn, (char *) &reply, sizeof(plasma_reply));
  CHECKM(fd != -1, "recv not successful");
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
    plasma_request req = make_plasma_request(object_id);
    plasma_send_request(conn->store_conn, PLASMA_RELEASE, &req);
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
  plasma_request req = make_plasma_request(object_id);
  plasma_send_request(conn->store_conn, PLASMA_CONTAINS, &req);
  plasma_reply reply;
  int r = read(conn->store_conn, &reply, sizeof(plasma_reply));
  CHECKM(r != -1, "read error");
  CHECKM(r != 0, "connection disconnected");
  *has_object = reply.has_object;
}

void plasma_seal(plasma_connection *conn, object_id object_id) {
  plasma_request req = make_plasma_request(object_id);
  plasma_send_request(conn->store_conn, PLASMA_SEAL, &req);
  if (conn->manager_conn >= 0) {
    plasma_send_request(conn->manager_conn, PLASMA_SEAL, &req);
  }
}

void plasma_delete(plasma_connection *conn, object_id object_id) {
  plasma_request req = make_plasma_request(object_id);
  plasma_send_request(conn->store_conn, PLASMA_DELETE, &req);
}

int64_t plasma_evict(plasma_connection *conn, int64_t num_bytes) {
  /* Send a request to the store to evict objects. */
  plasma_request req = {.num_bytes = num_bytes};
  plasma_send_request(conn->store_conn, PLASMA_EVICT, &req);
  /* Wait for a response with the number of bytes actually evicted. */
  plasma_reply reply;
  int r = read(conn->store_conn, &reply, sizeof(plasma_reply));
  CHECKM(r != -1, "read error");
  CHECKM(r != 0, "connection disconnected");
  return reply.num_bytes;
}

int plasma_subscribe(plasma_connection *conn) {
  int fd[2];
  /* Create a non-blocking socket pair. This will only be used to send
   * notifications from the Plasma store to the client. */
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  /* Make the socket non-blocking. */
  int flags = fcntl(fd[1], F_GETFL, 0);
  CHECK(fcntl(fd[1], F_SETFL, flags | O_NONBLOCK) == 0);
  /* Tell the Plasma store about the subscription. */
  plasma_request req = {};
  plasma_send_request(conn->store_conn, PLASMA_SUBSCRIBE, &req);
  /* Send the file descriptor that the Plasma store should use to push
   * notifications about sealed objects to this client. We include a one byte
   * message because otherwise it seems to hang on Linux. */
  char dummy = '\0';
  send_fd(conn->store_conn, fd[1], &dummy, 1);
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
  close(conn->store_conn);
  if (conn->manager_conn >= 0) {
    close(conn->manager_conn);
  }
  object_id *id = NULL;
  while ((id = (object_id *) utringbuffer_next(conn->release_history, id))) {
    plasma_perform_release(conn, *id);
  }
  utringbuffer_free(conn->release_history);
  free(conn);
}

#define h_addr h_addr_list[0]

int plasma_manager_try_connect(const char *ip_addr, int port) {
  int fd = socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_ERROR("could not create socket");
    return -1;
  }

  struct hostent *manager = gethostbyname(ip_addr); /* TODO(pcm): cache this */
  if (!manager) {
    LOG_ERROR("plasma manager %s not found", ip_addr);
    return -1;
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  memcpy(&addr.sin_addr.s_addr, manager->h_addr, manager->h_length);
  addr.sin_port = htons(port);

  int r = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
  if (r < 0) {
    LOG_ERROR(
        "could not establish connection to manager with id %s:%d (may have run "
        "out of ports)",
        &ip_addr[0], port);
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
  return fd;
}

void plasma_transfer(plasma_connection *conn,
                     const char *addr,
                     int port,
                     object_id object_id) {
  plasma_request req = make_plasma_request(object_id);
  req.port = port;
  char *end = NULL;
  for (int i = 0; i < 4; ++i) {
    req.addr[i] = strtol(end ? end : addr, &end, 10);
    /* skip the '.' */
    end += 1;
  }
  plasma_send_request(conn->manager_conn, PLASMA_TRANSFER, &req);
}

void plasma_fetch(plasma_connection *conn,
                  int num_object_ids,
                  object_id object_ids[],
                  int is_fetched[]) {
  CHECK(conn->manager_conn >= 0);
  plasma_request *req =
      make_plasma_multiple_request(num_object_ids, object_ids);
  LOG_DEBUG("Requesting fetch");
  plasma_send_request(conn->manager_conn, PLASMA_FETCH, req);
  free(req);

  plasma_reply reply;
  int nbytes, success;
  for (int received = 0; received < num_object_ids; ++received) {
    nbytes = recv(conn->manager_conn, (uint8_t *) &reply, sizeof(reply),
                  MSG_WAITALL);
    if (nbytes < 0) {
      LOG_ERROR("Error while waiting for manager response in fetch");
      success = 0;
    } else if (nbytes == 0) {
      success = 0;
    } else {
      CHECK(nbytes == sizeof(reply));
      success = reply.has_object;
    }
    CHECK(reply.num_object_ids == 1);
    /* Update the correct index in is_fetched. */
    int i = 0;
    for (; i < num_object_ids; i++) {
      if (memcmp(&object_ids[i], &reply.object_ids[0], sizeof(object_id)) ==
          0) {
        /* Check that this isn't a duplicate response. */
        CHECK(!is_fetched[i]);
        is_fetched[i] = success;
        break;
      }
    }
    CHECKM(i != num_object_ids,
           "Received unexpected object ID from manager during fetch.");
  }
}

int plasma_wait(plasma_connection *conn,
                int num_object_ids,
                object_id object_ids[],
                uint64_t timeout,
                int num_returns,
                object_id return_object_ids[]) {
  CHECK(conn->manager_conn >= 0);
  plasma_request *req =
      make_plasma_multiple_request(num_object_ids, object_ids);
  req->num_returns = num_returns;
  req->timeout = timeout;
  plasma_send_request(conn->manager_conn, PLASMA_WAIT, req);
  free(req);
  int64_t return_size =
      sizeof(plasma_reply) + (num_returns - 1) * sizeof(object_id);
  plasma_reply *reply = malloc(return_size);
  int nbytes = recv(conn->manager_conn, (uint8_t *) reply, return_size, 0);
  CHECK(nbytes == return_size);
  memcpy(return_object_ids, reply->object_ids, num_returns * sizeof(object_id));
  int num_objects_returned = reply->num_objects_returned;
  free(reply);
  return num_objects_returned;
}

int get_manager_fd(plasma_connection *conn) {
  return conn->manager_conn;
}
