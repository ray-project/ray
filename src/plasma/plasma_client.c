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
#include "plasma_protocol.h"
#include "plasma_client.h"
#include "fling.h"
#include "uthash.h"
#include "utringbuffer.h"
#include "sha256.h"

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

#define XXH64_DEFAULT_SEED 0

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
  /** A count of the number of times this client has called plasma_create or
   *  plasma_get on this object ID minus the number of calls to plasma_release.
   *  When this count reaches zero, we remove the entry from the objects_in_use
   *  and decrement a count in the relevant client_mmap_table_entry. */
  int count;
  /** Cached information to read the object. */
  plasma_object object;
  /** A flag representing whether the object has been sealed. */
  bool is_sealed;
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
  /** Buffer that holds memory for serializing plasma protocol messages. */
  protocol_builder *builder;
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

/* Get a pointer to a file that we know has been memory mapped in this client
 * process before. */
uint8_t *lookup_mmapped_file(plasma_connection *conn, int store_fd_val) {
  client_mmap_table_entry *entry;
  HASH_FIND_INT(conn->mmap_table, &store_fd_val, entry);
  CHECK(entry);
  return entry->pointer;
}

void increment_object_count(plasma_connection *conn,
                            object_id object_id,
                            plasma_object *object,
                            bool is_sealed) {
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
    object_entry->object = *object;
    object_entry->count = 0;
    object_entry->is_sealed = is_sealed;
    HASH_ADD(hh, conn->objects_in_use, object_id, sizeof(object_id),
             object_entry);
    /* Increment the count of the number of objects in the memory-mapped file
     * that are being used. The corresponding decrement should happen in
     * plasma_release. */
    client_mmap_table_entry *entry;
    HASH_FIND_INT(conn->mmap_table, &object->handle.store_fd, entry);
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
                   object_id obj_id,
                   int64_t data_size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data) {
  LOG_DEBUG("called plasma_create on conn %d with size %" PRId64
            " and metadata size %" PRId64,
            conn->store_conn, data_size, metadata_size);
  CHECK(plasma_send_CreateRequest(conn->store_conn, conn->builder, obj_id,
                                  data_size, metadata_size) >= 0);
  uint8_t *reply_data =
      plasma_receive(conn->store_conn, MessageType_PlasmaCreateReply);
  int error;
  object_id id;
  plasma_object object;
  plasma_read_CreateReply(reply_data, &id, &object, &error);
  free(reply_data);
  if (error == PlasmaError_ObjectExists) {
    LOG_DEBUG("returned from plasma_create with error %d", error);
    return false;
  }
  int fd = recv_fd(conn->store_conn);
  CHECKM(fd >= 0, "recv not successful");
  CHECK(object.data_size == data_size);
  CHECK(object.metadata_size == metadata_size);
  /* The metadata should come right after the data. */
  CHECK(object.metadata_offset == object.data_offset + data_size);
  *data = lookup_or_mmap(conn, fd, object.handle.store_fd,
                         object.handle.mmap_size) +
          object.data_offset;
  /* If plasma_create is being called from a transfer, then we will not copy the
   * metadata here. The metadata will be written along with the data streamed
   * from the transfer. */
  if (metadata != NULL) {
    /* Copy the metadata to the buffer. */
    memcpy(*data + object.data_size, metadata, metadata_size);
  }
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. Cache the reference to the object. */
  increment_object_count(conn, obj_id, &object, false);
  return true;
}

/* This method is used to get both the data and the metadata. */
void plasma_get(plasma_connection *conn,
                object_id obj_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata) {
  /* Check if we already have a reference to the object. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &obj_id, sizeof(object_id), object_entry);
  plasma_object object_data;
  plasma_object *object;
  if (object_entry) {
    /* If we have already have a reference to the object, use it to get the
     * data pointer.
     * NOTE: If the object is still unsealed, we will deadlock, since we must
     * have been the one who created it. */
    CHECKM(object_entry->is_sealed,
           "Plasma client called get on an unsealed object that it created");
    object = &object_entry->object;
    *data = lookup_mmapped_file(conn, object->handle.store_fd);
  } else {
    /* Else, request a reference to the object data from the plasma store. */
    CHECK(plasma_send_GetRequest(conn->store_conn, conn->builder, &obj_id, 1) >=
          0);
    uint8_t *reply_data =
        plasma_receive(conn->store_conn, MessageType_PlasmaGetReply);
    object_id received_obj_id;
    plasma_read_GetReply(reply_data, &received_obj_id, &object_data, 1);
    free(reply_data);
    DCHECK(memcmp(&received_obj_id, &obj_id, sizeof(obj_id)) == 0);
    int fd = recv_fd(conn->store_conn);
    CHECK(fd >= 0);
    object = &object_data;
    *data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                           object->handle.mmap_size);
  }
  /* Finish filling out the return values. */
  *data = *data + object->data_offset;
  *size = object->data_size;
  /* If requested, return the metadata as well. */
  if (metadata != NULL) {
    *metadata = *data + object->data_size;
    *metadata_size = object->metadata_size;
  }
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. Cache the reference to the object. */
  increment_object_count(conn, obj_id, object, true);
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
    int fd = object_entry->object.handle.store_fd;
    HASH_FIND_INT(conn->mmap_table, &fd, entry);
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
    CHECK(plasma_send_ReleaseRequest(conn->store_conn, conn->builder,
                                     object_id) >= 0);
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
                     object_id obj_id,
                     int *has_object) {
  /* Check if we already have a reference to the object. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &obj_id, sizeof(obj_id), object_entry);
  if (object_entry) {
    *has_object = 1;
  } else {
    /* If we don't already have a reference to the object, check with the store
     * to see if we have the object. */
    plasma_send_ContainsRequest(conn->store_conn, conn->builder, obj_id);
    uint8_t *reply_data =
        plasma_receive(conn->store_conn, MessageType_PlasmaContainsReply);
    object_id object_id2;
    plasma_read_ContainsReply(reply_data, &object_id2, has_object);
    free(reply_data);
  }
}

bool plasma_compute_object_hash(plasma_connection *conn,
                                object_id obj_id,
                                unsigned char *digest) {
  /* If we don't have the object, return an empty digest. */
  int has_object;
  plasma_contains(conn, obj_id, &has_object);
  if (!has_object) {
    return false;
  }
  /* Get the plasma object data. */
  int64_t size;
  uint8_t *data;
  int64_t metadata_size;
  uint8_t *metadata;
  plasma_get(conn, obj_id, &size, &data, &metadata_size, &metadata);
  /* Compute the hash. */
  XXH64_state_t hash_state;
  XXH64_reset(&hash_state, XXH64_DEFAULT_SEED);
  XXH64_update(&hash_state, (unsigned char *) data, size);
  XXH64_update(&hash_state, (unsigned char *) metadata, metadata_size);
  uint64_t hash = XXH64_digest(&hash_state);
  DCHECK(DIGEST_SIZE >= sizeof(uint64_t));
  memcpy(digest, &hash, DIGEST_SIZE);
  /* Release the plasma object. */
  plasma_release(conn, obj_id);
  return true;
}

void plasma_seal(plasma_connection *conn, object_id object_id) {
  /* Make sure this client has a reference to the object before sending the
   * request to Plasma. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &object_id, sizeof(object_id),
            object_entry);
  CHECKM(object_entry != NULL,
         "Plasma client called seal an object without a reference to it");
  CHECKM(!object_entry->is_sealed,
         "Plasma client called seal an already sealed object");
  object_entry->is_sealed = true;
  /* Send the seal request to Plasma. */
  unsigned char digest[DIGEST_SIZE];
  CHECK(plasma_compute_object_hash(conn, object_id, &digest[0]));
  CHECK(plasma_send_SealRequest(conn->store_conn, conn->builder, object_id,
                                &digest[0]) >= 0);
}

void plasma_delete(plasma_connection *conn, object_id object_id) {
  /* TODO(rkn): In the future, we can use this method to give hints to the
   * eviction policy about when an object will no longer be needed. */
}

int64_t plasma_evict(plasma_connection *conn, int64_t num_bytes) {
  /* Send a request to the store to evict objects. */
  CHECK(plasma_send_EvictRequest(conn->store_conn, conn->builder, num_bytes) >=
        0);
  /* Wait for a response with the number of bytes actually evicted. */
  int64_t type;
  int64_t length;
  uint8_t *reply_data;
  read_message(conn->store_conn, &type, &length, &reply_data);
  int64_t num_bytes_evicted;
  plasma_read_EvictReply(reply_data, &num_bytes_evicted);
  free(reply_data);
  return num_bytes_evicted;
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
  CHECK(plasma_send_SubscribeRequest(conn->store_conn, conn->builder) >= 0);
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
  result->builder = make_protocol_builder();
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
  free_protocol_builder(conn->builder);
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
                     const char *address,
                     int port,
                     object_id object_id) {
  CHECK(plasma_send_DataRequest(conn->manager_conn, conn->builder, object_id,
                                address, port) >= 0);
}

void plasma_fetch(plasma_connection *conn,
                  int num_object_ids,
                  object_id object_ids[]) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);
  CHECK(plasma_send_FetchRequest(conn->manager_conn, conn->builder, object_ids,
                                 num_object_ids) >= 0);
}

int get_manager_fd(plasma_connection *conn) {
  return conn->manager_conn;
}

bool plasma_get_local(plasma_connection *conn,
                      object_id obj_id,
                      object_buffer *object_buffer) {
  CHECK(conn != NULL);
  /* Check if we already have a reference to the object. */
  object_in_use_entry *object_entry;
  HASH_FIND(hh, conn->objects_in_use, &obj_id, sizeof(obj_id), object_entry);
  plasma_object *object;
  if (object_entry) {
    /* If we have already have a reference to the object, use it to get the
     * data pointer. */
    if (!object_entry->is_sealed) {
      /* The object is in our local store, but it hasn't been sealed yet. */
      return false;
    }
    object = &object_entry->object;
    object_buffer->data = lookup_mmapped_file(conn, object->handle.store_fd);
  } else {
    /* Else, request a reference to the object data from the plasma store. */
    CHECK(plasma_send_GetLocalRequest(conn->store_conn, conn->builder, &obj_id,
                                      1) >= 0);
    uint8_t *reply_data =
        plasma_receive(conn->store_conn, MessageType_PlasmaGetLocalReply);
    plasma_object object_data;
    int has_object;
    plasma_read_GetLocalReply(reply_data, &obj_id, &object_data, &has_object,
                              1);
    free(reply_data);

    if (!has_object) {
      /* The object is not in our local store. */
      return false;
    }

    int fd = recv_fd(conn->store_conn);
    CHECKM(fd >= 0, "recv_fd not successful");
    object = &object_data;
    object_buffer->data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                                         object->handle.mmap_size);
  }
  /* Finish filling out the return values. */
  object_buffer->data += object->data_offset;
  object_buffer->data_size = object->data_size;
  object_buffer->metadata = object_buffer->data + object->data_size;
  object_buffer->metadata_size = object->metadata_size;
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. Cache the reference to the object. */
  increment_object_count(conn, obj_id, object, true);
  return true;
}

int plasma_status(plasma_connection *conn, object_id object_id) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  plasma_send_StatusRequest(conn->manager_conn, conn->builder, &object_id, 1);
  uint8_t *reply_data =
      plasma_receive(conn->manager_conn, MessageType_PlasmaStatusReply);
  int object_status;
  plasma_read_StatusReply(reply_data, &object_id, &object_status, 1);
  free(reply_data);
  return object_status;
}

int plasma_wait(plasma_connection *conn,
                int num_object_requests,
                object_request object_requests[],
                int num_ready_objects,
                uint64_t timeout_ms) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);
  CHECK(num_object_requests > 0);
  CHECK(num_ready_objects > 0);
  CHECK(num_ready_objects <= num_object_requests);

  for (int i = 0; i < num_object_requests; ++i) {
    CHECK(object_requests[i].type == PLASMA_QUERY_LOCAL ||
          object_requests[i].type == PLASMA_QUERY_ANYWHERE);
  }

  CHECK(plasma_send_WaitRequest(conn->manager_conn, conn->builder,
                                object_requests, num_object_requests,
                                num_ready_objects, timeout_ms) >= 0);
  uint8_t *reply_data =
      plasma_receive(conn->manager_conn, MessageType_PlasmaWaitReply);
  plasma_read_WaitReply(reply_data, object_requests, &num_ready_objects);
  free(reply_data);

  int num_objects_ready = 0;
  for (int i = 0; i < num_object_requests; ++i) {
    int type = object_requests[i].type;
    int status = object_requests[i].status;
    switch (type) {
    case PLASMA_QUERY_LOCAL:
      if (status == ObjectStatus_Local) {
        num_objects_ready += 1;
      }
      break;
    case PLASMA_QUERY_ANYWHERE:
      if (status == ObjectStatus_Local || status == ObjectStatus_Remote) {
        num_objects_ready += 1;
      } else {
        CHECK(status == ObjectStatus_Nonexistent);
      }
      break;
    default:
      LOG_FATAL("This code should be unreachable.");
    }
  }
  return num_objects_ready;
}

/*
 *  TODO: maybe move the plasma_client_* functions in another file.
 *
 *  plasma_client_* represent functions implemented by client; so probably
 *  need to be in a different file.
 */

void plasma_client_get(plasma_connection *conn,
                       object_id obj_id,
                       object_buffer *object_buffer) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  object_request request;
  request.object_id = obj_id;

  while (true) {
    if (plasma_get_local(conn, obj_id, object_buffer)) {
      /* Object is in the local Plasma Store, and it is sealed. */
      return;
    }

    object_id object_ids[1] = {obj_id};
    plasma_fetch(conn, 1, object_ids);
    switch (plasma_status(conn, obj_id)) {
    case ObjectStatus_Local:
      /* Object has finished being transfered just after calling
       * plasma_get_local(), and it is now in the local Plasma Store. Loop again
       * to call plasma_get_local() and eventually return. */
      continue;
    case ObjectStatus_Remote:
      /* A fetch request has been already scheduled for obj_id, so wait for
       * it to complete. */
      request.type = PLASMA_QUERY_LOCAL;
      break;
    case ObjectStatus_Nonexistent:
      /* Object doesn’t exist in the system so ask local scheduler to create it.
       */
      /* TODO: scheduler_create_object(obj_id); */
      /* Wait for the object to be (re)constructed and sealed either in the
       * local Plasma Store or remotely. */
      request.type = PLASMA_QUERY_ANYWHERE;
      break;
    default:
      CHECKM(0, "Unrecognizable object status.")
    }

/*
 * Wait for obj_id to (1) be transferred and sealed in the local
 * Plasma Store, if available remotely, or (2) be (re)constructued either
 * locally or remotely, if obj_id didn't exist in the system.
 * - if timeout, next iteration will retry plasma_fetch() or
 *   scheduler_create_object()
 * - if request.status == ObjectStatus_Local, next iteration
 *     will get object and return
 * - if request.status == ObjectStatus_Remote, next iteration
 *     will call plasma_fetch()
 * - if request.status == ObjectStatus_Nonexistent, next iteration
 *     will call scheduler_create_object()
 */
#define TIMEOUT_WAIT_MS 200
    plasma_wait(conn, 1, &request, 1, TIMEOUT_WAIT_MS);
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

    int n = plasma_wait(conn, num_object_ids, requests, num_returns,
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
        if (requests[i].status == ObjectStatus_Local ||
            requests[i].status == ObjectStatus_Remote) {
          return_object_ids[idx_returns] = requests[i].object_id;
          idx_returns += 1;
        }
      }
      return idx_returns;
    }
    /* The timeout hasn't expired and we got less than num_returns in the
     * system. Trigger reconstruction of the missing objects. */
    for (int i = 0; i < num_returns; ++i) {
      if (requests[i].status == ObjectStatus_Nonexistent) {
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

  /* Set all request types to ObjectStatus_Local, as we want to get all objects
   * into the local Plasma Store. */
  for (int i = 0; i < num_object_ids; ++i) {
    requests[i].object_id = object_ids[i];
    requests[i].type = PLASMA_QUERY_LOCAL;
  }

  while (true) {
    int n;

    /* Issue a fetch command so the object IDs end up locally. */
    plasma_fetch(conn, num_object_ids, object_ids);

    /* Wait to get all objects in the system. The reason we call plasma_wait()
     * here instead of iterating over plasma_client_get() is to increase
     * concurrency as plasma_client_get() is blocking. */
    n = plasma_wait(conn, num_object_ids, requests, num_object_ids,
                    TIMEOUT_WAIT_MS);

    if (n == num_object_ids) {
      /* All objects are in the system either on the local or a remote Plasma
       * store, so we are done. */
      break;
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
