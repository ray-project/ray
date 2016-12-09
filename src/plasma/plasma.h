#ifndef PLASMA_H
#define PLASMA_H

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h> /* pid_t */

#include "common.h"
#include "object_info.h"

#include "utarray.h"
#include "uthash.h"

/**
 * Object request data structure. Used in the plasma_wait_for_objects()
 * argument.
 */
typedef struct {
  /** The ID of the requested object. If ID_NIL request any object. */
  object_id object_id;
  /** Request associated to the object. It can take one of the following values:
   *  - PLASMA_QUERY_LOCAL: return if or when the object is available in the
   *    local Plasma Store.
   *  - PLASMA_QUERY_ANYWHERE: return if or when the object is available in
   *    the system (i.e., either in the local or a remote Plasma Store). */
  int type;
  /** Object status. Same as the status returned by plasma_status() function
   *  call. This is filled in by plasma_wait_for_objects1():
   *  - PLASMA_OBJECT_LOCAL: object is ready at the local Plasma Store.
   *  - PLASMA_OBJECT_REMOTE: object is ready at a remote Plasma Store.
   *  - PLASMA_OBJECT_NONEXISTENT: object does not exist in the system.
   *  - PLASMA_CLIENT_IN_TRANSFER, if the object is currently being scheduled
   *    for being transferred or it is transferring. */
  int status;
} object_request;

/* Handle to access memory mapped file and map it into client address space. */
typedef struct {
  /** The file descriptor of the memory mapped file in the store. It is used as
   *  a unique identifier of the file in the client to look up the corresponding
   *  file descriptor on the client's side. */
  int store_fd;
  /** The size in bytes of the memory mapped file. */
  int64_t mmap_size;
} object_handle;

typedef struct {
  /** Handle for memory mapped file the object is stored in. */
  object_handle handle;
  /** The offset in bytes in the memory mapped file of the data. */
  ptrdiff_t data_offset;
  /** The offset in bytes in the memory mapped file of the metadata. */
  ptrdiff_t metadata_offset;
  /** The size in bytes of the data. */
  int64_t data_size;
  /** The size in bytes of the metadata. */
  int64_t metadata_size;
} plasma_object;

typedef enum {
  /** Object was created but not sealed in the local Plasma Store. */
  PLASMA_CREATED = 1,
  /** Object is sealed and stored in the local Plasma Store. */
  PLASMA_SEALED
} object_state;

typedef enum {
  /** The object was not found. */
  OBJECT_NOT_FOUND = 0,
  /** The object was found. */
  OBJECT_FOUND = 1
} object_status;

typedef enum {
  /** Object is stored in the local Plasma Store. */
  PLASMA_OBJECT_LOCAL = 1,
  /** Object is stored on a remote Plasma store, and it is not stored on the
   *  local Plasma Store. */
  PLASMA_OBJECT_REMOTE,
  /** Object is currently transferred from a remote Plasma store the the local
   *  Plasma Store. */
  PLASMA_OBJECT_IN_TRANSFER,
  /** Object is not stored in the system. */
  PLASMA_OBJECT_NONEXISTENT
} object_status1;

typedef enum {
  /** Query for object in the local plasma store. */
  PLASMA_QUERY_LOCAL = 1,
  /** Query for object in the local plasma store or in a remote plasma store. */
  PLASMA_QUERY_ANYWHERE
} object_request_type;

enum plasma_message_type {
  /** Create a new object. */
  PLASMA_CREATE = 128,
  /** Get an object. */
  PLASMA_GET,
  /** Get an object stored at the local Plasma Store. */
  PLASMA_GET_LOCAL,
  /** Tell the store that the client no longer needs an object. */
  PLASMA_RELEASE,
  /** Check if an object is present. */
  PLASMA_CONTAINS,
  /** Seal an object. */
  PLASMA_SEAL,
  /** Delete an object. */
  PLASMA_DELETE,
  /** Evict objects from the store. */
  PLASMA_EVICT,
  /** Subscribe to notifications about sealed objects. */
  PLASMA_SUBSCRIBE,
  /** Request transfer to another store. */
  PLASMA_TRANSFER,
  /** Header for sending data. */
  PLASMA_DATA,
  /** Request a fetch of an object in another store. Non-blocking call. */
  PLASMA_FETCH_REMOTE,
  /** Request a fetch of an object in another store. Blocking call. */
  PLASMA_FETCH,
  /** Request a fetch of an object in another store. Non-blocking call. */
  PLASMA_FETCH2,
  /** Request status of an object, i.e., whether the object is stored in the
   *  local Plasma Store, in a remote Plasma Store, in transfer, or doesn't
   *  exist in the system. */
  PLASMA_STATUS,
  /** Wait until an object becomes available. */
  PLASMA_WAIT,
  /** Wait until an object becomes available. */
  PLASMA_WAIT1,
  /** Wait until an object becomes available. */
  PLASMA_WAIT2
};

typedef struct {
  /** The size of the object's data. */
  int64_t data_size;
  /** The size of the object's metadata. */
  int64_t metadata_size;
  /** The timeout of the request. */
  uint64_t timeout;
  /** The number of objects we are waiting for to be ready. */
  int num_ready_objects;
  /** In a transfer request, this is the IP address of the Plasma Manager to
   *  transfer the object to. */
  uint8_t addr[4];
  /** In a transfer request, this is the port of the Plasma Manager to transfer
   *  the object to. */
  int port;
  /** A number of bytes. This is used for eviction requests. */
  int64_t num_bytes;
  /** A digest describing the object. This is used for detecting
   *  nondeterministic tasks. */
  unsigned char digest[DIGEST_SIZE];
  /** The number of object IDs that will be included in this request. */
  int num_object_ids;
  /** The object requests that the request is about. */
  object_request object_requests[1];
} plasma_request;

typedef enum {
  /** There is no error. */
  PLASMA_REPLY_OK = 1,
  /** The object already exists. */
  PLASMA_OBJECT_ALREADY_EXISTS
} plasma_error;

typedef struct {
  /** The object that is returned with this reply. */
  plasma_object object;
  /** TODO: document this. */
  int object_status;
  /** This is used only to respond to requests of type
   *  PLASMA_CONTAINS or PLASMA_FETCH. It is 1 if the object is
   *  present and 0 otherwise. Used for plasma_contains and
   *  plasma_fetch. */
  int has_object;
  /** A number of bytes. This is used for replies to eviction requests. */
  int64_t num_bytes;
  /** Number of object IDs a wait is returning. */
  int num_objects_returned;
  /** The number of object IDs that will be included in this reply. */
  int num_object_ids;
  /** The object requests that this reply refers to. */
  object_request object_requests[1];
  /** Return error code. */
  plasma_error error_code;
} plasma_reply;

/** This type is used by the Plasma store. It is here because it is exposed to
 *  the eviction policy. */
typedef struct {
  /** Object id of this object. */
  object_id object_id;
  /** Object info like size, creation time and owner. */
  object_info info;
  /** Memory mapped file containing the object. */
  int fd;
  /** Size of the underlying map. */
  int64_t map_size;
  /** Offset from the base of the mmap. */
  ptrdiff_t offset;
  /** Handle for the uthash table. */
  UT_hash_handle handle;
  /** Pointer to the object data. Needed to free the object. */
  uint8_t *pointer;
  /** An array of the clients that are currently using this object. */
  UT_array *clients;
  /** The state of the object, e.g., whether it is open or sealed. */
  object_state state;
  /** The digest of the object. Used to see if two objects are the same. */
  unsigned char digest[DIGEST_SIZE];
} object_table_entry;

/** The plasma store information that is exposed to the eviction policy. */
typedef struct {
  /** Objects that are in the Plasma store. */
  object_table_entry *objects;
} plasma_store_info;

typedef struct {
  /** The ID of the object. */
  object_id obj_id;
  /** The size of the object. */
  int64_t object_size;
  /** The digest of the object used, used to see if two objects are the same. */
  unsigned char digest[DIGEST_SIZE];
} object_id_notification;

/**
 * Create a plasma request with one object ID on the stack.
 *
 * @param object_id The object ID to include in the request.
 * @return The plasma request.
 */
plasma_request plasma_make_request(object_id object_id);

/**
 * Create a plasma request with one or more object IDs on the heap. The caller
 * must free the returned plasma request pointer with plasma_free_request.
 *
 * @param num_object_ids The number of object IDs to include in the request.
 * @return A pointer to the newly created plasma request.
 */
plasma_request *plasma_alloc_request(int num_object_ids);

/**
 * Free a plasma request.
 *
 * @param request Pointer to the plasma request to be freed.
 * @return Void.
 */
void plasma_free_request(plasma_request *request);

/**
 * Size of a request in bytes.
 *
 * @param num_object_ids Number of object IDs in the request.
 * @return The size of the request in bytes.
 */
int64_t plasma_request_size(int num_object_ids);

/**
 * Create a plasma reply with one object ID on the stack.
 *
 * @param object_id The object ID to include in the reply.
 * @return The plasma reply.
 */
plasma_reply plasma_make_reply(object_id object_id);

/**
 * Create a plasma reply with one or more object IDs on the heap. The caller
 * must free the returned plasma reply pointer with plasma_free_reply.
 *
 * @param num_object_ids The number of object IDs to include in the reply.
 * @return A pointer to the newly created plasma reply.
 */
plasma_reply *plasma_alloc_reply(int num_object_ids);

/**
 * Free a plasma reply.
 *
 * @param request Pointer to the plasma reply to be freed.
 * @return Void.
 */
void plasma_free_reply(plasma_reply *request);

/**
 * Size of a reply in bytes.
 *
 * @param num_returns Number of object IDs returned with this reply.
 * @return The size of the reply in bytes.
 */
int64_t plasma_reply_size(int num_returns);

/**
 * Send a plasma reply.
 *
 * @param sock The file descriptor to use to send the request.
 * @param reply Address of the reply that is sent.
 * @return Returns a value >= 0 on success.
 */
int plasma_send_reply(int sock, plasma_reply *reply);

/**
 * Receive a plasma reply.
 *
 * @param sock The file descriptor to use to get the reply.
 * @param reply Address of the reply that is received.
 * @return Returns a value >= 0 on success.
 */
int plasma_receive_reply(int sock, int64_t receive_size, plasma_reply *reply);

/**
 * This is used to send a request to the Plasma Store or
 * the Plasma Manager.
 *
 * @param sock The file descriptor to use to send the request.
 * @param type The type of request.
 * @param req The address of the request to send.
 * @return Returns a value >= 0 on success.
 */
int plasma_send_request(int sock, int64_t type, plasma_request *request);

/**
 * Receive a plasma request. This allocates memory for the request which
 * needs to be freed by the user.
 *
 * @param sock The file descriptor to use to get the reply.
 * @param type Address where the type of the request is written to.
 * @param request Address at which the address of the allocated request is
 *        written.
 * @return Returns a value >= 0 on success.
 */
int plasma_receive_request(int sock, int64_t *type, plasma_request **request);

/**
 * Check if a collection of object IDs contains any duplicates.
 *
 * @param num_object_ids The number of object IDs.
 * @param object_ids[] The list of object IDs to check.
 * @return True if the object IDs are all distinct and false otherwise.
 */
bool plasma_object_ids_distinct(int num_object_ids, object_id object_ids[]);

#endif /* PLASMA_H */
