#ifndef PLASMA_H
#define PLASMA_H

#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include "common.h"

typedef struct {
  int64_t data_size;
  int64_t metadata_size;
  int64_t create_time;
  int64_t construct_duration;
} plasma_object_info;

/* Object was cerated but not sealed in the local Plasma store. */
#define PLASMA_OBJECT_CREATED 10
/* Object is sealed and stored on the local Plasma Store. */
#define PLASMA_OBJECT_SEALED 20
#define PLASMA_OBJECT_LOCAL  PLASMA_OBJECT_SEALED
/* Object is stored on a remote Plasma store, and it is not stored on the local Plasma Store */
#define PLASMA_OBJECT_REMOTE 30
/* Object is not stored in the system. */
#define PLASMA_OBJECT_DOES_NOT_EXIST 50
/* Object is currently transferred from a remote Plasma store the the local Plasma Store. */
#define PLASMA_OBJECT_TRANSFER 60
#define PLASMA_OBJECT_ANYWHERE 70

/**
 * Object rquest data structure. Used in the plasma_wait_for_objects() argument.
 */
typedef struct {
  /** ID of the requested object. If ID_NIL request any object */
  object_id object_id;
  /** Request associated to the object. It can take one of the following values:
   * - PLASMA_OBJECT_LOCAL: return if or when the object is available in the local Plasma Store.
   * - PLASMA_OBJECT_ANYWHWERE: return if or when the object is available in the
   *                            system (i.e., either in the local or a remote Plasma Store. */
  int type;
  /** Object status. Same as the status returned by plasma_status() function call.
   *  This is filled in by plasma_wait_for_objects1():
   * - PLASMA_OBJECT_LOCAL: object is ready at the local Plasma Store.
   * - PLASMA_OBJECT_REMOTE: object is ready at a remote Plasma Store.
   * - PLASMA_OBJECT_DOES_NOT_EXIST: object does not exist in the system.
   * - PLASMA_CLIENT_IN_TRANSFER, if the object is currently being scheduled for
   *                              being transferred or it is transferring. */
  int status;
} object_request;


/* Handle to access memory mapped file and map it into client address space */
typedef struct {
  /** The file descriptor of the memory mapped file in the store. It is used
   * as a unique identifier of the file in the client to look up the
   * corresponding file descriptor on the client's side. */
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

enum object_status { OBJECT_NOT_FOUND = 0, OBJECT_FOUND = 1 };

typedef enum { OPEN, SEALED } object_state;

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
  /** Subscribe to notifications about sealed objects. */
  PLASMA_SUBSCRIBE,
  /** Request transfer to another store. */
  PLASMA_TRANSFER,
  /** Header for sending data. */
  PLASMA_DATA,
  /** Request a fetch of an object in another store. Unblocking call */
  PLASMA_FETCH_REMOTE,
  /** Request a fetch of an object in another store. Blocking call. */
  PLASMA_FETCH,
  /** Request status of an object, i.e., whether the object is stored in
   * the local Plasma Store, in a remote Plasma Store, in transfer, or
   * doesn't exist in the system */
  PLASMA_STATUS,
  /** Wait until an object becomes available. */
  PLASMA_WAIT,
  /** Wait until an object becomes available. */
  PLASMA_WAIT1
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
  /** The number of object IDs that will be included in this request. */
  int num_object_ids;
  /** The IDs of the objects that the request is about. */
  union {
    object_id object_ids[1];
    object_request object_requests[1];
  };
} plasma_request;

typedef struct {
  /** The object that is returned with this reply. */
  plasma_object object;
  /** This is used only to respond to requests of type
   *  PLASMA_CONTAINS or PLASMA_FETCH. It is 1 if the object is
   *  present and 0 otherwise. Used for plasma_contains and
   *  plasma_fetch. */
   /* XXX */
  union {
    int object_status;
    int has_object;
  };
  /** Number of object IDs a wait is returning. */
  int num_objects_returned;
  /** The number of object IDs that will be included in this reply. */
  int num_object_ids;
  /** The IDs of the objects that this reply refers to. */
  union {
    object_id object_ids[1];
    object_request object_requests[1];
  };
  /** Return error code. */
#define PLASMA_REPLY_OK 0
#define PLASMA_REPLY_OBJECT_ALREADY_EXISTS 1
  int error_code;
} plasma_reply;

#endif /* PLASMA_H */
