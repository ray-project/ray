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
   *  - ObjectStatus_Local: object is ready at the local Plasma Store.
   *  - ObjectStatus_Remote: object is ready at a remote Plasma Store.
   *  - ObjectStatus_Nonexistent: object does not exist in the system.
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
  /** Query for object in the local plasma store. */
  PLASMA_QUERY_LOCAL = 1,
  /** Query for object in the local plasma store or in a remote plasma store. */
  PLASMA_QUERY_ANYWHERE
} object_request_type;

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
  /** The amount of memory (in bytes) that we allow to be allocated in the
   *  store. */
  int64_t memory_capacity;
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
 * Check if a collection of object IDs contains any duplicates.
 *
 * @param num_object_ids The number of object IDs.
 * @param object_ids[] The list of object IDs to check.
 * @return True if the object IDs are all distinct and false otherwise.
 */
bool plasma_object_ids_distinct(int num_object_ids, object_id object_ids[]);

/**
 * Print a warning if the status is less than zero. This should be used to check
 * the success of messages sent to plasma clients. We print a warning instead of
 * failing because the plasma clients are allowed to die. This is used to handle
 * situations where the store writes to a client file descriptor, and the client
 * may already have disconnected. If we have processed the disconnection and
 * closed the file descriptor, we should get a BAD FILE DESCRIPTOR error. If we
 * have not, then we should get a SIGPIPE.
 *
 * @param status The status to check. If it is less less than zero, we will
 *        print a warning.
 * @param client_sock The client socket. This is just used to print some extra
 *        information.
 * @return Void.
 */
void warn_if_sigpipe(int status, int client_sock);

#endif /* PLASMA_H */
