#ifndef PLASMA_H
#define PLASMA_H

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h> /* pid_t */

#include <unordered_map>
#include <unordered_set>

#include "format/common_generated.h"

#include <inttypes.h>

#define HANDLE_SIGPIPE(s, fd_) \
  do { \
    Status _s = (s); \
    if (!_s.ok()) { \
      if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) { \
        ARROW_LOG(WARNING) << \
          "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when " \
          "sending a message to client on fd " << fd_ << ". " \
          "The client on the other end may have hung up."; \
      } else { \
        return _s; \
      } \
    } \
  } while (0);

/** Allocation granularity used in plasma for object allocation. */
#define BLOCK_SIZE 64

struct Client;

/**
 * Object request data structure. Used in the plasma_wait_for_objects()
 * argument.
 */
typedef struct {
  /** The ID of the requested object. If ID_NIL request any object. */
  ObjectID object_id;
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
} ObjectRequest;

/** Mapping from object IDs to type and status of the request. */
typedef std::unordered_map<ObjectID, ObjectRequest, UniqueIDHasher>
    ObjectRequestMap;

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
} PlasmaObject;

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
struct ObjectTableEntry {
  /** Object id of this object. */
  ObjectID object_id;
  /** Object info like size, creation time and owner. */
  ObjectInfoT info;
  /** Memory mapped file containing the object. */
  int fd;
  /** Size of the underlying map. */
  int64_t map_size;
  /** Offset from the base of the mmap. */
  ptrdiff_t offset;
  /** Pointer to the object data. Needed to free the object. */
  uint8_t *pointer;
  /** Set of clients currently using this object. */
  std::unordered_set<Client *> clients;
  /** The state of the object, e.g., whether it is open or sealed. */
  object_state state;
  /** The digest of the object. Used to see if two objects are the same. */
  unsigned char digest[kDigestSize];
};

/** The plasma store information that is exposed to the eviction policy. */
struct PlasmaStoreInfo {
  /** Objects that are in the Plasma store. */
  std::unordered_map<ObjectID,
                     std::unique_ptr<ObjectTableEntry>,
                     UniqueIDHasher>
      objects;
  /** The amount of memory (in bytes) that we allow to be allocated in the
   *  store. */
  int64_t memory_capacity;
};

/**
 * Get an entry from the object table and return NULL if the object_id
 * is not present.
 *
 * @param store_info The PlasmaStoreInfo that contains the object table.
 * @param object_id The object_id of the entry we are looking for.
 * @return The entry associated with the object_id or NULL if the object_id
 *         is not present.
 */
ObjectTableEntry *get_object_table_entry(PlasmaStoreInfo *store_info,
                                         ObjectID object_id);

/**
 * Print a warning if the status is less than zero. This should be used to check
 * the success of messages sent to plasma clients. We print a warning instead of
 * failing because the plasma clients are allowed to die. This is used to handle
 * situations where the store writes to a client file descriptor, and the client
 * may already have disconnected. If we have processed the disconnection and
 * closed the file descriptor, we should get a BAD FILE DESCRIPTOR error. If we
 * have not, then we should get a SIGPIPE. If we write to a TCP socket that
 * isn't connected yet, then we should get an ECONNRESET.
 *
 * @param status The status to check. If it is less less than zero, we will
 *        print a warning.
 * @param client_sock The client socket. This is just used to print some extra
 *        information.
 * @return The errno set.
 */
int warn_if_sigpipe(int status, int client_sock);

uint8_t *create_object_info_buffer(ObjectInfoT *object_info);

#endif /* PLASMA_H */
