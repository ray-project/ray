#ifndef ERROR_TABLE_H
#define ERROR_TABLE_H

#include "db.h"
#include "table.h"

/// Data that is needed to push an error.
typedef struct {
  /// The ID of the driver to push the error to.
  DBClientID driver_id;
  /// An index into the error_types array indicating the type of the error.
  int error_type;
  /// The key to use for the error message in Redis.
  UniqueID error_key;
  /// The length of the error message.
  int64_t size;
  /// The error message.
  uint8_t error_message[0];
} ErrorInfo;

/// An error_index may be used as an index into error_types.
typedef enum {
  /// An object was added with a different hash from the existing one.
  OBJECT_HASH_MISMATCH_ERROR_INDEX = 0,
  /// An object that was created through a ray.put is lost.
  PUT_RECONSTRUCTION_ERROR_INDEX,
  /// A worker died or was killed while executing a task.
  WORKER_DIED_ERROR_INDEX,
  /// An actor hasn't been created for a while.
  ACTOR_NOT_CREATED_ERROR_INDEX,
  /// The total number of error types.
  MAX_ERROR_INDEX
} error_index;

extern const char *error_types[];

/// Push an error to the given Python driver.
///
/// \param db_handle Database handle.
/// \param driver_id The ID of the Python driver to push the error to.
/// \param error_type An index specifying the type of the error. This should
/// be a value from the error_index enum.
/// \param error_message The error message to print.
/// \return Void.
void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                int error_type,
                const std::string &error_message);

#endif
