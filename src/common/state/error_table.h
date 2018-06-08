#ifndef ERROR_TABLE_H
#define ERROR_TABLE_H

#include "db.h"
#include "table.h"

/// An ErrorIndex may be used as an index into error_types.
enum class ErrorIndex : int32_t {
  /// An object was added with a different hash from the existing one.
  OBJECT_HASH_MISMATCH = 0,
  /// An object that was created through a ray.put is lost.
  PUT_RECONSTRUCTION,
  /// A worker died or was killed while executing a task.
  WORKER_DIED,
  /// An actor hasn't been created for a while.
  ACTOR_NOT_CREATED,
  /// The total number of error types.
  MAX
};

/// Data that is needed to push an error.
typedef struct {
  /// The ID of the driver to push the error to.
  DBClientID driver_id;
  /// An index into the error_types array indicating the type of the error.
  ErrorIndex error_type;
  /// The key to use for the error message in Redis.
  UniqueID error_key;
  /// The length of the error message.
  int64_t size;
  /// The error message.
  uint8_t error_message[0];
} ErrorInfo;

extern const char *error_types[];

/// Push an error to the given Python driver.
///
/// \param db_handle Database handle.
/// \param driver_id The ID of the Python driver to push the error to.
/// \param error_type An index specifying the type of the error. This should
/// be a value from the ErrorIndex enum.
/// \param error_message The error message to print.
/// \return Void.
void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                ErrorIndex error_type,
                const std::string &error_message);

#endif
