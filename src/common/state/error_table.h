#ifndef ERROR_TABLE_H
#define ERROR_TABLE_H

#include "db.h"
#include "table.h"

/* Data structures that are needed for pushing error messages to Redis. These
 * data structures are only used internally. */

typedef struct {
  /* The ID of the driver that the error should be pushed to. */
  unique_id driver_id;
  /* The string used as an identifier for this error message. */
  unique_id error_id;
  /* The length of the error type. */
  int64_t error_type_len;
  /* The length of the error message. */
  int64_t error_message_len;
  /* The concatenation of the error type and error message. The size of this
   * array will actually be strlen(error_type) + strlen(error_message). The
   * error type will come first, followed by the error message. The substrings
   * in this array are not null-terminated. */
  uint8_t error_type_and_message[1];
} error_table_push_error_hmset_data;

/* Data that is needed to publish local scheduer heartbeats to the local
 * scheduler table. */
typedef struct {
  /* The ID used to identify the driver involved. */
  unique_id driver_id;
  /* The ID used to identify the error. */
  unique_id error_id;
} error_table_push_error_rpush_data;

/* Callback used internally for pushing an error to Redis. This is only used
 * internally. */
typedef void (*error_table_push_error_hmset_callback)(db_handle *db,
                                                      unique_id driver_id,
                                                      unique_id error_id);

/**
 * Send an error message to a driver process. This will be printed in the
 * background for the driver process.
 *
 * @param db_handle Database handle.
 * @param driver_id The ID of the driver or job process.
 * @param error_type A null-terminated string indicating the type of error being
 *        pushed.
 * @param error_message A null-terminated string indicating the error message to
 *        send to the driver.
 * @param retry Information about retrying the request to the database.
 * @return Void.
 */
void push_error(db_handle *db_handle,
                unique_id driver_id,
                char *error_type,
                char *error_message,
                retry_info *retry);

#endif /* ERROR_TABLE_H */
