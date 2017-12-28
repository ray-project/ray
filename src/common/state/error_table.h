#ifndef ERROR_TABLE_H
#define ERROR_TABLE_H

#include "db.h"
#include "table.h"

typedef struct {
  DBClientID driver_id;
  unsigned char error_key[20];
  int error_index;
  size_t data_length;
  unsigned char data[0];
} ErrorInfo;

/** An error_index may be used as an index into error_types and
 *  error_messages. */
typedef enum {
  /** An object was added with a different hash from the existing
   *  one. */
  OBJECT_HASH_MISMATCH_ERROR_INDEX = 0,
  /** An object that was created through a ray.put is lost. */
  PUT_RECONSTRUCTION_ERROR_INDEX,
  /** A worker died or was killed while executing a task. */
  WORKER_DIED_ERROR_INDEX,
  /** The total number of error types. */
  MAX_ERROR_INDEX
} error_index;

/** Information about the error to be displayed to the user. */
extern const char *error_types[];
extern const char *error_messages[];

/**
 * Push an error to the given Python driver.
 *
 * @param db_handle Database handle.
 * @param driver_id The ID of the Python driver to push the error
 *        to.
 * @param error_index The error information at this index in
 *        error_types and error_messages will be included in the
 *        error pushed to the driver.
 * @param data_length The length of the custom data to be included
 *        in the error.
 * @param data The custom data to be included in the error.
 * @return Void.
 */
void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                int error_index,
                size_t data_length,
                const unsigned char *data);

#endif
