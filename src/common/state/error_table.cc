#include "error_table.h"
#include "redis.h"

void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                int error_index,
                size_t data_length,
                unsigned char *data) {
  CHECK(error_index >= 0 && error_index < MAX_ERROR_INDEX);
  /* Allocate a struct to hold the error information. */
  ErrorInfo *info = (ErrorInfo *) malloc(sizeof(ErrorInfo) + data_length);
  info->driver_id = driver_id;
  info->error_index = error_index;
  info->data_length = data_length;
  memcpy(info->data, data, data_length);
  /* Generate a random key to identify this error message. */
  CHECK(sizeof(info->error_key) >= UNIQUE_ID_SIZE);
  UniqueID error_key = globally_unique_id();
  memcpy(info->error_key, error_key.id, sizeof(info->error_key));

  init_table_callback(db_handle, NIL_ID, __func__, info, NULL, NULL,
                      redis_push_error, NULL);
}
