#include "error_table.h"
#include "redis.h"

void push_error_hmset(db_handle *db_handle,
                      unique_id driver_id,
                      unique_id error_id,
                      char *error_type,
                      char *error_message,
                      error_table_push_error_hmset_callback done_callback,
                      retry_info *retry) {
  error_table_push_error_hmset_data *data =
      malloc(sizeof(error_table_push_error_hmset_data) - 1 +
             strlen(error_type) + strlen(error_message));
  data->driver_id = driver_id;
  data->error_id = error_id;
  data->error_type_len = strlen(error_type);
  data->error_message_len = strlen(error_message);
  memcpy(&data->error_type_and_message[0], error_type, data->error_type_len);
  memcpy(&data->error_type_and_message[data->error_type_len], error_message,
         data->error_message_len);

  init_table_callback(db_handle, NIL_ID, __func__, data, retry, done_callback,
                      redis_error_table_push_error_hmset, NULL);
}

void push_error_rpush(db_handle *db_handle,
                      unique_id driver_id,
                      unique_id error_id,
                      retry_info *retry) {
  error_table_push_error_rpush_data *data =
      malloc(sizeof(error_table_push_error_rpush_data));
  data->error_id = error_id;
  data->driver_id = driver_id;

  init_table_callback(db_handle, NIL_ID, __func__, data, retry, NULL,
                      redis_error_table_push_error_rpush, NULL);
}

void hmset_error_done_callback(db_handle *db_handle,
                               unique_id driver_id,
                               unique_id error_id) {
  push_error_rpush(db_handle, driver_id, error_id, NULL);
}

void push_error(db_handle *db_handle,
                unique_id driver_id,
                char *error_type,
                char *error_message,
                retry_info *retry) {
  /* TODO(rkn): The method globally_unique_id needs to be more efficient. */
  unique_id error_id = globally_unique_id();
  push_error_hmset(db_handle, driver_id, error_id, error_type, error_message,
                   hmset_error_done_callback, retry);
}
