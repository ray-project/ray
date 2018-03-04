#include "error_table.h"
#include "redis.h"

const char *error_types[] = {"object_hash_mismatch", "put_reconstruction",
                             "worker_died"};
const char *error_messages[] = {
    "A nondeterministic task was reexecuted.",
    "An object created by ray.put was evicted and could not be reconstructed. "
    "The driver may need to be restarted.",
    "A worker died or was killed while executing a task."};

void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                int error_index,
                size_t data_length,
                const unsigned char *data) {
  RAY_CHECK(error_index >= 0 && error_index < MAX_ERROR_INDEX);
  /* Allocate a struct to hold the error information. */
  ErrorInfo *info = (ErrorInfo *) malloc(sizeof(ErrorInfo) + data_length);
  info->driver_id = driver_id;
  info->error_index = error_index;
  info->data_length = data_length;
  memcpy(info->data, data, data_length);
  /* Generate a random key to identify this error message. */
  RAY_CHECK(sizeof(info->error_key) >= sizeof(UniqueID));
  UniqueID error_key = UniqueID::from_random();
  memcpy(info->error_key, error_key.data(), sizeof(info->error_key));

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(info), NULL, NULL,
                      redis_push_error, NULL);
}
