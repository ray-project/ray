#include "error_table.h"
#include "redis.h"

const char *error_types[] = {"object_hash_mismatch", "put_reconstruction",
                             "worker_died", "actor_not_created"};

void push_error(DBHandle *db_handle,
                DBClientID driver_id,
                ErrorIndex error_type,
                const std::string &error_message) {
  int64_t message_size = error_message.size();

  /* Allocate a struct to hold the error information. */
  ErrorInfo *info = (ErrorInfo *) malloc(sizeof(ErrorInfo) + message_size);
  info->driver_id = driver_id;
  info->error_type = error_type;
  info->error_key = UniqueID::from_random();
  info->size = message_size;
  memcpy(info->error_message, error_message.data(), message_size);

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(info), NULL, NULL,
                      redis_push_error, NULL);
}
