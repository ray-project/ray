#include "db_client_table.h"
#include "redis.h"

void db_client_table_remove(DBHandle *db_handle,
                            DBClientID db_client_id,
                            RetryInfo *retry,
                            db_client_table_done_callback done_callback,
                            void *user_context) {
  init_table_callback(db_handle, db_client_id, __func__, NULL, retry,
                      (table_done_callback) done_callback,
                      redis_db_client_table_remove, user_context);
}

void db_client_table_subscribe(
    DBHandle *db_handle,
    db_client_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry,
    db_client_table_done_callback done_callback,
    void *user_context) {
  DBClientTableSubscribeData *sub_data =
      (DBClientTableSubscribeData *) malloc(sizeof(DBClientTableSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry,
                      (table_done_callback) done_callback,
                      redis_db_client_table_subscribe, user_context);
}

void plasma_manager_send_heartbeat(DBHandle *db_handle) {
  RetryInfo heartbeat_retry = {.num_retries = 0,
                               .timeout = HEARTBEAT_TIMEOUT_MILLISECONDS,
                               .fail_callback = NULL};

  init_table_callback(db_handle, NIL_ID, __func__, NULL,
                      (RetryInfo *) &heartbeat_retry, NULL,
                      redis_plasma_manager_send_heartbeat, NULL);
}
