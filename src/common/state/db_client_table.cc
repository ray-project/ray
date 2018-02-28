#include "db_client_table.h"
#include "redis.h"

void db_client_table_remove(DBHandle *db_handle,
                            DBClientID db_client_id,
                            RetryInfo *retry,
                            db_client_table_done_callback done_callback,
                            void *user_context) {
  init_table_callback(db_handle, db_client_id, __func__,
                      new CommonCallbackData(NULL), retry,
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

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(sub_data), retry,
                      (table_done_callback) done_callback,
                      redis_db_client_table_subscribe, user_context);
}

const std::vector<std::string> db_client_table_get_ip_addresses(
    DBHandle *db_handle,
    const std::vector<DBClientID> &manager_ids) {
  /* We time this function because in the past this loop has taken multiple
   * seconds under stressful situations on hundreds of machines causing the
   * plasma manager to die (because it went too long without sending
   * heartbeats). */
  int64_t start_time = current_time_ms();

  /* Construct the manager vector from the flatbuffers object. */
  std::vector<std::string> manager_vector;

  for (auto const &manager_id : manager_ids) {
    DBClient client = redis_cache_get_db_client(db_handle, manager_id);
    RAY_CHECK(!client.manager_address.empty());
    manager_vector.push_back(client.manager_address);
  }

  int64_t end_time = current_time_ms();
  if (end_time - start_time > RayConfig::instance().max_time_for_loop()) {
    RAY_LOG(WARNING) << "calling redis_get_cached_db_client in a loop in with "
                     << manager_ids.size() << " manager IDs took "
                     << end_time - start_time << " milliseconds.";
  }

  return manager_vector;
}

void db_client_table_update_cache_callback(DBClient *db_client,
                                           void *user_context) {
  DBHandle *db_handle = (DBHandle *) user_context;
  redis_cache_set_db_client(db_handle, *db_client);
}

void db_client_table_cache_init(DBHandle *db_handle) {
  db_client_table_subscribe(db_handle, db_client_table_update_cache_callback,
                            db_handle, NULL, NULL, NULL);
}

DBClient db_client_table_cache_get(DBHandle *db_handle, DBClientID client_id) {
  RAY_CHECK(!client_id.is_nil());
  return redis_cache_get_db_client(db_handle, client_id);
}

void plasma_manager_send_heartbeat(DBHandle *db_handle) {
  RetryInfo heartbeat_retry;
  heartbeat_retry.num_retries = 0;
  heartbeat_retry.timeout =
      RayConfig::instance().heartbeat_timeout_milliseconds();
  heartbeat_retry.fail_callback = NULL;

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(NULL),
                      (RetryInfo *) &heartbeat_retry, NULL,
                      redis_plasma_manager_send_heartbeat, NULL);
}
