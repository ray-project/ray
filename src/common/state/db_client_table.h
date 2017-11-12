#ifndef DB_CLIENT_TABLE_H
#define DB_CLIENT_TABLE_H

#include <vector>

#include "db.h"
#include "table.h"

typedef void (*db_client_table_done_callback)(DBClientID db_client_id,
                                              void *user_context);

/**
 * Remove a client from the db clients table.
 *
 * @param db_handle Database handle.
 * @param db_client_id The database client ID to remove.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 *
 */
void db_client_table_remove(DBHandle *db_handle,
                            DBClientID db_client_id,
                            RetryInfo *retry,
                            db_client_table_done_callback done_callback,
                            void *user_context);

/*
 *  ==== Subscribing to the db client table ====
 */

/* An entry in the db client table. */
typedef struct {
  /** The database client ID. */
  DBClientID id;
  /** The database client type. */
  std::string client_type;
  /** An optional auxiliary address for the plasma manager associated with this
   *  database client. */
  std::string manager_address;
  /** Whether or not the database client exists. If this is false for an entry,
   *  then it will never again be true. */
  bool is_alive;
} DBClient;

/* Callback for subscribing to the db client table. */
typedef void (*db_client_table_subscribe_callback)(DBClient *db_client,
                                                   void *user_context);

/**
 * Register a callback for a db client table event.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the db client
 *        table is updated.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void db_client_table_subscribe(
    DBHandle *db_handle,
    db_client_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry,
    db_client_table_done_callback done_callback,
    void *user_context);

/* Data that is needed to register db client table subscribe callbacks with the
 * state database. */
typedef struct {
  db_client_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} DBClientTableSubscribeData;

const std::vector<std::string> db_client_table_get_ip_addresses(
    DBHandle *db,
    const std::vector<DBClientID> &manager_ids);

/**
 * Initialize the db client cache. The cache is updated with each notification
 * from the db client table.
 *
 * @param db_handle Database handle.
 * @return Void.
 */
void db_client_table_cache_init(DBHandle *db_handle);

/**
 * Get a db client from the cache. If the requested client is not there,
 * request the latest entry from the db client table.
 *
 * @param db_handle Database handle.
 * @param client_id The ID of the client to look up in the cache.
 * @return The database client in the cache.
 */
DBClient db_client_table_cache_get(DBHandle *db_handle, DBClientID client_id);

/*
 * ==== Plasma manager heartbeats ====
 */

/**
 * Start sending heartbeats to the plasma_managers channel. Each
 * heartbeat contains this database client's ID. Heartbeats can be subscribed
 * to through the plasma_managers channel. Once called, this "retries" the
 * heartbeat operation forever, every heartbeat_timeout_milliseconds
 * milliseconds.
 *
 * @param db_handle Database handle.
 * @return Void.
 */
void plasma_manager_send_heartbeat(DBHandle *db_handle);

#endif /* DB_CLIENT_TABLE_H */
