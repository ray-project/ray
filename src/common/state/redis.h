#ifndef REDIS_H
#define REDIS_H

#include <unordered_map>

#include "db.h"
#include "db_client_table.h"
#include "object_table.h"
#include "task_table.h"

#include "hiredis/hiredis.h"
#include "hiredis/async.h"

#define LOG_REDIS_ERROR(context, M, ...)                                     \
  RAY_LOG(ERROR) << "Redis error " << context->err << " " << context->errstr \
                 << "; " << M

#define LOG_REDIS_DEBUG(context, M, ...)                                     \
  RAY_LOG(DEBUG) << "Redis error " << context->err << " " << context->errstr \
                 << "; " << M;

struct DBHandle {
  /** String that identifies this client type. */
  char *client_type;
  /** Unique ID for this client. */
  DBClientID client;
  /** Primary redis context for all non-subscribe connections. This is used for
   * the database client table, heartbeats, and errors that should be pushed to
   * the driver. */
  redisAsyncContext *context;
  /** Primary redis context for "subscribe" communication. A separate context
   *  is needed for this communication (see
   *  https://github.com/redis/hiredis/issues/55). This is used for the
   *  database client table, heartbeats, and errors that should be pushed to
   *  the driver. */
  redisAsyncContext *subscribe_context;
  /** Redis contexts for shards for all non-subscribe connections. All requests
   *  to the object table, task table, and event table should be directed here.
   *  The correct shard can be retrieved using get_redis_context below. */
  std::vector<redisAsyncContext *> contexts;
  /** Redis contexts for shards for "subscribe" communication. All requests
   *  to the object table, task table, and event table should be directed here.
   *  The correct shard can be retrieved using get_redis_context below. */
  std::vector<redisAsyncContext *> subscribe_contexts;
  /** The event loop this global state store connection is part of. */
  event_loop *loop;
  /** Index of the database connection in the event loop */
  int64_t db_index;
  /** Cache for the IP addresses of db clients. This is an unordered map mapping
   *  client IDs to addresses. */
  std::unordered_map<DBClientID, DBClient, UniqueIDHasher> db_client_cache;
  /** Redis context for synchronous connections. This should only be used very
   *  rarely, it is not asynchronous. */
  redisContext *sync_context;
};

/**
 * Get the Redis asynchronous context responsible for non-subscription
 * communication for the given UniqueID.
 *
 * @param db The database handle.
 * @param id The ID whose location we are querying for.
 * @return The redisAsyncContext responsible for the given ID.
 */
redisAsyncContext *get_redis_context(DBHandle *db, UniqueID id);

/**
 * Get the Redis asynchronous context responsible for subscription
 * communication for the given UniqueID.
 *
 * @param db The database handle.
 * @param id The ID whose location we are querying for.
 * @return The redisAsyncContext responsible for the given ID.
 */
redisAsyncContext *get_redis_subscribe_context(DBHandle *db, UniqueID id);

/**
 * Get a list of Redis shard IP addresses from the primary shard.
 *
 * @param context A Redis context connected to the primary shard.
 * @param db_shards_addresses The IP addresses for the shards registered
 *        with the primary shard will be added to this vector.
 * @param db_shards_ports  The IP ports for the shards registered with the
 *        primary shard will be added to this vector, in the same order as
 *        db_shards_addresses.
 */
void get_redis_shards(redisContext *context,
                      std::vector<std::string> &db_shards_addresses,
                      std::vector<int> &db_shards_ports);

void redis_cache_set_db_client(DBHandle *db, DBClient client);

DBClient redis_cache_get_db_client(DBHandle *db, DBClientID db_client_id);

void redis_object_table_get_entry(redisAsyncContext *c,
                                  void *r,
                                  void *privdata);

void object_table_lookup_callback(redisAsyncContext *c,
                                  void *r,
                                  void *privdata);

/*
 * ==== Redis object table functions ====
 */

/**
 * Lookup object table entry in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_lookup(TableCallbackData *callback_data);

/**
 * Add a location entry to the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_add(TableCallbackData *callback_data);

/**
 * Remove a location entry from the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_remove(TableCallbackData *callback_data);

/**
 * Create a client-specific channel for receiving notifications from the object
 * table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_subscribe_to_notifications(
    TableCallbackData *callback_data);

/**
 * Request notifications about when certain objects become available.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_request_notifications(TableCallbackData *callback_data);

/**
 * Add a new object to the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_result_table_add(TableCallbackData *callback_data);

/**
 * Lookup the task that created the object in redis. The result is the task ID.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_result_table_lookup(TableCallbackData *callback_data);

/**
 * Callback invoked when the reply from the object table lookup command is
 * received.
 *
 * @param c Redis context.
 * @param r Reply.
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_object_table_lookup_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata);

/*
 * ==== Redis task table function =====
 */

/**
 * Get a task table entry, including the task spec and the task's scheduling
 * information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_get_task(TableCallbackData *callback_data);

/**
 * Add a task table entry with a new task spec and the task's scheduling
 * information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_add_task(TableCallbackData *callback_data);

/**
 * Update a task table entry with the task's scheduling information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_update(TableCallbackData *callback_data);

/**
 * Update a task table entry with the task's scheduling information, if the
 * task's current scheduling information matches the test value.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_test_and_update(TableCallbackData *callback_data);

/**
 * Callback invoked when the reply from the task push command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_task_table_publish_push_callback(redisAsyncContext *c,
                                            void *r,
                                            void *privdata);

/**
 * Callback invoked when the reply from the task publish command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_task_table_publish_publish_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata);

/**
 * Subscribe to updates of the task table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_subscribe(TableCallbackData *callback_data);

/**
 * Remove a client from the db clients table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_db_client_table_remove(TableCallbackData *callback_data);

/**
 * Subscribe to updates from the db client table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_db_client_table_subscribe(TableCallbackData *callback_data);

/**
 * Subscribe to updates from the local scheduler table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_local_scheduler_table_subscribe(TableCallbackData *callback_data);

/**
 * Publish an update to the local scheduler table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_local_scheduler_table_send_info(TableCallbackData *callback_data);

/**
 * Synchronously publish a null update to the local scheduler table signifying
 * that we are about to exit.
 *
 * @param db The database handle of the dying local scheduler.
 * @return Void.
 */
void redis_local_scheduler_table_disconnect(DBHandle *db);

/**
 * Subscribe to updates from the driver table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_driver_table_subscribe(TableCallbackData *callback_data);

/**
 * Publish an update to the driver table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_driver_table_send_driver_death(TableCallbackData *callback_data);

void redis_plasma_manager_send_heartbeat(TableCallbackData *callback_data);

/**
 * Marks an actor as removed. This prevents the actor from being resurrected.
 *
 * @param db The database handle.
 * @param actor_id The actor id to mark as removed.
 * @return Void.
 */
void redis_actor_table_mark_removed(DBHandle *db, ActorID actor_id);

/**
 * Subscribe to updates about newly created actors.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_actor_notification_table_subscribe(TableCallbackData *callback_data);

void redis_object_info_subscribe(TableCallbackData *callback_data);

void redis_push_error(TableCallbackData *callback_data);

#endif /* REDIS_H */
