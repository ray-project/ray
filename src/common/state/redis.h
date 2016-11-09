#ifndef REDIS_H
#define REDIS_H

#include "db.h"
#include "object_table.h"
#include "task_log.h"

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"
#include "utarray.h"

typedef struct {
  /** Unique ID for this service. */
  int service_id;
  /** IP address and port of this service. */
  char *addr;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} service_cache_entry;

struct db_handle {
  /** String that identifies this client type. */
  char *client_type;
  /** Unique ID for this client within the type. */
  int64_t client_id;
  /** Redis context for this global state store connection. */
  redisAsyncContext *context;
  /** Redis context for "subscribe" communication.
   * Yes, we need a separate one for that, see
   * https://github.com/redis/hiredis/issues/55 */
  redisAsyncContext *sub_context;
  /** The event loop this global state store connection is part of. */
  event_loop *loop;
  /** Index of the database connection in the event loop */
  int64_t db_index;
  /** Cache for the IP addresses of services. */
  service_cache_entry *service_cache;
  /** Redis context for synchronous connections.
   * Should only be used very rarely, it is not asynchronous. */
  redisContext *sync_context;
  /** Data structure for callbacks that needs to be freed. */
  UT_array *callback_freelist;
};

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
void redis_object_table_lookup(table_callback_data *callback_data);

/**
 * Add an entry to the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_add(table_callback_data *callback_data);

/**
 * Subscribe to learn when a new object becomes available.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_subscribe(table_callback_data *callback_data);

/*
 * ==== Redis task table function =====
 */

/**
 * Add or update task log entry with new scheduling information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_log_publish(table_callback_data *callback_data);

/**
 * Callback invoked when the replya from the task push command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_task_log_publish_push_callback(redisAsyncContext *c,
                                          void *r,
                                          void *privdata);

/**
 * Callback invoked when the replya from the task publish command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_task_log_publish_publish_callback(redisAsyncContext *c,
                                             void *r,
                                             void *privdata);

/**
 * Subscribe to updates of the task log.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_log_subscribe(table_callback_data *callback_data);

#endif /* REDIS_H */
