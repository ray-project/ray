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
  /* Unique ID for this service. */
  int service_id;
  /* IP address and port of this service. */
  char *addr;
  /* Handle for the uthash table. */
  UT_hash_handle hh;
} service_cache_entry;

typedef struct {
  /* The callback that will be called. */
  task_log_callback callback;
  /* Userdata associated with the callback. */
  void *userdata;
} task_log_callback_data;

struct db_handle_impl {
  /* String that identifies this client type. */
  char *client_type;
  /* Unique ID for this client within the type. */
  int64_t client_id;
  /* Redis context for this global state store connection. */
  redisAsyncContext *context;
  /* Redis context for "subscribe" communication.
   * Yes, we need a separate one for that, see
   * https://github.com/redis/hiredis/issues/55 */
  redisAsyncContext *sub_context;
  /* The event loop this global state store connection is part of. */
  event_loop *loop;
  /* Index of the database connection in the event loop */
  int64_t db_index;
  /* Cache for the IP addresses of services. */
  service_cache_entry *service_cache;
  /* Redis context for synchronous connections.
   * Should only be used very rarely, it is not asynchronous. */
  redisContext *sync_context;
  /* Data structure for callbacks that needs to be freed. */
  UT_array *callback_freelist;
};

typedef struct {
  /* The callback that will be called. */
  lookup_callback callback;
  /* Object ID that is looked up. */
  object_id object_id;
  /* Data context for the callback. */
  void *context;
} lookup_callback_data;

void object_table_get_entry(redisAsyncContext *c, void *r, void *privdata);

void object_table_lookup_callback(redisAsyncContext *c,
                                  void *r,
                                  void *privdata);

#endif
