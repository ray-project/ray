/* Redis implementation of the global state store */

#include <assert.h>
#include <stdbool.h>

#include <stdlib.h>
#include "hiredis/adapters/ae.h"
#include "utstring.h"

#include "common.h"
#include "db.h"
#include "object_table.h"
#include "task.h"
#include "task_log.h"
#include "task_table.h"
#include "event_loop.h"
#include "redis.h"
#include "io.h"

#define LOG_REDIS_ERR(context, M, ...)                                        \
  fprintf(stderr, "[ERROR] (%s:%d: message: %s) " M "\n", __FILE__, __LINE__, \
          context->errstr, ##__VA_ARGS__)

#define CHECK_REDIS_CONNECT(CONTEXT_TYPE, context, M, ...) \
  do {                                                     \
    CONTEXT_TYPE *_context = (context);                    \
    if (!_context) {                                       \
      LOG_ERR("could not allocate redis context");         \
      exit(-1);                                            \
    }                                                      \
    if (_context->err) {                                   \
      LOG_REDIS_ERR(_context, M, ##__VA_ARGS__);           \
      exit(-1);                                            \
    }                                                      \
  } while (0);

#define REDIS_CALLBACK_HEADER(DB, CB_DATA, REPLY)     \
  if ((REPLY) == NULL) {                              \
    return;                                           \
  }                                                   \
  db_handle *DB = c->data;                            \
  table_callback_data *CB_DATA =                      \
      outstanding_callbacks_find((int64_t) privdata); \
  if (CB_DATA == NULL)                                \
    /* the callback data structure has been           \
     * already freed; just ignore this reply */       \
    return;

db_handle *db_connect(const char *address,
                      int port,
                      const char *client_type,
                      const char *client_addr,
                      int client_port) {
  db_handle *db = malloc(sizeof(db_handle));
  /* Sync connection for initial handshake */
  redisReply *reply;
  long long num_clients;
  redisContext *context = redisConnect(address, port);
  CHECK_REDIS_CONNECT(redisContext, context, "could not connect to redis %s:%d",
                      address, port);
  /* Add new client using optimistic locking. */
  while (1) {
    reply = redisCommand(context, "WATCH %s", client_type);
    freeReplyObject(reply);
    reply = redisCommand(context, "HLEN %s", client_type);
    num_clients = reply->integer;
    freeReplyObject(reply);
    reply = redisCommand(context, "MULTI");
    freeReplyObject(reply);
    reply = redisCommand(context, "HSET %s %lld %s:%d", client_type,
                         num_clients, client_addr, client_port);
    freeReplyObject(reply);
    reply = redisCommand(context, "EXEC");
    CHECK(reply);
    if (reply->type != REDIS_REPLY_NIL) {
      freeReplyObject(reply);
      break;
    }
    freeReplyObject(reply);
  }

  db->client_type = strdup(client_type);
  db->client_id = num_clients;
  db->service_cache = NULL;
  db->sync_context = context;
  utarray_new(db->callback_freelist, &ut_ptr_icd);

  /* Establish async connection */
  db->context = redisAsyncConnect(address, port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->context,
                      "could not connect to redis %s:%d", address, port);
  db->context->data = (void *) db;
  /* Establish async connection for subscription */
  db->sub_context = redisAsyncConnect(address, port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->sub_context,
                      "could not connect to redis %s:%d", address, port);
  db->sub_context->data = (void *) db;

  return db;
}

void db_disconnect(db_handle *db) {
  redisFree(db->sync_context);
  redisAsyncFree(db->context);
  redisAsyncFree(db->sub_context);
  service_cache_entry *e, *tmp;
  HASH_ITER(hh, db->service_cache, e, tmp) {
    free(e->addr);
    HASH_DEL(db->service_cache, e);
    free(e);
  }
  free(db->client_type);
  void **p = NULL;
  while ((p = (void **) utarray_next(db->callback_freelist, p))) {
    free(*p);
  }
  utarray_free(db->callback_freelist);
  free(db);
}

void db_attach(db_handle *db, event_loop *loop) {
  db->loop = loop;
  redisAeAttach(loop, db->context);
  redisAeAttach(loop, db->sub_context);
}

/*
 *  ==== object_table callbacks ====
 */

void redis_object_table_add_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)

  if (callback_data->done_callback) {
    task_log_done_callback done_callback = callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_add_location(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  redisAsyncCommand(db->context, redis_object_table_add_callback,
                    (void *) callback_data->timer_id, "SADD obj:%b %d",
                    &callback_data->id.id[0], UNIQUE_ID_SIZE, db->client_id);
  if (db->context->err) {
    LOG_REDIS_ERR(db->context, "could not add object_table entry");
  }
}

void redis_object_table_lookup_location(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;

  /* Call redis asynchronously */
  redisAsyncCommand(db->context, redis_object_table_get_entry,
                    (void *) callback_data->timer_id, "SMEMBERS obj:%b",
                    &callback_data->id.id[0], UNIQUE_ID_SIZE);
  if (db->context->err) {
    LOG_REDIS_ERR(db->context, "error in object_table lookup");
  }
}

void redis_object_table_new_object_callback(redisAsyncContext *c,
                                            void *r,
                                            void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;
  CHECK(reply->type == REDIS_REPLY_STATUS ||
        reply->type == REDIS_REPLY_INTEGER);
  object_metadata *metadata = callback_data->data;
  if (callback_data->done_callback) {
    object_table_metadata_done_callback done_callback =
        callback_data->done_callback;
    done_callback(callback_data->id, metadata, callback_data->user_context);
  }
  free_object_metadata(metadata);
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_new_object(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  object_metadata *metadata = callback_data->data;
  /* Add the metadata entry to the object table. */
  redisAsyncCommand(db->context, redis_object_table_new_object_callback,
                    (void *) callback_data->timer_id,
                    "HSET obj:metadata:%b task_id %b", callback_data->id.id,
                    UNIQUE_ID_SIZE, metadata->task_id.id, UNIQUE_ID_SIZE);
}

void redis_object_table_lookup_metadata_task_callback(redisAsyncContext *c,
                                                      void *r,
                                                      void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;
  object_table_metadata_done_callback done_callback =
      callback_data->done_callback;
  object_metadata *metadata = callback_data->data;
  if (reply->type == REDIS_REPLY_STRING) {
    /* If we found the task. */
    metadata->task = malloc(reply->len);
    memcpy(metadata->task, reply->str, reply->len);
    if (done_callback) {
      done_callback(callback_data->id, metadata, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  } else if (reply->type == REDIS_REPLY_NIL) {
    /* If the task that created the requested object is not in the table. */
    LOG_ERR("Task that created object not in task table.");
    if (done_callback) {
      done_callback(callback_data->id, metadata, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  } else {
    LOG_ERR("expected string or nil, received type %d", reply->type);
    exit(-1);
  }
}

void redis_object_table_lookup_metadata_object_callback(redisAsyncContext *c,
                                                        void *r,
                                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;

  if (reply->type == REDIS_REPLY_STRING) {
    /* If we found the object, get the spec of the task that created it. */
    object_metadata *metadata = callback_data->data;
    CHECK(reply->len == sizeof(metadata->task_id));
    memcpy(&metadata->task_id, reply->str, reply->len);
    redisAsyncCommand(db->context,
                      redis_object_table_lookup_metadata_task_callback,
                      (void *) callback_data->timer_id, "GET task:%b",
                      metadata->task_id.id, UNIQUE_ID_SIZE);
  } else if (reply->type == REDIS_REPLY_NIL) {
    /* The object with the requested ID was not in the table. */
    LOG_DEBUG("Object metadata not in table.");
    object_table_metadata_done_callback done_callback =
        callback_data->done_callback;
    if (done_callback) {
      done_callback(callback_data->id, NULL, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
    return;
  } else {
    LOG_ERR("expected string or nil, received type %d", reply->type);
    exit(-1);
  }
}

void redis_object_table_lookup_metadata(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  /* First, lookup the ID of the task that created this object. */
  redisAsyncCommand(
      db->context, redis_object_table_lookup_metadata_object_callback,
      (void *) callback_data->timer_id, "HGET obj:metadata:%b task_id",
      callback_data->id.id, UNIQUE_ID_SIZE);
}

/**
 * Get an entry from the plasma manager table in redis.
 *
 * @param db The database handle.
 * @param index The index of the plasma manager.
 * @param *manager The pointer where the IP address of the manager gets written.
 * @return Void.
 */
void redis_get_cached_service(db_handle *db, int index, const char **manager) {
  service_cache_entry *entry;
  HASH_FIND_INT(db->service_cache, &index, entry);
  if (!entry) {
    /* This is a very rare case. */
    redisReply *reply =
        redisCommand(db->sync_context, "HGET %s %lld", db->client_type, index);
    CHECK(reply->type == REDIS_REPLY_STRING);
    entry = malloc(sizeof(service_cache_entry));
    entry->service_id = index;
    entry->addr = strdup(reply->str);
    HASH_ADD_INT(db->service_cache, service_id, entry);
    freeReplyObject(reply);
  }
  *manager = entry->addr;
}

void redis_object_table_get_entry(redisAsyncContext *c,
                                  void *r,
                                  void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;

  int *managers = malloc(reply->elements * sizeof(int));
  int64_t manager_count = reply->elements;

  if (reply->type == REDIS_REPLY_ARRAY) {
    const char **manager_vector = malloc(manager_count * sizeof(char *));
    for (int j = 0; j < reply->elements; j++) {
      CHECK(reply->element[j]->type == REDIS_REPLY_STRING);
      managers[j] = atoi(reply->element[j]->str);
      redis_get_cached_service(db, managers[j], manager_vector + j);
    }

    object_table_lookup_location_done_callback done_callback =
        callback_data->done_callback;
    done_callback(callback_data->id, manager_count, manager_vector,
                  callback_data->user_context);
    /* remove timer */
    destroy_timer_callback(callback_data->db_handle->loop, callback_data);
    free(managers);
  } else {
    LOG_ERR("expected integer or string, received type %d", reply->type);
    exit(-1);
  }
}

void object_table_redis_callback(redisAsyncContext *c,
                                 void *r,
                                 void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* First entry is message type, second is topic, third is payload. */
  CHECK(reply->elements > 2);
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
  if (strncmp(reply->element[1]->str, "add", 3) != 0) {
    if (callback_data->done_callback) {
      object_table_location_done_callback done_callback =
          callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the task and call the callback. */
  object_table_subscribe_data *data = callback_data->data;

  if (data->object_available_callback) {
    data->object_available_callback(callback_data->id, data->subscribe_context);
  }
}

void redis_object_table_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;

  /* subscribe to key notification associated to object id */

  redisAsyncCommand(db->sub_context, object_table_redis_callback,
                    (void *) callback_data->timer_id,
                    "SUBSCRIBE __keyspace@0__:%b add",
                    (char *) &callback_data->id.id[0], UNIQUE_ID_SIZE);

  if (db->sub_context->err) {
    LOG_REDIS_ERR(db->sub_context,
                  "error in redis_object_table_subscribe_callback");
  }
}

/*
 *  ==== task_log callbacks ====
 */

void redis_task_log_publish(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  task_instance *task_instance = callback_data->data;
  task_iid task_iid = *task_instance_id(task_instance);
  node_id node = *task_instance_node(task_instance);
  int32_t state = *task_instance_state(task_instance);

  LOG_DEBUG("Called log_publish callback");

/* Check whether the vector (requests_info) indicating the status of the
 * requests has been allocated.
 * If was not allocate it, allocate it and initialize it.
 * This vector has an entry for each redis command, and it stores true if a
 * reply for that command
 * has been received, and false otherwise.
 * The first entry in the callback corresponds to RPUSH, and the second entry to
 * PUBLISH.
 */
#define NUM_DB_REQUESTS 2
#define PUSH_INDEX 0
#define PUBLISH_INDEX 1
  if (callback_data->requests_info == NULL) {
    callback_data->requests_info = malloc(NUM_DB_REQUESTS * sizeof(bool));
    for (int i = 0; i < NUM_DB_REQUESTS; i++) {
      ((bool *) callback_data->requests_info)[i] = false;
    }
  }

  if (((bool *) callback_data->requests_info)[PUSH_INDEX] == false) {
    if (*task_instance_state(task_instance) == TASK_STATUS_WAITING) {
      redisAsyncCommand(db->context, redis_task_log_publish_push_callback,
                        (void *) callback_data->timer_id, "RPUSH tasklog:%b %b",
                        (char *) &task_iid.id[0], UNIQUE_ID_SIZE,
                        (char *) task_instance,
                        task_instance_size(task_instance));
    } else {
      task_update update = {.state = state, .node = node};
      redisAsyncCommand(db->context, redis_task_log_publish_push_callback,
                        (void *) callback_data->timer_id, "RPUSH tasklog:%b %b",
                        (char *) &task_iid.id[0], UNIQUE_ID_SIZE,
                        (char *) &update, sizeof(update));
    }

    if (db->context->err) {
      LOG_REDIS_ERR(db->context, "error setting task in task_log_add_task");
    }
  }

  if (((bool *) callback_data->requests_info)[PUBLISH_INDEX] == false) {
    redisAsyncCommand(db->context, redis_task_log_publish_publish_callback,
                      (void *) callback_data->timer_id,
                      "PUBLISH task_log:%b:%d %b", (char *) &node.id[0],
                      UNIQUE_ID_SIZE, state, (char *) task_instance,
                      task_instance_size(task_instance));

    if (db->context->err) {
      LOG_REDIS_ERR(db->context, "error publishing task in task_log_add_task");
    }
  }
}

void redis_task_log_publish_push_callback(redisAsyncContext *c,
                                          void *r,
                                          void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)

  CHECK(callback_data->requests_info != NULL);
  ((bool *) callback_data->requests_info)[PUSH_INDEX] = true;

  if (((bool *) callback_data->requests_info)[PUBLISH_INDEX] == true) {
    if (callback_data->done_callback) {
      task_log_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  }
}

void redis_task_log_publish_publish_callback(redisAsyncContext *c,
                                             void *r,
                                             void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)

  CHECK(callback_data->requests_info != NULL);
  ((bool *) callback_data->requests_info)[PUBLISH_INDEX] = true;

  if (((bool *) callback_data->requests_info)[PUSH_INDEX] == true) {
    if (callback_data->done_callback) {
      task_log_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  }
}

void task_log_redis_callback(redisAsyncContext *c, void *r, void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* First entry is message type, second is topic, third is payload. */
  CHECK(reply->elements > 2);
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
  if (reply->element[2]->str == NULL) {
    if (callback_data->done_callback) {
      task_log_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the task and call the callback. */
  task_log_subscribe_data *data = callback_data->data;

  task_instance *instance = malloc(reply->element[2]->len);
  memcpy(instance, reply->element[2]->str, reply->element[2]->len);
  if (data->subscribe_callback) {
    data->subscribe_callback(instance, data->subscribe_context);
  }
  task_instance_free(instance);
}

void redis_task_log_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  task_log_subscribe_data *data = callback_data->data;

  if (memcmp(&data->node.id[0], &NIL_ID.id[0], UNIQUE_ID_SIZE) == 0) {
    redisAsyncCommand(db->sub_context, task_log_redis_callback,
                      (void *) callback_data->timer_id,
                      "PSUBSCRIBE task_log:*:%d", data->state_filter);
  } else {
    redisAsyncCommand(db->sub_context, task_log_redis_callback,
                      (void *) callback_data->timer_id,
                      "SUBSCRIBE task_log:%b:%d", (char *) &data->node.id[0],
                      UNIQUE_ID_SIZE, data->state_filter);
  }
  if (db->sub_context->err) {
    LOG_REDIS_ERR(db->sub_context, "error in task_log_register_callback");
  }
}

void redis_task_table_add_task_callback(redisAsyncContext *c, void *r, void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;
  CHECK(reply->type == REDIS_REPLY_STATUS);
  if (callback_data->done_callback) {
    task_table_callback done_callback = callback_data->done_callback;
    done_callback(callback_data->id, callback_data->data,
                  callback_data->user_context);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_add_task(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  task_spec *task = callback_data->data;
  redisAsyncCommand(db->context, redis_task_table_add_task_callback,
                    (void *) callback_data->timer_id, "SET task:%b %b",
                    callback_data->id.id, UNIQUE_ID_SIZE,
                    task, task_size(task));
}

void redis_task_table_get_task_callback(redisAsyncContext *c, void *r, void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r)
  redisReply *reply = r;

  if (reply->type == REDIS_REPLY_STRING) {
    if (callback_data->done_callback) {
      task_spec *task = malloc(reply->len);
      memcpy(task, reply->str, reply->len);
      task_table_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, task, callback_data->user_context);
      free(task);
    }
    destroy_timer_callback(db->loop, callback_data);
  } else if (reply->type == REDIS_REPLY_NIL) {
    if (callback_data->done_callback) {
      task_table_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, NULL, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  } else {
    LOG_ERR("expected string, received type %d", reply->type);
    exit(-1);
  }
}

void redis_task_table_get_task(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  redisAsyncCommand(db->context, redis_task_table_get_task_callback,
                    (void *) callback_data->timer_id, "GET task:%b",
                    callback_data->id.id, UNIQUE_ID_SIZE);
}

int get_client_id(db_handle *db) {
  if (db) {
    return db->client_id;
  } else {
    return -1;
  }
}
