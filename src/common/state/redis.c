/* Redis implementation of the global state store */

#include <assert.h>
#include <stdbool.h>

#include <stdlib.h>
/* Including hiredis here is necessary on Windows for typedefs used in ae.h. */
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
#include "utstring.h"

#include "common.h"
#include "db.h"
#include "db_client_table.h"
#include "object_table.h"
#include "task.h"
#include "task_table.h"
#include "event_loop.h"
#include "redis.h"
#include "io.h"

#define CHECK_REDIS_CONNECT(CONTEXT_TYPE, context, M, ...) \
  do {                                                     \
    CONTEXT_TYPE *_context = (context);                    \
    if (!_context) {                                       \
      LOG_FATAL("could not allocate redis context");       \
    }                                                      \
    if (_context->err) {                                   \
      LOG_ERROR(M, ##__VA_ARGS__);                         \
      LOG_REDIS_ERROR(_context, "");                       \
      exit(-1);                                            \
    }                                                      \
  } while (0)

/**
 * A header for callbacks of a single Redis asynchronous command. The user must
 * pass in the table operation's timer ID as the asynchronous command's
 * privdata field when executing the asynchronous command. The user must define
 * variable names for DB and CB_DATA. After this piece of code runs, DB
 * will hold a reference to the database handle, CB_DATA will hold a reference
 * to the callback data for this table operation. The user must pass in the
 * redisReply pointer as the REPLY argument.
 *
 * This header also short-circuits the entire callback if: (1) there was no
 * reply from Redis, or (2) the callback data for this table operation was
 * already removed, meaning that the operation was already marked as succeeded
 * or failed.
 */
#define REDIS_CALLBACK_HEADER(DB, CB_DATA, REPLY)     \
  if ((REPLY) == NULL) {                              \
    return;                                           \
  }                                                   \
  db_handle *DB = c->data;                            \
  table_callback_data *CB_DATA =                      \
      outstanding_callbacks_find((int64_t) privdata); \
  if (CB_DATA == NULL) {                              \
    /* the callback data structure has been           \
     * already freed; just ignore this reply */       \
    return;                                           \
  }                                                   \
  do {                                                \
  } while (0)

/**
 * A data structure to track the status of a table operation attempt that spans
 * multiple Redis commands. Each attempt at a table operation is associated
 * with a unique redis_requests_info instance. To use this data structure, pass
 * it as the `privdata` argument for the callback of each asynchronous Redis
 * command.
 */
typedef struct {
  /** The timer ID that uniquely identifies this table operation. All retry
   *  attempts of a table operation share the same timer ID. */
  int64_t timer_id;
  /** The index of the next command to try for this operation. This may be
   *  different across different attempts of the same table operation. */
  int request_index;
  /** Whether the current invocation of the callback was triggered by a reply
   *  to an asynchronous Redis command. If not, then the callback was called
   *  directly. */
  bool is_redis_reply;
} redis_requests_info;

/**
 * A header for callbacks similar to REDIS_CALLBACK_HEADER, but for operations
 * that span multiple Redis commands. The differences are:
 * - Instead of passing in the table operation's timer ID as the asynchronous
 *   command callback's `privdata` argument, the user must pass a pointer to a
 *   redis_requests_info instance.
 * - The user must define an additional REQUEST_INFO variable name, which will
 *   hold a reference to the redis_requests_info passed into the Redis
 *   asynchronous command.
 */
#define REDIS_MULTI_CALLBACK_HEADER(DB, CB_DATA, REPLY, REQUEST_INFO) \
  db_handle *DB = c->data;                                            \
  redis_requests_info *REQUEST_INFO = privdata;                       \
  DCHECK(REQUEST_INFO != NULL);                                       \
  if ((REPLY) == NULL && REQUEST_INFO->is_redis_reply) {              \
    free(REQUEST_INFO);                                               \
    return;                                                           \
  }                                                                   \
  table_callback_data *CB_DATA =                                      \
      outstanding_callbacks_find(REQUEST_INFO->timer_id);             \
  if (CB_DATA == NULL) {                                              \
    /* the callback data structure has been                           \
     * already freed; just ignore this reply */                       \
    free(privdata);                                                   \
    return;                                                           \
  }                                                                   \
  do {                                                                \
  } while (0)

db_handle *db_connect(const char *address,
                      int port,
                      const char *client_type,
                      const char *client_addr,
                      int client_port) {
  db_handle *db = malloc(sizeof(db_handle));
  /* Sync connection for initial handshake */
  redisReply *reply;
  redisContext *context = redisConnect(address, port);
  CHECK_REDIS_CONNECT(redisContext, context, "could not connect to redis %s:%d",
                      address, port);
  /* Enable keyspace events. */
  reply = redisCommand(context, "CONFIG SET notify-keyspace-events AKE");
  CHECK(reply != NULL);
  freeReplyObject(reply);
  /* Add new client using optimistic locking. */
  db_client_id client = globally_unique_id();
  while (true) {
    reply = redisCommand(context, "WATCH %s", client_type);
    freeReplyObject(reply);
    reply = redisCommand(context, "HLEN %s", client_type);
    freeReplyObject(reply);
    reply = redisCommand(context, "MULTI");
    freeReplyObject(reply);
    reply = redisCommand(
        context,
        "HMSET db_clients:%b client_type %s address %s:%d db_client_id %b",
        (char *) client.id, sizeof(client.id), client_type, client_addr,
        client_port, (char *) client.id, sizeof(client.id));
    freeReplyObject(reply);
    reply = redisCommand(context, "PUBLISH db_clients %b:%s",
                         (char *) client.id, sizeof(client.id), client_type);
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
  db->client = client;
  db->db_client_cache = NULL;
  db->sync_context = context;

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
  db_client_cache_entry *e, *tmp;
  HASH_ITER(hh, db->db_client_cache, e, tmp) {
    free(e->addr);
    HASH_DELETE(hh, db->db_client_cache, e);
    free(e);
  }
  free(db->client_type);
  free(db);
}

void db_attach(db_handle *db, event_loop *loop, bool reattach) {
  db->loop = loop;
  int err = redisAeAttach(loop, db->context);
  /* If the database is reattached in the tests, redis normally gives
   * an error which we can safely ignore. */
  if (!reattach) {
    CHECKM(err == REDIS_OK, "failed to attach the event loop");
  }
  err = redisAeAttach(loop, db->sub_context);
  if (!reattach) {
    CHECKM(err == REDIS_OK, "failed to attach the event loop");
  }
}

/**
 * An internal function to allocate a task object and parse a hashmap reply
 * from Redis into the task object.  If the Redis reply is malformed, an empty
 * task with the given task ID is returned.
 *
 * @param id The ID of the task we're looking up. If the reply from Redis is
 *        well-formed, the reply's ID should match this ID. Else, the returned
 *        task will have its ID set to this ID.
 * @param num_redis_replies The number of keys and values in the Redis hashmap.
 * @param redis_replies A pointer to the Redis hashmap keys and values.
 * @return A pointer to the parsed task.
 */
task *parse_redis_task_table_entry(task_id id,
                                   int num_redis_replies,
                                   redisReply **redis_replies) {
  task *task_result;
  if (num_redis_replies == 0) {
    /* There was no information about this task. */
    return NULL;
  }
  /* Exit immediately if there weren't 6 fields, one for each key-value pair.
   * The keys are "node", "state", and "task_spec". */
  DCHECK(num_redis_replies == 6);
  /* Parse the task struct's fields. */
  scheduling_state state = 0;
  node_id node = NIL_ID;
  task_spec *spec = NULL;
  for (int i = 0; i < num_redis_replies; i = i + 2) {
    char *key = redis_replies[i]->str;
    redisReply *value = redis_replies[i + 1];
    if (strcmp(key, "node") == 0) {
      DCHECK(value->len == sizeof(node_id));
      memcpy(&node, value->str, value->len);
    } else if (strcmp(key, "state") == 0) {
      int scanned = sscanf(value->str, "%d", (int *) &state);
      if (scanned != 1) {
        LOG_FATAL("Scheduling state for task is malformed");
        state = 0;
      }
    } else if (strcmp(key, "task_spec") == 0) {
      spec = malloc(value->len);
      memcpy(spec, value->str, value->len);
    } else {
      LOG_FATAL("Found unexpected %s field in task log", key);
    }
  }
  /* Exit immediately if we couldn't parse the task spec. */
  if (spec == NULL) {
    LOG_FATAL("Could not parse task spec from task log");
  }
  /* Build and return the task. */
  DCHECK(task_ids_equal(task_spec_id(spec), id));
  task_result = alloc_task(spec, state, node);
  free_task_spec(spec);
  return task_result;
}

/*
 *  ==== object_table callbacks ====
 */

void redis_object_table_add_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  LOG_DEBUG("Calling object table add callback");
  REDIS_MULTI_CALLBACK_HEADER(db, callback_data, r, requests_info);
  redisReply *reply = r;
  object_id id = callback_data->id;
  unsigned char *digest = callback_data->data;

#define NUM_CHECK_AND_SET_COMMANDS 3
#define CHECK_AND_SET_SETNX_INDEX 0
#define CHECK_AND_SET_GET_INDEX 1
#define CHECK_AND_SET_SADD_INDEX 2

  /* Check that we're at a valid command index. */
  int request_index = requests_info->request_index;
  LOG_DEBUG("Object table add request index is %d", request_index);
  CHECK(request_index <= NUM_CHECK_AND_SET_COMMANDS);
  /* If we're on a valid command index, execute the current command and
   * register a callback that will execute the next command by incrementing the
   * request_index. */
  int status = REDIS_OK;
  ++requests_info->request_index;
  if (request_index == CHECK_AND_SET_SETNX_INDEX) {
    /* Atomically set the object hash and get the previous value to compare to
     * our hash, if a previous value existed. */
    requests_info->is_redis_reply = true;
    status =
        redisAsyncCommand(db->context, redis_object_table_add_callback,
                          (void *) requests_info, "SETNX objhash:%b %b", id.id,
                          sizeof(object_id), digest, (size_t) DIGEST_SIZE);
  } else if (request_index == CHECK_AND_SET_GET_INDEX) {
    /* If there was an object hash in the table previously, check that it's
     * equal to ours. */
    CHECKM(reply->type == REDIS_REPLY_INTEGER,
           "Expected Redis integer, received type %d %s", reply->type,
           reply->str);
    CHECKM(reply->integer == 0 || reply->integer == 1,
           "Expected 0 or 1 from REDIS, received %lld", reply->integer);
    if (reply->integer == 1) {
      requests_info->is_redis_reply = false;
      redis_object_table_add_callback(c, reply, (void *) requests_info);
    } else {
      requests_info->is_redis_reply = true;
      status = redisAsyncCommand(db->context, redis_object_table_add_callback,
                                 (void *) requests_info, "GET objhash:%b",
                                 id.id, sizeof(object_id));
    }
  } else if (request_index == CHECK_AND_SET_SADD_INDEX) {
    if (requests_info->is_redis_reply) {
      CHECKM(reply->type == REDIS_REPLY_STRING,
             "Expected Redis string, received type %d %s", reply->type,
             reply->str);
      DCHECK(reply->len == DIGEST_SIZE);
      if (memcmp(digest, reply->str, reply->len) != 0) {
        /* If our object hash doesn't match the one recorded in the table,
         * report the error back to the user and exit immediately. */
        LOG_FATAL("Object hash collision while adding manager");
      }
    }
    /* Add ourselves to the object's locations. */
    requests_info->is_redis_reply = true;
    status = redisAsyncCommand(db->context, redis_object_table_add_callback,
                               (void *) requests_info, "SADD obj:%b %b", id.id,
                               sizeof(id.id), (char *) db->client.id,
                               sizeof(db->client.id));
  } else {
    /* We finished executing all the Redis commands for this attempt at the
     * table operation. */
    free(requests_info);
    /* If the transaction failed, exit and let the table operation's timout
     * handler handle it. */
    if (reply->type == REDIS_REPLY_NIL) {
      return;
    }
    /* Else, call the done callback and clean up the table state. */
    if (callback_data->done_callback) {
      task_table_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  }
  /* If there was an error executing the current command, this attempt was a
   * failure, so clean up the request info. */
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "could not add object_table entry");
    free(requests_info);
  }
}

void redis_object_table_add(table_callback_data *callback_data) {
  CHECK(callback_data);
  LOG_DEBUG("Calling object table add");
  redis_requests_info *requests_info = malloc(sizeof(redis_requests_info));
  requests_info->timer_id = callback_data->timer_id;
  requests_info->request_index = 0;
  requests_info->is_redis_reply = false;
  db_handle *db = callback_data->db_handle;
  redis_object_table_add_callback(db->context, NULL, (void *) requests_info);
}

void redis_object_table_lookup(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;

  /* Call redis asynchronously */
  object_id id = callback_data->id;
  int status = redisAsyncCommand(db->context, redis_object_table_get_entry,
                                 (void *) callback_data->timer_id,
                                 "SMEMBERS obj:%b", id.id, sizeof(id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in object_table lookup");
  }
}

void redis_result_table_add_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;
  CHECK(reply->type == REDIS_REPLY_STATUS ||
        reply->type == REDIS_REPLY_INTEGER);
  if (callback_data->done_callback) {
    result_table_done_callback done_callback = callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_result_table_add(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  object_id id = callback_data->id;
  task_id *result_task_id = (task_id *) callback_data->data;
  /* Add the result entry to the result table. */
  int status = redisAsyncCommand(
      db->context, redis_result_table_add_callback,
      (void *) callback_data->timer_id, "SET result:%b %b", id.id,
      sizeof(id.id), result_task_id->id, sizeof(result_task_id->id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "Error in result table add");
  }
}

void redis_result_table_lookup_task_callback(redisAsyncContext *c,
                                             void *r,
                                             void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;
  /* Check that we received a Redis hashmap. */
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_FATAL("Expected Redis array, received type %d %s", reply->type,
              reply->str);
  }
  /* If the user registered a success callback, construct the task object from
   * the Redis reply and call the callback. */
  result_table_lookup_callback done_callback = callback_data->done_callback;
  task_id *result_task_id = callback_data->data;
  if (done_callback) {
    task *task_reply = parse_redis_task_table_entry(
        *result_task_id, reply->elements, reply->element);
    done_callback(callback_data->id, task_reply, callback_data->user_context);
    free_task(task_reply);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_result_table_lookup_object_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  if (reply->type == REDIS_REPLY_STRING) {
    /* If we found the object, get the spec of the task that created it. */
    DCHECK(reply->len == sizeof(task_id));
    task_id *result_task_id = malloc(sizeof(task_id));
    memcpy(result_task_id, reply->str, reply->len);
    callback_data->data = (void *) result_task_id;
    int status =
        redisAsyncCommand(db->context, redis_result_table_lookup_task_callback,
                          (void *) callback_data->timer_id, "HGETALL task:%b",
                          result_task_id->id, sizeof(result_task_id->id));
    if ((status == REDIS_ERR) || db->context->err) {
      LOG_REDIS_DEBUG(db->context, "Could not look up result table entry");
    }
  } else if (reply->type == REDIS_REPLY_NIL) {
    /* The object with the requested ID was not in the table. */
    LOG_INFO("Object's result not in table.");
    result_table_lookup_callback done_callback = callback_data->done_callback;
    if (done_callback) {
      done_callback(callback_data->id, NULL, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
    return;
  } else {
    LOG_FATAL("expected string or nil, received type %d", reply->type);
  }
}

void redis_result_table_lookup(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  /* First, lookup the ID of the task that created this object. */
  object_id id = callback_data->id;
  int status = redisAsyncCommand(
      db->context, redis_result_table_lookup_object_callback,
      (void *) callback_data->timer_id, "GET result:%b", id.id, sizeof(id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "Error in result table lookup");
  }
}

/**
 * Get an entry from the plasma manager table in redis.
 *
 * @param db The database handle.
 * @param index The index of the plasma manager.
 * @param manager The pointer where the IP address of the manager gets written.
 * @return Void.
 */
void redis_get_cached_db_client(db_handle *db,
                                db_client_id db_client_id,
                                const char **manager) {
  db_client_cache_entry *entry;
  HASH_FIND(hh, db->db_client_cache, &db_client_id, sizeof(db_client_id),
            entry);
  if (!entry) {
    /* This is a very rare case. It should happen at most once per db client. */
    redisReply *reply =
        redisCommand(db->sync_context, "HGET db_clients:%b address",
                     (char *) db_client_id.id, sizeof(db_client_id.id));
    CHECK(reply->type == REDIS_REPLY_STRING);
    entry = malloc(sizeof(db_client_cache_entry));
    entry->db_client_id = db_client_id;
    entry->addr = strdup(reply->str);
    HASH_ADD(hh, db->db_client_cache, db_client_id, sizeof(db_client_id),
             entry);
    freeReplyObject(reply);
  }
  *manager = entry->addr;
}

void redis_object_table_get_entry(redisAsyncContext *c,
                                  void *r,
                                  void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  db_client_id *managers = malloc(reply->elements * sizeof(db_client_id));
  int64_t manager_count = reply->elements;

  if (reply->type == REDIS_REPLY_ARRAY) {
    const char **manager_vector = NULL;
    if (manager_count > 0) {
      manager_vector = malloc(manager_count * sizeof(char *));
    }
    for (int j = 0; j < reply->elements; ++j) {
      CHECK(reply->element[j]->type == REDIS_REPLY_STRING);
      memcpy(managers[j].id, reply->element[j]->str, sizeof(managers[j].id));
      redis_get_cached_db_client(db, managers[j], manager_vector + j);
    }
    object_table_lookup_done_callback done_callback =
        callback_data->done_callback;
    if (done_callback) {
      done_callback(callback_data->id, manager_count, manager_vector,
                    callback_data->user_context);
    }

    if (callback_data->data != NULL) {
      /* This callback was called from a subscribe call. */
      object_table_subscribe_data *sub_data = callback_data->data;
      object_table_object_available_callback sub_callback =
          sub_data->object_available_callback;
      if (manager_count > 0) {
        if (sub_callback) {
          sub_callback(callback_data->id, manager_count, manager_vector,
                       sub_data->subscribe_context);
        }
      }
      /* For the subscribe, don't delete the callback, only the timer. */
      event_loop_remove_timer(callback_data->db_handle->loop,
                              callback_data->timer_id);
    } else {
      /* This callback was called from a publish call. */
      /* For the lookup, remove timer and callback handler. */
      destroy_timer_callback(callback_data->db_handle->loop, callback_data);
    }

    if (manager_count > 0) {
      free(manager_vector);
    }
  } else {
    LOG_FATAL("expected integer or string, received type %d", reply->type);
  }
  free(managers);
}

void object_table_redis_subscribe_callback(redisAsyncContext *c,
                                           void *r,
                                           void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* First entry is message type, second is topic, third is payload. */
  CHECK(reply->elements > 2);

  /* Do a lookup for the actual data. */
  int status =
      redisAsyncCommand(db->context, redis_object_table_get_entry,
                        (void *) callback_data->timer_id, "SMEMBERS obj:%b",
                        callback_data->id.id, sizeof(callback_data->id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_ERROR(db->context,
                    "error in redis_object_table_subscribe_callback");
  }
}

void redis_object_table_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;

  /* subscribe to key notification associated to object id */
  object_id id = callback_data->id;
  int status = redisAsyncCommand(
      db->sub_context, object_table_redis_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE __keyspace@0__:obj:%b",
      id.id, sizeof(id.id));
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in redis_object_table_subscribe_callback");
  }
}

/*
 *  ==== task_table callbacks ====
 */

void redis_task_table_get_task_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;
  /* Check that we received a Redis hashmap. */
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_FATAL("Expected Redis array, received type %d %s", reply->type,
              reply->str);
  }
  /* If the user registered a success callback, construct the task object from
   * the Redis reply and call the callback. */
  if (callback_data->done_callback) {
    task_table_get_callback done_callback = callback_data->done_callback;
    task *task_reply = parse_redis_task_table_entry(
        callback_data->id, reply->elements, reply->element);
    done_callback(task_reply, callback_data->user_context);
    free_task(task_reply);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_get_task(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;
  task_id id = callback_data->id;
  int status =
      redisAsyncCommand(db->context, redis_task_table_get_task_callback,
                        (void *) callback_data->timer_id, "HGETALL task:%b",
                        id.id, sizeof(id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "Could not get task from task table");
  }
}

void redis_task_table_publish(table_callback_data *callback_data,
                              bool task_added) {
  db_handle *db = callback_data->db_handle;
  task *task = callback_data->data;
  task_id id = task_task_id(task);
  node_id node = task_node(task);
  scheduling_state state = task_state(task);
  task_spec *spec = task_task_spec(task);

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
#define NUM_PUBLISH_COMMANDS 2
#define PUBLISH_PUSH_INDEX 0
#define PUBLISH_PUBLISH_INDEX 1
  if (callback_data->requests_info == NULL) {
    callback_data->requests_info = malloc(NUM_PUBLISH_COMMANDS * sizeof(bool));
    for (int i = 0; i < NUM_PUBLISH_COMMANDS; i++) {
      ((bool *) callback_data->requests_info)[i] = false;
    }
  }

  if (((bool *) callback_data->requests_info)[PUBLISH_PUSH_INDEX] == false) {
    /* If the task has already been added to the task table, only update the
     * scheduling information fields. */
    int status = REDIS_OK;
    if (task_added) {
      status = redisAsyncCommand(
          db->context, redis_task_table_publish_push_callback,
          (void *) callback_data->timer_id, "HMSET task:%b state %d node %b",
          (char *) id.id, sizeof(id.id), state, (char *) node.id,
          sizeof(node.id));
    } else {
      status = redisAsyncCommand(
          db->context, redis_task_table_publish_push_callback,
          (void *) callback_data->timer_id,
          "HMSET task:%b state %d node %b task_spec %b", (char *) id.id,
          sizeof(id.id), state, (char *) node.id, sizeof(node.id),
          (char *) spec, task_spec_size(spec));
    }
    if ((status = REDIS_ERR) || db->context->err) {
      LOG_REDIS_DEBUG(db->context, "error setting task in task_table_add_task");
    }
  }

  if (((bool *) callback_data->requests_info)[PUBLISH_PUBLISH_INDEX] == false) {
    int status = redisAsyncCommand(
        db->context, redis_task_table_publish_publish_callback,
        (void *) callback_data->timer_id, "PUBLISH task:%b:%d %b",
        (char *) node.id, sizeof(node.id), state, (char *) task,
        task_size(task));

    if ((status == REDIS_ERR) || db->context->err) {
      LOG_REDIS_DEBUG(db->context,
                      "error publishing task in task_table_add_task");
    }
  }
}

void redis_task_table_add_task(table_callback_data *callback_data) {
  redis_task_table_publish(callback_data, false);
}

void redis_task_table_update(table_callback_data *callback_data) {
  redis_task_table_publish(callback_data, true);
}

void redis_task_table_publish_push_callback(redisAsyncContext *c,
                                            void *r,
                                            void *privdata) {
  LOG_DEBUG("Calling publish push callback");
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  CHECK(callback_data->requests_info != NULL);
  ((bool *) callback_data->requests_info)[PUBLISH_PUSH_INDEX] = true;

  if (((bool *) callback_data->requests_info)[PUBLISH_PUBLISH_INDEX] == true) {
    if (callback_data->done_callback) {
      task_table_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  }
}

void redis_task_table_publish_publish_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata) {
  LOG_DEBUG("Calling publish publish callback");
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  CHECK(callback_data->requests_info != NULL);
  ((bool *) callback_data->requests_info)[PUBLISH_PUBLISH_INDEX] = true;

  if (((bool *) callback_data->requests_info)[PUBLISH_PUSH_INDEX] == true) {
    if (callback_data->done_callback) {
      task_table_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    destroy_timer_callback(db->loop, callback_data);
  }
}

void redis_task_table_subscribe_callback(redisAsyncContext *c,
                                         void *r,
                                         void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
  CHECK(reply->elements > 2);
  /* First entry is message type, then possibly the regex we psubscribed to,
   * then topic, then payload. */
  redisReply *payload = reply->element[reply->elements - 1];
  if (payload->str == NULL) {
    if (callback_data->done_callback) {
      task_table_done_callback done_callback = callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the task and call the callback. */
  task_table_subscribe_data *data = callback_data->data;

  task *task = malloc(payload->len);
  memcpy(task, payload->str, payload->len);
  if (data->subscribe_callback) {
    data->subscribe_callback(task, data->subscribe_context);
  }
  free_task(task);
}

void redis_task_table_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  task_table_subscribe_data *data = callback_data->data;
  int status = REDIS_OK;
  if (IS_NIL_ID(data->node)) {
    /* TODO(swang): Implement the state_filter by translating the bitmask into
     * a Redis key-matching pattern. */
    status =
        redisAsyncCommand(db->sub_context, redis_task_table_subscribe_callback,
                          (void *) callback_data->timer_id,
                          "PSUBSCRIBE task:*:%d", data->state_filter);
  } else {
    node_id node = data->node;
    status = redisAsyncCommand(
        db->sub_context, redis_task_table_subscribe_callback,
        (void *) callback_data->timer_id, "SUBSCRIBE task:%b:%d",
        (char *) node.id, sizeof(node.id), data->state_filter);
  }
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in task_table_register_callback");
  }
}

/*
 *  ==== db client table callbacks ====
 */

void redis_db_client_table_subscribe_callback(redisAsyncContext *c,
                                              void *r,
                                              void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
  CHECK(reply->elements > 2);
  /* First entry is message type, then possibly the regex we psubscribed to,
   * then topic, then payload. */
  redisReply *payload = reply->element[reply->elements - 1];
  if (payload->str == NULL) {
    if (callback_data->done_callback) {
      db_client_table_done_callback done_callback =
          callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the payload and call the callback. */
  db_client_table_subscribe_data *data = callback_data->data;
  db_client_id client;
  memcpy(client.id, payload->str, sizeof(client.id));
  /* We subtract 1 + sizeof(client.id) to compute the length of the
   * client_type string, and we add 1 to null-terminate the string. */
  int client_type_length = payload->len - 1 - sizeof(client.id) + 1;
  char *client_type = malloc(client_type_length);
  memcpy(client_type, &payload->str[1 + sizeof(client.id)], client_type_length);
  if (data->subscribe_callback) {
    data->subscribe_callback(client, client_type, data->subscribe_context);
  }
  free(client_type);
}

void redis_db_client_table_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_db_client_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE db_clients");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in db_client_table_register_callback");
  }
}

db_client_id get_db_client_id(db_handle *db) {
  CHECK(db != NULL);
  return db->client;
}
