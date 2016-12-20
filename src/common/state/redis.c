/* Redis implementation of the global state store */

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
/* Including hiredis here is necessary on Windows for typedefs used in ae.h. */
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
#include "utstring.h"

#include "common.h"
#include "db.h"
#include "db_client_table.h"
#include "object_table.h"
#include "object_info.h"
#include "task.h"
#include "task_table.h"
#include "event_loop.h"
#include "redis.h"
#include "io.h"

#ifndef _WIN32
/* This function is actually not declared in standard POSIX, so declare it. */
extern int usleep(useconds_t usec);
#endif

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

/**
 * A data structure to keep track of object IDs when doing object table
 * lookups.
 * TODO(swang): Remove this when we integrate a Redis module implementation.
 */
typedef struct {
  /** The timer ID that uniquely identifies this table operation. All retry
   *  attempts of a table operation share the same timer ID. */
  int64_t timer_id;
  /** The object ID that the request was for. */
  object_id object_id;
} object_table_get_entry_info;

db_handle *db_connect(const char *address,
                      int port,
                      const char *client_type,
                      const char *client_addr,
                      int client_port) {
  return db_connect_extended(address, port, client_type, client_addr,
                             client_port, ":");
}

db_handle *db_connect_extended(const char *address,
                               int port,
                               const char *client_type,
                               const char *client_addr,
                               int client_port,
                               const char *aux_address) {
  db_handle *db = malloc(sizeof(db_handle));
  /* Sync connection for initial handshake */
  redisReply *reply;
  int connection_attempts = 0;
  redisContext *context = redisConnect(address, port);
  /* Sanity check aux_address. */
  if (aux_address == NULL || strlen(aux_address) == 0) {
    LOG_WARN("db_connect: received empty aux_address, replacing with ':'");
    aux_address = ":";
  }
  while (context == NULL || context->err) {
    if (connection_attempts >= REDIS_DB_CONNECT_RETRIES) {
      break;
    }
    LOG_WARN("Failed to connect to Redis, retrying.");
    /* Sleep for a little. */
    usleep(REDIS_DB_CONNECT_WAIT_MS * 1000);
    context = redisConnect(address, port);
    connection_attempts += 1;
  }
  CHECK_REDIS_CONNECT(redisContext, context,
                      "could not establish synchronous connection to redis "
                      "%s:%d",
                      address, port);
  /* Enable keyspace events. */
  reply = redisCommand(context, "CONFIG SET notify-keyspace-events AKE");
  CHECKM(reply != NULL, "db_connect failed on CONFIG SET");
  freeReplyObject(reply);
  /* Add new client using optimistic locking. */
  db_client_id client = globally_unique_id();

  /* Register this client with Redis. RAY.CONNECT is a custom Redis command that
   * we've defined. */
  reply = redisCommand(context, "RAY.CONNECT %s %s:%d %b %s", client_type,
                       client_addr, client_port, (char *) client.id,
                       sizeof(client.id), aux_address);
  CHECKM(reply != NULL, "db_connect failed on RAY.CONNECT");
  freeReplyObject(reply);

  db->client_type = strdup(client_type);
  db->client = client;
  db->db_client_cache = NULL;
  db->sync_context = context;

  /* Establish async connection */
  db->context = redisAsyncConnect(address, port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->context,
                      "could not establish asynchronous connection to redis "
                      "%s:%d",
                      address, port);
  db->context->data = (void *) db;
  /* Establish async connection for subscription */
  db->sub_context = redisAsyncConnect(address, port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->sub_context,
                      "could not establish asynchronous subscription "
                      "connection to redis %s:%d",
                      address, port);
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
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = r;
  if (strcmp(reply->str, "hash mismatch") == 0) {
    /* If our object hash doesn't match the one recorded in the table, report
     * the error back to the user and exit immediately. */
    LOG_FATAL(
        "Found objects with different value but same object ID, most likely "
        "because a nondeterministic task was executed twice, either for "
        "reconstruction or for speculation.");
  }
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);
  /* Call the done callback if there is one. */
  if (callback_data->done_callback != NULL) {
    task_table_done_callback done_callback = callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_add(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;

  object_table_add_data *info = callback_data->data;
  object_id obj_id = callback_data->id;
  int64_t object_size = info->object_size;
  unsigned char *digest = info->digest;

  int status = redisAsyncCommand(
      db->context, redis_object_table_add_callback,
      (void *) callback_data->timer_id, "RAY.OBJECT_TABLE_ADD %b %ld %b %b",
      obj_id.id, sizeof(obj_id.id), object_size, digest, (size_t) DIGEST_SIZE,
      db->client.id, sizeof(db->client.id));

  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_object_table_add");
  }
}

void redis_object_table_lookup(table_callback_data *callback_data) {
  CHECK(callback_data);
  db_handle *db = callback_data->db_handle;

  object_id obj_id = callback_data->id;
  // object_table_get_entry_info *context =
  //     malloc(sizeof(object_table_get_entry_info));
  // context->timer_id = callback_data->timer_id;
  // context->object_id = id;

  int status = redisAsyncCommand(
      db->context, redis_object_table_lookup_callback,
      (void *) callback_data->timer_id, "RAY.OBJECT_TABLE_LOOKUP %b", obj_id.id,
      sizeof(obj_id.id));
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
        redisCommand(db->sync_context, "RAY.GET_CLIENT_ADDRESS %b",
                     (char *) db_client_id.id, sizeof(db_client_id.id));
    CHECKM(reply->type == REDIS_REPLY_STRING, "REDIS reply type=%d",
           reply->type);
    entry = malloc(sizeof(db_client_cache_entry));
    entry->db_client_id = db_client_id;
    entry->addr = strdup(reply->str);
    HASH_ADD(hh, db->db_client_cache, db_client_id, sizeof(db_client_id),
             entry);
    freeReplyObject(reply);
  }
  *manager = entry->addr;
}

void redis_object_table_lookup_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  object_id obj_id = callback_data->id;

  LOG_DEBUG("Object table lookup callback");
  CHECK(reply->type == REDIS_REPLY_ARRAY);

  int64_t manager_count = reply->elements;
  db_client_id *managers = NULL;
  const char **manager_vector = NULL;
  if (manager_count > 0) {
    managers = malloc(reply->elements * sizeof(db_client_id));
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
    done_callback(obj_id, manager_count, manager_vector,
                  callback_data->user_context);
  }

  /* Clean up timer and callback. */
  destroy_timer_callback(callback_data->db_handle->loop, callback_data);
  if (manager_count > 0) {
    free(managers);
    free(manager_vector);
  }
}

/**
 * This will parse a payload string published on the object notification
 * channel. The string must have the format:
 *
 *    <object id> MANAGERS <manager id1> <manager id2> ...
 *
 * where there may be any positive number of manager IDs.
 *
 * @param db The db handle.
 * @param payload The payload string.
 * @param length The length of the string.
 * @param manager_count This method will write the number of managers at this
 *        address.
 * @param manager_vector This method will allocate an array of pointers to
 *        manager addresses and write the address of the array at this address.
 *        The caller is responsible for freeing this array.
 * @return The object ID that the notification is about.
 */
object_id parse_subscribe_to_notifications_payload(
    db_handle *db,
    char *payload,
    int length,
    int64_t *data_size,
    int *manager_count,
    const char ***manager_vector) {
  long long data_size_value = 0;
  int num_managers = (length - sizeof(object_id) - 1 - sizeof(data_size_value) -
                      1 - strlen("MANAGERS")) /
                     (1 + sizeof(db_client_id));
  CHECK(length ==
        sizeof(object_id) + 1 + sizeof(data_size_value) + 1 +
            strlen("MANAGERS") + num_managers * (1 + sizeof(db_client_id)));
  CHECK(num_managers > 0);
  object_id obj_id;
  /* Track our current offset in the payload. */
  int offset = 0;
  /* Parse the object ID. */
  memcpy(&obj_id.id, &payload[offset], sizeof(obj_id.id));
  offset += sizeof(obj_id.id);
  /* The next part of the payload is a space. */
  char *space_str = " ";
  CHECK(memcmp(&payload[offset], space_str, strlen(space_str)) == 0);
  offset += strlen(space_str);
  /* The next part of the payload is binary data_size. */
  memcpy(&data_size_value, &payload[offset], sizeof(data_size_value));
  offset += sizeof(data_size_value);
  /* The next part of the payload is the string " MANAGERS" with leading ' '. */
  char *managers_str = " MANAGERS";
  CHECK(memcmp(&payload[offset], managers_str, strlen(managers_str)) == 0);
  offset += strlen(managers_str);
  /* Parse the managers. */
  const char **managers = malloc(num_managers * sizeof(char *));
  for (int i = 0; i < num_managers; ++i) {
    /* First there is a space. */
    CHECK(memcmp(&payload[offset], " ", strlen(" ")) == 0);
    offset += strlen(" ");
    /* Get the manager ID. */
    db_client_id manager_id;
    memcpy(&manager_id.id, &payload[offset], sizeof(manager_id.id));
    offset += sizeof(manager_id.id);
    /* Write the address of the corresponding manager to the returned array. */
    redis_get_cached_db_client(db, manager_id, &managers[i]);
  }
  CHECK(offset == length);
  /* Return the manager array and the object ID. */
  *manager_count = num_managers;
  *manager_vector = managers;
  *data_size = data_size_value;
  return obj_id;
}

void object_table_redis_subscribe_to_notifications_callback(
    redisAsyncContext *c,
    void *r,
    void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Replies to the SUBSCRIBE command have 3 elements. There are two
   * possibilities. Either the reply is the initial acknowledgment of the
   * subscribe command, or it is a message. If it is the initial acknowledgment,
   * then
   *     - reply->element[0]->str is "subscribe"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is null.
   * If it is an actual message, then
   *     - reply->element[0]->str is "message"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is the contents of the message.
   */
  redisReply *reply = r;
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  redisReply *message_type = reply->element[0];
  LOG_DEBUG("Object table subscribe to notifications callback, message %s",
            message_type->str);

  if (strcmp(message_type->str, "message") == 0) {
    /* Handle an object notification. */
    int64_t data_size = 0;
    int manager_count;
    const char **manager_vector;
    object_id obj_id = parse_subscribe_to_notifications_payload(
        db, reply->element[2]->str, reply->element[2]->len, &data_size,
        &manager_count, &manager_vector);
    /* Call the subscribe callback. */
    object_table_subscribe_data *data = callback_data->data;
    if (data->object_available_callback) {
      data->object_available_callback(obj_id, data_size, manager_count,
                                      manager_vector, data->subscribe_context);
    }
    free(manager_vector);
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    /* Call the done callback if there is one. This code path should only be
     * used in the tests. */
    if (callback_data->done_callback != NULL) {
      object_table_lookup_done_callback done_callback =
          callback_data->done_callback;
      done_callback(NIL_ID, 0, NULL, callback_data->user_context);
    }
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    event_loop_remove_timer(callback_data->db_handle->loop,
                            callback_data->timer_id);
  } else {
    LOG_FATAL(
        "Unexpected reply type from object table subscribe to notifications.");
  }
}

void redis_object_table_subscribe_to_notifications(
    table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  /* The object channel prefix must match the value defined in
   * src/common/redismodule/ray_redis_module.c. */
  const char *object_channel_prefix = "OC:";
  const char *object_channel_bcast = "BCAST";
  int status = REDIS_OK;
  /* Subscribe to notifications from the object table. This uses the client ID
   * as the channel name so this channel is specific to this client. TODO(rkn):
   * The channel name should probably be the client ID with some prefix. */
  CHECKM(callback_data->data != NULL,
         "Object table subscribe data passed as NULL.");
  if (((object_table_subscribe_data *) (callback_data->data))->subscribe_all) {
    /* Subscribe to the object broadcast channel. */
    status = redisAsyncCommand(
        db->sub_context, object_table_redis_subscribe_to_notifications_callback,
        (void *) callback_data->timer_id, "SUBSCRIBE %s%s",
        object_channel_prefix, object_channel_bcast);
  } else {
    status = redisAsyncCommand(
        db->sub_context, object_table_redis_subscribe_to_notifications_callback,
        (void *) callback_data->timer_id, "SUBSCRIBE %s%b",
        object_channel_prefix, db->client.id, sizeof(db->client.id));
  }

  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in redis_object_table_subscribe_to_notifications");
  }
}

void redis_object_table_request_notifications_callback(redisAsyncContext *c,
                                                       void *r,
                                                       void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = r;
  CHECK(strcmp(reply->str, "OK") == 0);
  CHECK(callback_data->done_callback == NULL);
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_request_notifications(
    table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;

  object_table_request_notifications_data *request_data = callback_data->data;
  int num_object_ids = request_data->num_object_ids;
  object_id *object_ids = request_data->object_ids;

  /* Create the arguments for the Redis command. */
  int num_args = 1 + 1 + num_object_ids;
  const char **argv = malloc(sizeof(char *) * num_args);
  size_t *argvlen = malloc(sizeof(size_t) * num_args);
  /* Set the command name argument. */
  argv[0] = "RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS";
  argvlen[0] = strlen(argv[0]);
  /* Set the client ID argument. */
  argv[1] = (char *) db->client.id;
  argvlen[1] = sizeof(db->client.id);
  /* Set the object ID arguments. */
  for (int i = 0; i < num_object_ids; ++i) {
    argv[2 + i] = (char *) object_ids[i].id;
    argvlen[2 + i] = sizeof(object_ids[i].id);
  }

  int status = redisAsyncCommandArgv(
      db->context, redis_object_table_request_notifications_callback,
      (void *) callback_data->timer_id, num_args, argv, argvlen);
  free(argv);
  free(argvlen);

  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context,
                    "error in redis_object_table_subscribe_to_notifications");
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
    if ((status == REDIS_ERR) || db->context->err) {
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
  CHECK(reply->elements > 2);
  /* First entry is message type, then possibly the regex we psubscribed to,
   * then topic, then payload. */
  redisReply *payload = reply->element[reply->elements - 1];
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
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
  CHECK(reply->elements > 2);
  /* First entry is message type, then possibly the regex we psubscribed to,
   * then topic, then payload. */
  redisReply *payload = reply->element[reply->elements - 1];
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
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
  char *aux_address = malloc(client_type_length);
  memset(aux_address, 0, client_type_length);
  /* Published message format: <client_id:client_type aux_addr> */
  int rv = sscanf(&payload->str[1 + sizeof(client.id)], "%s %s", client_type,
                  aux_address);
  CHECKM(rv == 2,
         "redis_db_client_table_subscribe_callback: expected 2 parsed args, "
         "Got %d instead.",
         rv);
  if (data->subscribe_callback) {
    data->subscribe_callback(client, client_type, aux_address,
                             data->subscribe_context);
  }
  free(client_type);
  free(aux_address);
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

void redis_object_info_subscribe_callback(redisAsyncContext *c,
                                          void *r,
                                          void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);

  CHECK(reply->elements > 2);
  /* First entry is message type, then possibly the regex we psubscribed to,
   * then topic, then payload. */
  redisReply *payload = reply->element[reply->elements - 1];
  /* If this condition is true, we got the initial message that acknowledged the
   * subscription. */
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
  object_info_subscribe_data *data = callback_data->data;
  object_id object_id;
  memcpy(object_id.id, payload->str, sizeof(object_id.id));
  /* payload->str should have the format: "object_id:object_size_int" */
  LOG_DEBUG("obj:info channel received message <%s>", payload->str);
  if (data->subscribe_callback) {
    data->subscribe_callback(
        object_id, strtol(&payload->str[1 + sizeof(object_id)], NULL, 10),
        data->subscribe_context);
  }
}

void redis_object_info_subscribe(table_callback_data *callback_data) {
  db_handle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_object_info_subscribe_callback,
      (void *) callback_data->timer_id, "PSUBSCRIBE obj:info");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in object_info_register_callback");
  }
}

db_client_id get_db_client_id(db_handle *db) {
  CHECK(db != NULL);
  return db->client;
}
