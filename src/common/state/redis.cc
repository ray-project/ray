/* Redis implementation of the global state store */

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>

extern "C" {
/* Including hiredis here is necessary on Windows for typedefs used in ae.h. */
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

#include "utstring.h"

#include "common.h"
#include "db.h"
#include "db_client_table.h"
#include "actor_notification_table.h"
#include "local_scheduler_table.h"
#include "object_table.h"
#include "task.h"
#include "task_table.h"
#include "error_table.h"
#include "event_loop.h"
#include "redis.h"
#include "io.h"

#include "format/common_generated.h"

#include "common_protocol.h"

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
#define REDIS_CALLBACK_HEADER(DB, CB_DATA, REPLY)                              \
  if ((REPLY) == NULL) {                                                       \
    return;                                                                    \
  }                                                                            \
  DBHandle *DB = (DBHandle *) c->data;                                         \
  TableCallbackData *CB_DATA = outstanding_callbacks_find((int64_t) privdata); \
  if (CB_DATA == NULL) {                                                       \
    /* the callback data structure has been                                    \
     * already freed; just ignore this reply */                                \
    return;                                                                    \
  }                                                                            \
  do {                                                                         \
  } while (0)

DBHandle *db_connect(const char *db_address,
                     int db_port,
                     const char *client_type,
                     const char *node_ip_address,
                     int num_args,
                     const char **args) {
  /* Check that the number of args is even. These args will be passed to the
   * RAY.CONNECT Redis command, which takes arguments in pairs. */
  if (num_args % 2 != 0) {
    LOG_FATAL("The number of extra args must be divisible by two.");
  }

  DBHandle *db = (DBHandle *) malloc(sizeof(DBHandle));
  /* Sync connection for initial handshake */
  redisReply *reply;
  int connection_attempts = 0;
  redisContext *context = redisConnect(db_address, db_port);
  while (context == NULL || context->err) {
    if (connection_attempts >= REDIS_DB_CONNECT_RETRIES) {
      break;
    }
    LOG_WARN("Failed to connect to Redis, retrying.");
    /* Sleep for a little. */
    usleep(REDIS_DB_CONNECT_WAIT_MS * 1000);
    context = redisConnect(db_address, db_port);
    connection_attempts += 1;
  }
  CHECK_REDIS_CONNECT(redisContext, context,
                      "could not establish synchronous connection to redis "
                      "%s:%d",
                      db_address, db_port);
  /* Configure Redis to generate keyspace notifications for list events. This
   * should only need to be done once (by whoever started Redis), but since
   * Redis may be started in multiple places (e.g., for testing or when starting
   * processes by hand), it is easier to do it multiple times. */
  reply = (redisReply *) redisCommand(context,
                                      "CONFIG SET notify-keyspace-events Kl");
  CHECKM(reply != NULL, "db_connect failed on CONFIG SET");
  freeReplyObject(reply);
  /* Also configure Redis to not run in protected mode, so clients on other
   * hosts can connect to it. */
  reply = (redisReply *) redisCommand(context, "CONFIG SET protected-mode no");
  CHECKM(reply != NULL, "db_connect failed on CONFIG SET");
  freeReplyObject(reply);
  /* Create a client ID for this client. */
  DBClientID client = globally_unique_id();

  /* Construct the argument arrays for RAY.CONNECT. */
  int argc = num_args + 4;
  const char **argv = (const char **) malloc(sizeof(char *) * argc);
  size_t *argvlen = (size_t *) malloc(sizeof(size_t) * argc);
  /* Set the command name argument. */
  argv[0] = "RAY.CONNECT";
  argvlen[0] = strlen(argv[0]);
  /* Set the client ID argument. */
  argv[1] = (char *) client.id;
  argvlen[1] = sizeof(db->client.id);
  /* Set the node IP address argument. */
  argv[2] = node_ip_address;
  argvlen[2] = strlen(node_ip_address);
  /* Set the client type argument. */
  argv[3] = client_type;
  argvlen[3] = strlen(client_type);
  /* Set the remaining arguments. */
  for (int i = 0; i < num_args; ++i) {
    if (args[i] == NULL) {
      LOG_FATAL("Element %d of the args array passed to db_connect was NULL.",
                i);
    }
    argv[4 + i] = args[i];
    argvlen[4 + i] = strlen(args[i]);
  }

  /* Register this client with Redis. RAY.CONNECT is a custom Redis command that
   * we've defined. */
  reply = (redisReply *) redisCommandArgv(context, argc, argv, argvlen);
  CHECKM(reply != NULL, "db_connect failed on RAY.CONNECT");
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);
  freeReplyObject(reply);
  free(argv);
  free(argvlen);

  db->client_type = strdup(client_type);
  db->client = client;
  db->db_client_cache = NULL;
  db->sync_context = context;

  /* Establish async connection */
  db->context = redisAsyncConnect(db_address, db_port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->context,
                      "could not establish asynchronous connection to redis "
                      "%s:%d",
                      db_address, db_port);
  db->context->data = (void *) db;
  /* Establish async connection for subscription */
  db->sub_context = redisAsyncConnect(db_address, db_port);
  CHECK_REDIS_CONNECT(redisAsyncContext, db->sub_context,
                      "could not establish asynchronous subscription "
                      "connection to redis %s:%d",
                      db_address, db_port);
  db->sub_context->data = (void *) db;

  return db;
}

void db_disconnect(DBHandle *db) {
  redisFree(db->sync_context);
  redisAsyncFree(db->context);
  redisAsyncFree(db->sub_context);
  DBClientCacheEntry *e, *tmp;
  HASH_ITER(hh, db->db_client_cache, e, tmp) {
    free(e->addr);
    HASH_DELETE(hh, db->db_client_cache, e);
    free(e);
  }
  free(db->client_type);
  free(db);
}

void db_attach(DBHandle *db, event_loop *loop, bool reattach) {
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

/*
 *  ==== object_table callbacks ====
 */

void redis_object_table_add_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = (redisReply *) r;
  bool success = (strcmp(reply->str, "hash mismatch") != 0);
  if (!success) {
    /* If our object hash doesn't match the one recorded in the table, report
     * the error back to the user and exit immediately. */
    LOG_WARN(
        "Found objects with different value but same object ID, most likely "
        "because a nondeterministic task was executed twice, either for "
        "reconstruction or for speculation.");
  } else {
    CHECK(reply->type != REDIS_REPLY_ERROR);
    CHECK(strcmp(reply->str, "OK") == 0);
  }
  /* Call the done callback if there is one. */
  if (callback_data->done_callback != NULL) {
    object_table_done_callback done_callback =
        (object_table_done_callback) callback_data->done_callback;
    done_callback(callback_data->id, success, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_add(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;

  ObjectTableAddData *info = (ObjectTableAddData *) callback_data->data;
  ObjectID obj_id = callback_data->id;
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

void redis_object_table_remove_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = (redisReply *) r;
  if (strcmp(reply->str, "object not found") == 0) {
    /* If our object entry was not in the table, it's probably a race
     * condition with an object_table_add. */
    return;
  }
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);
  /* Call the done callback if there is one. */
  if (callback_data->done_callback != NULL) {
    object_table_done_callback done_callback =
        (object_table_done_callback) callback_data->done_callback;
    done_callback(callback_data->id, true, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_remove(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;

  ObjectID obj_id = callback_data->id;
  /* If the caller provided a manager ID to delete, use it. Otherwise, use our
   * own client ID as the ID to delete. */
  DBClientID *client_id = (DBClientID *) callback_data->data;
  if (client_id == NULL) {
    client_id = &db->client;
  }
  int status = redisAsyncCommand(
      db->context, redis_object_table_remove_callback,
      (void *) callback_data->timer_id, "RAY.OBJECT_TABLE_REMOVE %b %b",
      obj_id.id, sizeof(obj_id.id), client_id->id, sizeof(client_id->id));

  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_object_table_remove");
  }
}

void redis_object_table_lookup(TableCallbackData *callback_data) {
  CHECK(callback_data);
  DBHandle *db = callback_data->db_handle;

  ObjectID obj_id = callback_data->id;
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
  redisReply *reply = (redisReply *) r;
  /* Check that the command succeeded. */
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECKM(strncmp(reply->str, "OK", strlen("OK")) == 0, "reply->str is %s",
         reply->str);
  /* Call the done callback if there is one. */
  if (callback_data->done_callback) {
    result_table_done_callback done_callback =
        (result_table_done_callback) callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  destroy_timer_callback(db->loop, callback_data);
}

void redis_result_table_add(TableCallbackData *callback_data) {
  CHECK(callback_data);
  DBHandle *db = callback_data->db_handle;
  ObjectID id = callback_data->id;
  ResultTableAddInfo *info = (ResultTableAddInfo *) callback_data->data;
  int is_put = info->is_put ? 1 : 0;

  /* Add the result entry to the result table. */
  int status = redisAsyncCommand(
      db->context, redis_result_table_add_callback,
      (void *) callback_data->timer_id, "RAY.RESULT_TABLE_ADD %b %b %d", id.id,
      sizeof(id.id), info->task_id.id, sizeof(info->task_id.id), is_put);
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "Error in result table add");
  }
}

/* This allocates a task which must be freed by the caller, unless the returned
 * task is NULL. This is used by both redis_result_table_lookup_callback and
 * redis_task_table_get_task_callback. */
Task *parse_and_construct_task_from_redis_reply(redisReply *reply) {
  Task *task;
  if (reply->type == REDIS_REPLY_NIL) {
    /* There is no task in the reply, so return NULL. */
    task = NULL;
  } else if (reply->type == REDIS_REPLY_STRING) {
    /* The reply is a flatbuffer TaskReply object. Parse it and construct the
     * task. */
    auto message = flatbuffers::GetRoot<TaskReply>(reply->str);
    TaskSpec *spec = (TaskSpec *) message->task_spec()->data();
    int64_t task_spec_size = message->task_spec()->size();
    task = Task_alloc(spec, task_spec_size, message->state(),
                      from_flatbuf(message->local_scheduler_id()));
  } else {
    LOG_FATAL("Unexpected reply type %d", reply->type);
  }
  /* Return the task. If it is not NULL, then it must be freed by the caller. */
  return task;
}

void redis_result_table_lookup_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;
  CHECKM(reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_STRING,
         "Unexpected reply type %d in redis_result_table_lookup_callback",
         reply->type);
  /* Parse the task from the reply. */
  TaskID result_id = NIL_TASK_ID;
  bool is_put = false;
  if (reply->type == REDIS_REPLY_STRING) {
    auto message = flatbuffers::GetRoot<ResultTableReply>(reply->str);
    result_id = from_flatbuf(message->task_id());
    is_put = message->is_put();
  }

  /* Call the done callback if there is one. */
  result_table_lookup_callback done_callback =
      (result_table_lookup_callback) callback_data->done_callback;
  if (done_callback != NULL) {
    done_callback(callback_data->id, result_id, is_put,
                  callback_data->user_context);
  }
  /* Clean up timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_result_table_lookup(TableCallbackData *callback_data) {
  CHECK(callback_data);
  DBHandle *db = callback_data->db_handle;
  ObjectID id = callback_data->id;
  int status =
      redisAsyncCommand(db->context, redis_result_table_lookup_callback,
                        (void *) callback_data->timer_id,
                        "RAY.RESULT_TABLE_LOOKUP %b", id.id, sizeof(id.id));
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
void redis_get_cached_db_client(DBHandle *db,
                                DBClientID db_client_id,
                                const char **manager) {
  DBClientCacheEntry *entry;
  HASH_FIND(hh, db->db_client_cache, &db_client_id, sizeof(db_client_id),
            entry);
  if (!entry) {
    /* This is a very rare case. It should happen at most once per db client. */
    redisReply *reply = (redisReply *) redisCommand(
        db->sync_context, "RAY.GET_CLIENT_ADDRESS %b", (char *) db_client_id.id,
        sizeof(db_client_id.id));
    CHECKM(reply->type == REDIS_REPLY_STRING, "REDIS reply type=%d, str=%s",
           reply->type, reply->str);
    entry = (DBClientCacheEntry *) malloc(sizeof(DBClientCacheEntry));
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
  redisReply *reply = (redisReply *) r;
  LOG_DEBUG("Object table lookup callback");
  CHECK(reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ARRAY);

  ObjectID obj_id = callback_data->id;
  int64_t manager_count = 0;
  DBClientID *managers = NULL;
  const char **manager_vector = NULL;

  /* Parse the Redis reply. */
  if (reply->type == REDIS_REPLY_NIL) {
    /* The object entry did not exist. */
    manager_count = -1;
  } else if (reply->type == REDIS_REPLY_ARRAY) {
    manager_count = reply->elements;
    if (manager_count > 0) {
      managers = (DBClientID *) malloc(reply->elements * sizeof(DBClientID));
      manager_vector = (const char **) malloc(manager_count * sizeof(char *));
    }
    for (int j = 0; j < reply->elements; ++j) {
      CHECK(reply->element[j]->type == REDIS_REPLY_STRING);
      memcpy(managers[j].id, reply->element[j]->str, sizeof(managers[j].id));
      redis_get_cached_db_client(db, managers[j], manager_vector + j);
    }
  } else {
    LOG_FATAL("Unexpected reply type from object table lookup.");
  }

  object_table_lookup_done_callback done_callback =
      (object_table_lookup_done_callback) callback_data->done_callback;
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
  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  redisReply *message_type = reply->element[0];
  LOG_DEBUG("Object table subscribe to notifications callback, message %s",
            message_type->str);

  if (strcmp(message_type->str, "message") == 0) {
    /* We received an object notification. Parse the payload. */
    auto message = flatbuffers::GetRoot<SubscribeToNotificationsReply>(
        reply->element[2]->str);
    /* Extract the object ID. */
    ObjectID obj_id = from_flatbuf(message->object_id());
    /* Extract the data size. */
    int64_t data_size = message->object_size();
    int manager_count = message->manager_ids()->size();
    /* Construct the manager vector from the flatbuffers object. */
    const char **manager_vector =
        (const char **) malloc(manager_count * sizeof(char *));
    for (int i = 0; i < manager_count; ++i) {
      DBClientID manager_id = from_flatbuf(message->manager_ids()->Get(i));
      redis_get_cached_db_client(db, manager_id, &manager_vector[i]);
    }

    /* Call the subscribe callback. */
    ObjectTableSubscribeData *data =
        (ObjectTableSubscribeData *) callback_data->data;
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
          (object_table_lookup_done_callback) callback_data->done_callback;
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
    TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  /* The object channel prefix must match the value defined in
   * src/common/redismodule/ray_redis_module.cc. */
  const char *object_channel_prefix = "OC:";
  const char *object_channel_bcast = "BCAST";
  int status = REDIS_OK;
  /* Subscribe to notifications from the object table. This uses the client ID
   * as the channel name so this channel is specific to this client. TODO(rkn):
   * The channel name should probably be the client ID with some prefix. */
  CHECKM(callback_data->data != NULL,
         "Object table subscribe data passed as NULL.");
  if (((ObjectTableSubscribeData *) (callback_data->data))->subscribe_all) {
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
  redisReply *reply = (redisReply *) r;
  CHECK(strcmp(reply->str, "OK") == 0);
  CHECK(callback_data->done_callback == NULL);
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_object_table_request_notifications(
    TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;

  ObjectTableRequestNotificationsData *request_data =
      (ObjectTableRequestNotificationsData *) callback_data->data;
  int num_object_ids = request_data->num_object_ids;
  ObjectID *object_ids = request_data->object_ids;

  /* Create the arguments for the Redis command. */
  int num_args = 1 + 1 + num_object_ids;
  const char **argv = (const char **) malloc(sizeof(char *) * num_args);
  size_t *argvlen = (size_t *) malloc(sizeof(size_t) * num_args);
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
  redisReply *reply = (redisReply *) r;
  /* Parse the task from the reply. */
  Task *task = parse_and_construct_task_from_redis_reply(reply);
  /* Call the done callback if there is one. */
  task_table_get_callback done_callback =
      (task_table_get_callback) callback_data->done_callback;
  if (done_callback != NULL) {
    done_callback(task, callback_data->user_context);
  }
  /* Free the task if it is not NULL. */
  Task_free(task);

  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_get_task(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  CHECK(callback_data->data == NULL);
  TaskID task_id = callback_data->id;

  int status = redisAsyncCommand(
      db->context, redis_task_table_get_task_callback,
      (void *) callback_data->timer_id, "RAY.TASK_TABLE_GET %b", task_id.id,
      sizeof(task_id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_task_table_get_task");
  }
}

void redis_task_table_add_task_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = (redisReply *) r;
  CHECKM(strcmp(reply->str, "OK") == 0, "reply->str is %s", reply->str);
  /* Call the done callback if there is one. */
  if (callback_data->done_callback != NULL) {
    task_table_done_callback done_callback =
        (task_table_done_callback) callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_add_task(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  Task *task = (Task *) callback_data->data;
  TaskID task_id = Task_task_id(task);
  DBClientID local_scheduler_id = Task_local_scheduler(task);
  int state = Task_state(task);
  TaskSpec *spec = Task_task_spec(task);

  CHECKM(task != NULL, "NULL task passed to redis_task_table_add_task.");
  int status = redisAsyncCommand(
      db->context, redis_task_table_add_task_callback,
      (void *) callback_data->timer_id, "RAY.TASK_TABLE_ADD %b %d %b %b",
      task_id.id, sizeof(task_id.id), state, local_scheduler_id.id,
      sizeof(local_scheduler_id.id), spec, Task_task_spec_size(task));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_task_table_add_task");
  }
}

void redis_task_table_update_callback(redisAsyncContext *c,
                                      void *r,
                                      void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  /* Do some minimal checking. */
  redisReply *reply = (redisReply *) r;
  CHECKM(strcmp(reply->str, "OK") == 0, "reply->str is %s", reply->str);
  /* Call the done callback if there is one. */
  if (callback_data->done_callback != NULL) {
    task_table_done_callback done_callback =
        (task_table_done_callback) callback_data->done_callback;
    done_callback(callback_data->id, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_update(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  Task *task = (Task *) callback_data->data;
  TaskID task_id = Task_task_id(task);
  DBClientID local_scheduler_id = Task_local_scheduler(task);
  int state = Task_state(task);

  CHECKM(task != NULL, "NULL task passed to redis_task_table_update.");
  int status = redisAsyncCommand(
      db->context, redis_task_table_update_callback,
      (void *) callback_data->timer_id, "RAY.TASK_TABLE_UPDATE %b %d %b",
      task_id.id, sizeof(task_id.id), state, local_scheduler_id.id,
      sizeof(local_scheduler_id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_task_table_update");
  }
}

void redis_task_table_test_and_update_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;
  /* Parse the task from the reply. */
  Task *task = parse_and_construct_task_from_redis_reply(reply);
  /* Determine whether the update happened. */
  auto message = flatbuffers::GetRoot<TaskReply>(reply->str);
  bool updated = message->updated();

  /* Call the done callback if there is one. */
  task_table_test_and_update_callback done_callback =
      (task_table_test_and_update_callback) callback_data->done_callback;
  if (done_callback != NULL) {
    done_callback(task, callback_data->user_context, updated);
  }
  /* Free the task if it is not NULL. */
  if (task != NULL) {
    Task_free(task);
  }
  /* Clean up timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_test_and_update(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  TaskID task_id = callback_data->id;
  TaskTableTestAndUpdateData *update_data =
      (TaskTableTestAndUpdateData *) callback_data->data;

  int status = redisAsyncCommand(
      db->context, redis_task_table_test_and_update_callback,
      (void *) callback_data->timer_id,
      "RAY.TASK_TABLE_TEST_AND_UPDATE %b %d %d %b", task_id.id,
      sizeof(task_id.id), update_data->test_state_bitmask,
      update_data->update_state, update_data->local_scheduler_id.id,
      sizeof(update_data->local_scheduler_id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in redis_task_table_test_and_update");
  }
}

void redis_task_table_subscribe_callback(redisAsyncContext *c,
                                         void *r,
                                         void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;

  CHECK(reply->type == REDIS_REPLY_ARRAY);
  /* The number of elements is 3 for a reply to SUBSCRIBE, and 4 for a reply to
   * PSUBSCRIBE. */
  CHECKM(reply->elements == 3 || reply->elements == 4, "reply->elements is %zu",
         reply->elements);
  /* The first element is the message type and the last entry is the payload.
   * The middle one or middle two elements describe the channel that was
   * published on. */
  redisReply *message_type = reply->element[0];
  redisReply *payload = reply->element[reply->elements - 1];
  if (strcmp(message_type->str, "message") == 0 ||
      strcmp(message_type->str, "pmessage") == 0) {
    /* Handle a task table event. Parse the payload and call the callback. */
    auto message = flatbuffers::GetRoot<TaskReply>(payload->str);
    /* Extract the task ID. */
    TaskID task_id = from_flatbuf(message->task_id());
    /* Extract the scheduling state. */
    int64_t state = message->state();
    /* Extract the local scheduler ID. */
    DBClientID local_scheduler_id = from_flatbuf(message->local_scheduler_id());
    /* Extract the task spec. */
    TaskSpec *spec = (TaskSpec *) message->task_spec()->data();
    int64_t task_spec_size = message->task_spec()->size();
    /* Create a task. */
    Task *task = Task_alloc(spec, task_spec_size, state, local_scheduler_id);

    /* Call the subscribe callback if there is one. */
    TaskTableSubscribeData *data =
        (TaskTableSubscribeData *) callback_data->data;
    if (data->subscribe_callback != NULL) {
      data->subscribe_callback(task, data->subscribe_context);
    }
    Task_free(task);
  } else if (strcmp(message_type->str, "subscribe") == 0 ||
             strcmp(message_type->str, "psubscribe") == 0) {
    /* If this condition is true, we got the initial message that acknowledged
     * the subscription. */
    if (callback_data->done_callback != NULL) {
      task_table_done_callback done_callback =
          (task_table_done_callback) callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
  } else {
    LOG_FATAL(
        "Unexpected reply type from task table subscribe. Message type is %s.",
        message_type->str);
  }
}

void redis_task_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  TaskTableSubscribeData *data = (TaskTableSubscribeData *) callback_data->data;
  /* TASK_CHANNEL_PREFIX is defined in ray_redis_module.cc and must be kept in
   * sync with that file. */
  const char *TASK_CHANNEL_PREFIX = "TT:";
  int status;
  if (IS_NIL_ID(data->local_scheduler_id)) {
    /* TODO(swang): Implement the state_filter by translating the bitmask into
     * a Redis key-matching pattern. */
    status =
        redisAsyncCommand(db->sub_context, redis_task_table_subscribe_callback,
                          (void *) callback_data->timer_id, "PSUBSCRIBE %s*:%d",
                          TASK_CHANNEL_PREFIX, data->state_filter);
  } else {
    DBClientID local_scheduler_id = data->local_scheduler_id;
    status =
        redisAsyncCommand(db->sub_context, redis_task_table_subscribe_callback,
                          (void *) callback_data->timer_id, "SUBSCRIBE %s%b:%d",
                          TASK_CHANNEL_PREFIX, (char *) local_scheduler_id.id,
                          sizeof(local_scheduler_id.id), data->state_filter);
  }
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in redis_task_table_subscribe");
  }
}

/*
 *  ==== db client table callbacks ====
 */

void redis_db_client_table_remove_callback(redisAsyncContext *c,
                                           void *r,
                                           void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;

  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);

  /* Call the done callback if there is one. */
  db_client_table_done_callback done_callback =
      (db_client_table_done_callback) callback_data->done_callback;
  if (done_callback) {
    done_callback(callback_data->id, callback_data->user_context);
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_db_client_table_remove(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status =
      redisAsyncCommand(db->context, redis_db_client_table_remove_callback,
                        (void *) callback_data->timer_id, "RAY.DISCONNECT %b",
                        callback_data->id.id, sizeof(callback_data->id.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context, "error in db_client_table_remove");
  }
}

void redis_db_client_table_subscribe_callback(redisAsyncContext *c,
                                              void *r,
                                              void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;

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
          (db_client_table_done_callback) callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the payload and call the callback. */
  auto message =
      flatbuffers::GetRoot<SubscribeToDBClientTableReply>(payload->str);
  DBClientID client = from_flatbuf(message->db_client_id());

  /* Parse the client type and auxiliary address from the response. If there is
   * only client type, then the update was a delete. */
  char *client_type = (char *) message->client_type()->data();
  char *aux_address = (char *) message->aux_address()->data();
  bool is_insertion = message->is_insertion();

  /* Call the subscription callback. */
  DBClientTableSubscribeData *data =
      (DBClientTableSubscribeData *) callback_data->data;
  if (data->subscribe_callback) {
    data->subscribe_callback(client, client_type, aux_address, is_insertion,
                             data->subscribe_context);
  }
}

void redis_db_client_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_db_client_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE db_clients");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in db_client_table_register_callback");
  }
}

void redis_local_scheduler_table_subscribe_callback(redisAsyncContext *c,
                                                    void *r,
                                                    void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  redisReply *message_type = reply->element[0];
  LOG_DEBUG("Local scheduler table subscribe callback, message %s",
            message_type->str);

  if (strcmp(message_type->str, "message") == 0) {
    /* Handle a local scheduler heartbeat. Parse the payload and call the
     * subscribe callback. */
    auto message =
        flatbuffers::GetRoot<LocalSchedulerInfoMessage>(reply->element[2]->str);

    /* Extract the client ID. */
    DBClientID client_id = from_flatbuf(message->db_client_id());
    /* Extract the fields of the local scheduler info struct. */
    LocalSchedulerInfo info;
    info.total_num_workers = message->total_num_workers();
    info.task_queue_length = message->task_queue_length();
    info.available_workers = message->available_workers();
    for (int i = 0; i < ResourceIndex_MAX; ++i) {
      info.static_resources[i] = message->static_resources()->Get(i);
    }
    for (int i = 0; i < ResourceIndex_MAX; ++i) {
      info.dynamic_resources[i] = message->dynamic_resources()->Get(i);
    }

    /* Call the subscribe callback. */
    LocalSchedulerTableSubscribeData *data =
        (LocalSchedulerTableSubscribeData *) callback_data->data;
    if (data->subscribe_callback) {
      data->subscribe_callback(client_id, info, data->subscribe_context);
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    CHECK(callback_data->done_callback == NULL);
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);

  } else {
    LOG_FATAL("Unexpected reply type from local scheduler subscribe.");
  }
}

void redis_local_scheduler_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_local_scheduler_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE local_schedulers");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in redis_local_scheduler_table_subscribe");
  }
}

void redis_local_scheduler_table_send_info_callback(redisAsyncContext *c,
                                                    void *r,
                                                    void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_INTEGER);
  LOG_DEBUG("%" PRId64 " subscribers received this publish.\n", reply->integer);

  CHECK(callback_data->done_callback == NULL);
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_local_scheduler_table_send_info(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  LocalSchedulerTableSendInfoData *data =
      (LocalSchedulerTableSendInfoData *) callback_data->data;

  /* Create a flatbuffer object to serialize and publish. */
  flatbuffers::FlatBufferBuilder fbb;
  /* Create the flatbuffers message. */
  LocalSchedulerInfo info = data->info;
  auto message = CreateLocalSchedulerInfoMessage(
      fbb, to_flatbuf(fbb, db->client), info.total_num_workers,
      info.task_queue_length, info.available_workers,
      fbb.CreateVector(info.static_resources, ResourceIndex_MAX),
      fbb.CreateVector(info.dynamic_resources, ResourceIndex_MAX));
  fbb.Finish(message);

  int status = redisAsyncCommand(
      db->context, redis_local_scheduler_table_send_info_callback,
      (void *) callback_data->timer_id, "PUBLISH local_schedulers %b",
      fbb.GetBufferPointer(), fbb.GetSize());
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context,
                    "error in redis_local_scheduler_table_send_info");
  }
}

void redis_plasma_manager_send_heartbeat(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  /* NOTE(swang): We purposefully do not provide a callback, leaving the table
   * operation and timer active. This allows us to send a new heartbeat every
   * HEARTBEAT_TIMEOUT_MILLISECONDS without having to allocate and deallocate
   * memory for callback data each time. */
  int status = redisAsyncCommand(
      db->context, NULL, (void *) callback_data->timer_id,
      "PUBLISH plasma_managers %b", db->client.id, sizeof(db->client.id));
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context,
                    "error in redis_plasma_manager_send_heartbeat");
  }
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_actor_notification_table_subscribe_callback(redisAsyncContext *c,
                                                       void *r,
                                                       void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  redisReply *message_type = reply->element[0];
  LOG_DEBUG("Local scheduler table subscribe callback, message %s",
            message_type->str);

  if (strcmp(message_type->str, "message") == 0) {
    /* Handle an actor notification message. Parse the payload and call the
     * subscribe callback. */
    redisReply *payload = reply->element[2];
    ActorNotificationTableSubscribeData *data =
        (ActorNotificationTableSubscribeData *) callback_data->data;
    ActorInfo info;
    /* The payload should be the concatenation of these two structs. */
    CHECK(sizeof(info.actor_id) + sizeof(info.local_scheduler_id) ==
          payload->len);
    memcpy(&info.actor_id, payload->str, sizeof(info.actor_id));
    memcpy(&info.local_scheduler_id, payload->str + sizeof(info.actor_id),
           sizeof(info.local_scheduler_id));
    if (data->subscribe_callback) {
      data->subscribe_callback(info, data->subscribe_context);
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    CHECK(callback_data->done_callback == NULL);
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);

  } else {
    LOG_FATAL("Unexpected reply type from actor notification subscribe.");
  }
}

void redis_actor_notification_table_subscribe(
    TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_actor_notification_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE actor_notifications");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context,
                    "error in redis_actor_notification_table_subscribe");
  }
}

void redis_object_info_subscribe_callback(redisAsyncContext *c,
                                          void *r,
                                          void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;

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
          (db_client_table_done_callback) callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
    /* Note that we do not destroy the callback data yet because the
     * subscription callback needs this data. */
    event_loop_remove_timer(db->loop, callback_data->timer_id);
    return;
  }
  /* Otherwise, parse the payload and call the callback. */
  ObjectInfoSubscribeData *data =
      (ObjectInfoSubscribeData *) callback_data->data;
  ObjectID object_id;
  memcpy(object_id.id, payload->str, sizeof(object_id.id));
  /* payload->str should have the format: "ObjectID:object_size_int" */
  LOG_DEBUG("obj:info channel received message <%s>", payload->str);
  if (data->subscribe_callback) {
    data->subscribe_callback(
        object_id, strtol(&payload->str[1 + sizeof(object_id)], NULL, 10),
        data->subscribe_context);
  }
}

void redis_object_info_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->sub_context, redis_object_info_subscribe_callback,
      (void *) callback_data->timer_id, "PSUBSCRIBE obj:info");
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in object_info_register_callback");
  }
}

void redis_push_error_rpush_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;
  /* The reply should be the length of the errors list after our RPUSH. */
  CHECK(reply->type == REDIS_REPLY_INTEGER);
  destroy_timer_callback(db->loop, callback_data);
}

void redis_push_error_hmset_callback(redisAsyncContext *c,
                                     void *r,
                                     void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;

  /* Make sure we were able to add the error information. */
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);

  /* Add the error to this driver's list of errors. */
  ErrorInfo *info = (ErrorInfo *) callback_data->data;
  int status = redisAsyncCommand(db->context, redis_push_error_rpush_callback,
                                 (void *) callback_data->timer_id,
                                 "RPUSH ErrorKeys Error:%b:%b",
                                 info->driver_id.id, sizeof(info->driver_id.id),
                                 info->error_key, sizeof(info->error_key));
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in redis_push_error rpush");
  }
}

void redis_push_error(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  ErrorInfo *info = (ErrorInfo *) callback_data->data;
  CHECK(info->error_index < MAX_ERROR_INDEX && info->error_index >= 0);
  /* Look up the error type. */
  const char *error_type = error_types[info->error_index];
  const char *error_message = error_messages[info->error_index];

  /* Set the error information. */
  int status = redisAsyncCommand(
      db->context, redis_push_error_hmset_callback,
      (void *) callback_data->timer_id,
      "HMSET Error:%b:%b type %s message %s data %b", info->driver_id.id,
      sizeof(info->driver_id.id), info->error_key, sizeof(info->error_key),
      error_type, error_message, info->data, info->data_length);
  if ((status == REDIS_ERR) || db->sub_context->err) {
    LOG_REDIS_DEBUG(db->sub_context, "error in redis_push_error hmset");
  }
}

DBClientID get_db_client_id(DBHandle *db) {
  CHECK(db != NULL);
  return db->client;
}
