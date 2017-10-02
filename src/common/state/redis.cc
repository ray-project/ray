/* Redis implementation of the global state store */

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

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
#include "driver_table.h"
#include "local_scheduler_table.h"
#include "object_table.h"
#include "task.h"
#include "task_table.h"
#include "error_table.h"
#include "event_loop.h"
#include "redis.h"
#include "io.h"
#include "net.h"

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

redisAsyncContext *get_redis_context(DBHandle *db, UniqueID id) {
  /* NOTE: The hash function used here must match the one in
   * PyObjectID_redis_shard_hash in src/common/lib/python/common_extension.cc.
   * Changes to the hash function should only be made through
   * UniqueIDHasher in src/common/common.h */
  UniqueIDHasher index;
  return db->contexts[index(id) % db->contexts.size()];
}

redisAsyncContext *get_redis_subscribe_context(DBHandle *db, UniqueID id) {
  UniqueIDHasher index;
  return db->subscribe_contexts[index(id) % db->subscribe_contexts.size()];
}

void get_redis_shards(redisContext *context,
                      std::vector<std::string> &db_shards_addresses,
                      std::vector<int> &db_shards_ports) {
  /* Get the total number of Redis shards in the system. */
  int num_attempts = 0;
  redisReply *reply = NULL;
  while (num_attempts < REDIS_DB_CONNECT_RETRIES) {
    /* Try to read the number of Redis shards from the primary shard. If the
     * entry is present, exit. */
    reply = (redisReply *) redisCommand(context, "GET NumRedisShards");
    if (reply->type != REDIS_REPLY_NIL) {
      break;
    }

    /* Sleep for a little, and try again if the entry isn't there yet. */
    freeReplyObject(reply);
    usleep(REDIS_DB_CONNECT_WAIT_MS * 1000);
    num_attempts++;
    continue;
  }
  CHECKM(num_attempts < REDIS_DB_CONNECT_RETRIES,
         "No entry found for NumRedisShards");
  CHECKM(reply->type == REDIS_REPLY_STRING,
         "Expected string, found Redis type %d for NumRedisShards",
         reply->type);
  int num_redis_shards = atoi(reply->str);
  CHECKM(num_redis_shards >= 1, "Expected at least one Redis shard, found %d.",
         num_redis_shards);
  freeReplyObject(reply);

  /* Get the addresses of all of the Redis shards. */
  num_attempts = 0;
  while (num_attempts < REDIS_DB_CONNECT_RETRIES) {
    /* Try to read the Redis shard locations from the primary shard. If we find
     * that all of them are present, exit. */
    reply = (redisReply *) redisCommand(context, "LRANGE RedisShards 0 -1");
    if (reply->elements == num_redis_shards) {
      break;
    }

    /* Sleep for a little, and try again if not all Redis shard addresses have
     * been added yet. */
    freeReplyObject(reply);
    usleep(REDIS_DB_CONNECT_WAIT_MS * 1000);
    num_attempts++;
    continue;
  }
  CHECKM(num_attempts < REDIS_DB_CONNECT_RETRIES,
         "Expected %d Redis shard addresses, found %d", num_redis_shards,
         (int) reply->elements);

  /* Parse the Redis shard addresses. */
  char db_shard_address[16];
  int db_shard_port;
  for (int i = 0; i < reply->elements; ++i) {
    /* Parse the shard addresses and ports. */
    CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
    CHECK(parse_ip_addr_port(reply->element[i]->str, db_shard_address,
                             &db_shard_port) == 0);
    db_shards_addresses.push_back(std::string(db_shard_address));
    db_shards_ports.push_back(db_shard_port);
  }
  freeReplyObject(reply);
}

void db_connect_shard(const std::string &db_address,
                      int db_port,
                      DBClientID client,
                      const char *client_type,
                      const char *node_ip_address,
                      int num_args,
                      const char **args,
                      DBHandle *db,
                      redisAsyncContext **context_out,
                      redisAsyncContext **subscribe_context_out,
                      redisContext **sync_context_out) {
  /* Synchronous connection for initial handshake */
  redisReply *reply;
  int connection_attempts = 0;
  redisContext *sync_context = redisConnect(db_address.c_str(), db_port);
  while (sync_context == NULL || sync_context->err) {
    if (connection_attempts >= REDIS_DB_CONNECT_RETRIES) {
      break;
    }
    LOG_WARN("Failed to connect to Redis, retrying.");
    /* Sleep for a little. */
    usleep(REDIS_DB_CONNECT_WAIT_MS * 1000);
    sync_context = redisConnect(db_address.c_str(), db_port);
    connection_attempts += 1;
  }
  CHECK_REDIS_CONNECT(redisContext, sync_context,
                      "could not establish synchronous connection to redis "
                      "%s:%d",
                      db_address.c_str(), db_port);
  /* Configure Redis to generate keyspace notifications for list events. This
   * should only need to be done once (by whoever started Redis), but since
   * Redis may be started in multiple places (e.g., for testing or when starting
   * processes by hand), it is easier to do it multiple times. */
  reply = (redisReply *) redisCommand(sync_context,
                                      "CONFIG SET notify-keyspace-events Kl");
  CHECKM(reply != NULL, "db_connect failed on CONFIG SET");
  freeReplyObject(reply);
  /* Also configure Redis to not run in protected mode, so clients on other
   * hosts can connect to it. */
  reply =
      (redisReply *) redisCommand(sync_context, "CONFIG SET protected-mode no");
  CHECKM(reply != NULL, "db_connect failed on CONFIG SET");
  freeReplyObject(reply);

  /* Construct the argument arrays for RAY.CONNECT. */
  int argc = num_args + 4;
  const char **argv = (const char **) malloc(sizeof(char *) * argc);
  size_t *argvlen = (size_t *) malloc(sizeof(size_t) * argc);
  /* Set the command name argument. */
  argv[0] = "RAY.CONNECT";
  argvlen[0] = strlen(argv[0]);
  /* Set the client ID argument. */
  argv[1] = (char *) client.id;
  argvlen[1] = sizeof(client.id);
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
  reply = (redisReply *) redisCommandArgv(sync_context, argc, argv, argvlen);
  CHECKM(reply != NULL, "db_connect failed on RAY.CONNECT");
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(strcmp(reply->str, "OK") == 0);
  freeReplyObject(reply);
  free(argv);
  free(argvlen);

  *sync_context_out = sync_context;

  /* Establish connection for control data. */
  redisAsyncContext *context = redisAsyncConnect(db_address.c_str(), db_port);
  CHECK_REDIS_CONNECT(redisAsyncContext, context,
                      "could not establish asynchronous connection to redis "
                      "%s:%d",
                      db_address.c_str(), db_port);
  context->data = (void *) db;
  *context_out = context;

  /* Establish async connection for subscription. */
  redisAsyncContext *subscribe_context =
      redisAsyncConnect(db_address.c_str(), db_port);
  CHECK_REDIS_CONNECT(redisAsyncContext, subscribe_context,
                      "could not establish asynchronous subscription "
                      "connection to redis %s:%d",
                      db_address.c_str(), db_port);
  subscribe_context->data = (void *) db;
  *subscribe_context_out = subscribe_context;
}

DBHandle *db_connect(const std::string &db_primary_address,
                     int db_primary_port,
                     const char *client_type,
                     const char *node_ip_address,
                     int num_args,
                     const char **args) {
  /* Check that the number of args is even. These args will be passed to the
   * RAY.CONNECT Redis command, which takes arguments in pairs. */
  if (num_args % 2 != 0) {
    LOG_FATAL("The number of extra args must be divisible by two.");
  }

  /* Create a client ID for this client. */
  DBClientID client = globally_unique_id();

  DBHandle *db = new DBHandle();

  db->client_type = strdup(client_type);
  db->client = client;

  redisAsyncContext *context;
  redisAsyncContext *subscribe_context;
  redisContext *sync_context;

  /* Connect to the primary redis instance. */
  db_connect_shard(db_primary_address, db_primary_port, client, client_type,
                   node_ip_address, num_args, args, db, &context,
                   &subscribe_context, &sync_context);
  db->context = context;
  db->subscribe_context = subscribe_context;
  db->sync_context = sync_context;

  /* Get the shard locations. */
  std::vector<std::string> db_shards_addresses;
  std::vector<int> db_shards_ports;
  get_redis_shards(db->sync_context, db_shards_addresses, db_shards_ports);
  CHECKM(db_shards_addresses.size() > 0, "No Redis shards found");
  /* Connect to the shards. */
  for (int i = 0; i < db_shards_addresses.size(); ++i) {
    db_connect_shard(db_shards_addresses[i], db_shards_ports[i], client,
                     client_type, node_ip_address, num_args, args, db, &context,
                     &subscribe_context, &sync_context);
    db->contexts.push_back(context);
    db->subscribe_contexts.push_back(subscribe_context);
    redisFree(sync_context);
  }

  return db;
}

void DBHandle_free(DBHandle *db) {
  /* Clean up the primary Redis connection state. */
  redisFree(db->sync_context);
  redisAsyncFree(db->context);
  redisAsyncFree(db->subscribe_context);

  /* Clean up the Redis shards. */
  CHECK(db->contexts.size() == db->subscribe_contexts.size());
  for (int i = 0; i < db->contexts.size(); ++i) {
    redisAsyncFree(db->contexts[i]);
    redisAsyncFree(db->subscribe_contexts[i]);
  }

  /* Clean up memory. */
  for (auto it = db->db_client_cache.begin(); it != db->db_client_cache.end();
       it = db->db_client_cache.erase(it)) {
    free(it->second);
  }

  free(db->client_type);
  delete db;
}

void db_disconnect(DBHandle *db) {
  /* Notify others that this client is disconnecting from Redis. If a client of
   * the same type on the same node wants to reconnect again, they must
   * reconnect and get assigned a different client ID. */
  redisReply *reply =
      (redisReply *) redisCommand(db->sync_context, "RAY.DISCONNECT %b",
                                  db->client.id, sizeof(db->client.id));
  CHECK(strcmp(reply->str, "OK") == 0);
  freeReplyObject(reply);

  DBHandle_free(db);
}

void db_attach(DBHandle *db, event_loop *loop, bool reattach) {
  db->loop = loop;
  /* Attach primary redis instance to the event loop. */
  int err = redisAeAttach(loop, db->context);
  /* If the database is reattached in the tests, redis normally gives
   * an error which we can safely ignore. */
  if (!reattach) {
    CHECKM(err == REDIS_OK, "failed to attach the event loop");
  }
  err = redisAeAttach(loop, db->subscribe_context);
  if (!reattach) {
    CHECKM(err == REDIS_OK, "failed to attach the event loop");
  }
  /* Attach other redis shards to the event loop. */
  CHECK(db->contexts.size() == db->subscribe_contexts.size());
  for (int i = 0; i < db->contexts.size(); ++i) {
    int err = redisAeAttach(loop, db->contexts[i]);
    /* If the database is reattached in the tests, redis normally gives
     * an error which we can safely ignore. */
    if (!reattach) {
      CHECKM(err == REDIS_OK, "failed to attach the event loop");
    }
    err = redisAeAttach(loop, db->subscribe_contexts[i]);
    if (!reattach) {
      CHECKM(err == REDIS_OK, "failed to attach the event loop");
    }
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

  redisAsyncContext *context = get_redis_context(db, obj_id);

  int status = redisAsyncCommand(
      context, redis_object_table_add_callback,
      (void *) callback_data->timer_id, "RAY.OBJECT_TABLE_ADD %b %lld %b %b",
      obj_id.id, sizeof(obj_id.id), (long long) object_size, digest,
      (size_t) DIGEST_SIZE, db->client.id, sizeof(db->client.id));

  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_object_table_add");
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

  redisAsyncContext *context = get_redis_context(db, obj_id);

  int status = redisAsyncCommand(
      context, redis_object_table_remove_callback,
      (void *) callback_data->timer_id, "RAY.OBJECT_TABLE_REMOVE %b %b",
      obj_id.id, sizeof(obj_id.id), client_id->id, sizeof(client_id->id));

  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_object_table_remove");
  }
}

void redis_object_table_lookup(TableCallbackData *callback_data) {
  CHECK(callback_data);
  DBHandle *db = callback_data->db_handle;

  ObjectID obj_id = callback_data->id;

  redisAsyncContext *context = get_redis_context(db, obj_id);

  int status = redisAsyncCommand(context, redis_object_table_lookup_callback,
                                 (void *) callback_data->timer_id,
                                 "RAY.OBJECT_TABLE_LOOKUP %b", obj_id.id,
                                 sizeof(obj_id.id));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in object_table lookup");
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

  redisAsyncContext *context = get_redis_context(db, id);

  /* Add the result entry to the result table. */
  int status = redisAsyncCommand(
      context, redis_result_table_add_callback,
      (void *) callback_data->timer_id, "RAY.RESULT_TABLE_ADD %b %b %d", id.id,
      sizeof(id.id), info->task_id.id, sizeof(info->task_id.id), is_put);
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "Error in result table add");
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
  redisAsyncContext *context = get_redis_context(db, id);
  int status =
      redisAsyncCommand(context, redis_result_table_lookup_callback,
                        (void *) callback_data->timer_id,
                        "RAY.RESULT_TABLE_LOOKUP %b", id.id, sizeof(id.id));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "Error in result table lookup");
  }
}

/**
 * Get an entry from the plasma manager table in redis.
 *
 * @param db The database handle.
 * @param index The index of the plasma manager.
 * @return The IP address and port of the manager.
 */
const std::string redis_get_cached_db_client(DBHandle *db,
                                             DBClientID db_client_id) {
  auto it = db->db_client_cache.find(db_client_id);

  char *manager;
  if (it == db->db_client_cache.end()) {
    /* This is a very rare case. It should happen at most once per db client. */
    redisReply *reply = (redisReply *) redisCommand(
        db->sync_context, "RAY.GET_CLIENT_ADDRESS %b", (char *) db_client_id.id,
        sizeof(db_client_id.id));
    CHECKM(reply->type == REDIS_REPLY_STRING, "REDIS reply type=%d, str=%s",
           reply->type, reply->str);
    char *addr = strdup(reply->str);
    freeReplyObject(reply);
    db->db_client_cache[db_client_id] = addr;
    manager = addr;
  } else {
    manager = it->second;
  }
  std::string manager_address(manager);
  return manager_address;
}

const std::vector<std::string> redis_get_cached_db_clients(
    DBHandle *db,
    const std::vector<DBClientID> &manager_ids) {
  /* We time this function because in the past this loop has taken multiple
   * seconds under stressful situations on hundreds of machines causing the
   * plasma manager to die (because it went too long without sending
   * heartbeats). */
  int64_t start_time = current_time_ms();

  /* Construct the manager vector from the flatbuffers object. */
  std::vector<std::string> manager_vector;

  for (auto const &manager_id : manager_ids) {
    const std::string manager_address =
        redis_get_cached_db_client(db, manager_id);
    manager_vector.push_back(manager_address);
  }

  int64_t end_time = current_time_ms();
  int64_t max_time_for_loop = 1000;
  if (end_time - start_time > max_time_for_loop) {
    LOG_WARN(
        "calling redis_get_cached_db_client in a loop in with %zu manager IDs "
        "took %" PRId64 " milliseconds.",
        manager_ids.size(), end_time - start_time);
  }

  return manager_vector;
}

void redis_object_table_lookup_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;
  LOG_DEBUG("Object table lookup callback");
  CHECK(reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ARRAY);

  object_table_lookup_done_callback done_callback =
      (object_table_lookup_done_callback) callback_data->done_callback;

  ObjectID obj_id = callback_data->id;

  /* Parse the Redis reply. */
  if (reply->type == REDIS_REPLY_NIL) {
    /* The object entry did not exist. */
    if (done_callback) {
      done_callback(obj_id, true, std::vector<std::string>(),
                    callback_data->user_context);
    }
  } else if (reply->type == REDIS_REPLY_ARRAY) {
    /* Extract the manager IDs from the response into a vector. */
    std::vector<DBClientID> manager_ids;

    for (int j = 0; j < reply->elements; ++j) {
      CHECK(reply->element[j]->type == REDIS_REPLY_STRING);
      DBClientID manager_id;
      memcpy(manager_id.id, reply->element[j]->str, sizeof(manager_id.id));
      manager_ids.push_back(manager_id);
    }

    const std::vector<std::string> manager_vector =
        redis_get_cached_db_clients(db, manager_ids);

    if (done_callback) {
      done_callback(obj_id, false, manager_vector, callback_data->user_context);
    }
  } else {
    LOG_FATAL("Unexpected reply type from object table lookup.");
  }

  /* Clean up timer and callback. */
  destroy_timer_callback(callback_data->db_handle->loop, callback_data);
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

    /* Extract the manager IDs from the response into a vector. */
    std::vector<DBClientID> manager_ids;
    for (int i = 0; i < manager_count; ++i) {
      DBClientID manager_id = from_flatbuf(message->manager_ids()->Get(i));
      manager_ids.push_back(manager_id);
    }

    const std::vector<std::string> manager_vector =
        redis_get_cached_db_clients(db, manager_ids);

    /* Call the subscribe callback. */
    ObjectTableSubscribeData *data =
        (ObjectTableSubscribeData *) callback_data->data;
    if (data->object_available_callback) {
      data->object_available_callback(obj_id, data_size, manager_vector,
                                      data->subscribe_context);
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    /* Call the done callback if there is one. This code path should only be
     * used in the tests. */
    if (callback_data->done_callback != NULL) {
      object_table_lookup_done_callback done_callback =
          (object_table_lookup_done_callback) callback_data->done_callback;
      done_callback(NIL_ID, false, std::vector<std::string>(),
                    callback_data->user_context);
    }
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    remove_timer_callback(callback_data->db_handle->loop, callback_data);
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
  for (int i = 0; i < db->subscribe_contexts.size(); ++i) {
    int status = REDIS_OK;
    /* Subscribe to notifications from the object table. This uses the client ID
     * as the channel name so this channel is specific to this client.
     * TODO(rkn):
     * The channel name should probably be the client ID with some prefix. */
    CHECKM(callback_data->data != NULL,
           "Object table subscribe data passed as NULL.");
    if (((ObjectTableSubscribeData *) (callback_data->data))->subscribe_all) {
      /* Subscribe to the object broadcast channel. */
      status = redisAsyncCommand(
          db->subscribe_contexts[i],
          object_table_redis_subscribe_to_notifications_callback,
          (void *) callback_data->timer_id, "SUBSCRIBE %s%s",
          object_channel_prefix, object_channel_bcast);
    } else {
      status = redisAsyncCommand(
          db->subscribe_contexts[i],
          object_table_redis_subscribe_to_notifications_callback,
          (void *) callback_data->timer_id, "SUBSCRIBE %s%b",
          object_channel_prefix, db->client.id, sizeof(db->client.id));
    }

    if ((status == REDIS_ERR) || db->subscribe_contexts[i]->err) {
      LOG_REDIS_DEBUG(db->subscribe_contexts[i],
                      "error in redis_object_table_subscribe_to_notifications");
    }
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

  for (int i = 0; i < num_object_ids; ++i) {
    redisAsyncContext *context = get_redis_context(db, object_ids[i]);

    /* Create the arguments for the Redis command. */
    int num_args = 1 + 1 + 1;
    const char **argv = (const char **) malloc(sizeof(char *) * num_args);
    size_t *argvlen = (size_t *) malloc(sizeof(size_t) * num_args);
    /* Set the command name argument. */
    argv[0] = "RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS";
    argvlen[0] = strlen(argv[0]);
    /* Set the client ID argument. */
    argv[1] = (char *) db->client.id;
    argvlen[1] = sizeof(db->client.id);
    /* Set the object ID arguments. */
    argv[2] = (char *) object_ids[i].id;
    argvlen[2] = sizeof(object_ids[i].id);

    int status = redisAsyncCommandArgv(
        context, redis_object_table_request_notifications_callback,
        (void *) callback_data->timer_id, num_args, argv, argvlen);
    free(argv);
    free(argvlen);

    if ((status == REDIS_ERR) || context->err) {
      LOG_REDIS_DEBUG(context,
                      "error in redis_object_table_subscribe_to_notifications");
    }
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

  redisAsyncContext *context = get_redis_context(db, task_id);

  int status = redisAsyncCommand(context, redis_task_table_get_task_callback,
                                 (void *) callback_data->timer_id,
                                 "RAY.TASK_TABLE_GET %b", task_id.id,
                                 sizeof(task_id.id));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_task_table_get_task");
  }
}

void redis_task_table_add_task_callback(redisAsyncContext *c,
                                        void *r,
                                        void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  // If no subscribers received the message, call the failure callback. The
  // caller should decide whether to retry the add. NOTE(swang): The caller
  // should check whether the receiving subscriber is still alive in the
  // db_client table before retrying the add.
  if (reply->type == REDIS_REPLY_ERROR &&
      strcmp(reply->str, "No subscribers received message.") == 0) {
    LOG_WARN("No subscribers received the task_table_add message.");
    if (callback_data->retry.fail_callback != NULL) {
      callback_data->retry.fail_callback(
          callback_data->id, callback_data->user_context, callback_data->data);
    }
  } else {
    CHECKM(strcmp(reply->str, "OK") == 0, "reply->str is %s", reply->str);
    /* Call the done callback if there is one. */
    if (callback_data->done_callback != NULL) {
      task_table_done_callback done_callback =
          (task_table_done_callback) callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
  }

  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_add_task(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  Task *task = (Task *) callback_data->data;
  TaskID task_id = Task_task_id(task);
  DBClientID local_scheduler_id = Task_local_scheduler(task);
  redisAsyncContext *context = get_redis_context(db, task_id);
  int state = Task_state(task);
  TaskSpec *spec = Task_task_spec(task);

  CHECKM(task != NULL, "NULL task passed to redis_task_table_add_task.");
  int status = redisAsyncCommand(
      context, redis_task_table_add_task_callback,
      (void *) callback_data->timer_id, "RAY.TASK_TABLE_ADD %b %d %b %b",
      task_id.id, sizeof(task_id.id), state, local_scheduler_id.id,
      sizeof(local_scheduler_id.id), spec, Task_task_spec_size(task));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_task_table_add_task");
  }
}

void redis_task_table_update_callback(redisAsyncContext *c,
                                      void *r,
                                      void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  // If no subscribers received the message, call the failure callback. The
  // caller should decide whether to retry the update. NOTE(swang): Retrying a
  // task table update can race with the liveness monitor. Do not retry the
  // update unless the caller is sure that the receiving subscriber is still
  // alive in the db_client table.
  if (reply->type == REDIS_REPLY_ERROR &&
      strcmp(reply->str, "No subscribers received message.") == 0) {
    LOG_WARN("No subscribers received the task_table_update message.");
    if (callback_data->retry.fail_callback != NULL) {
      callback_data->retry.fail_callback(
          callback_data->id, callback_data->user_context, callback_data->data);
    }
  } else {
    CHECKM(strcmp(reply->str, "OK") == 0, "reply->str is %s", reply->str);

    /* Call the done callback if there is one. */
    if (callback_data->done_callback != NULL) {
      task_table_done_callback done_callback =
          (task_table_done_callback) callback_data->done_callback;
      done_callback(callback_data->id, callback_data->user_context);
    }
  }

  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_task_table_update(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  Task *task = (Task *) callback_data->data;
  TaskID task_id = Task_task_id(task);
  redisAsyncContext *context = get_redis_context(db, task_id);
  DBClientID local_scheduler_id = Task_local_scheduler(task);
  int state = Task_state(task);

  CHECKM(task != NULL, "NULL task passed to redis_task_table_update.");
  int status = redisAsyncCommand(
      context, redis_task_table_update_callback,
      (void *) callback_data->timer_id, "RAY.TASK_TABLE_UPDATE %b %d %b",
      task_id.id, sizeof(task_id.id), state, local_scheduler_id.id,
      sizeof(local_scheduler_id.id));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_task_table_update");
  }
}

void redis_task_table_test_and_update_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);
  redisReply *reply = (redisReply *) r;
  /* Parse the task from the reply. */
  Task *task = parse_and_construct_task_from_redis_reply(reply);
  if (task == NULL) {
    /* A NULL task means that the task was not in the task table. NOTE(swang):
     * For normal tasks, this is not expected behavior, but actor tasks may be
     * delayed when added to the task table if they are submitted to a local
     * scheduler before it receives the notification that maps the actor to a
     * local scheduler. */
    LOG_ERROR("No task found during task_table_test_and_update");
    return;
  }
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
  redisAsyncContext *context = get_redis_context(db, task_id);
  TaskTableTestAndUpdateData *update_data =
      (TaskTableTestAndUpdateData *) callback_data->data;

  int status = redisAsyncCommand(
      context, redis_task_table_test_and_update_callback,
      (void *) callback_data->timer_id,
      "RAY.TASK_TABLE_TEST_AND_UPDATE %b %d %d %b", task_id.id,
      sizeof(task_id.id), update_data->test_state_bitmask,
      update_data->update_state, update_data->local_scheduler_id.id,
      sizeof(update_data->local_scheduler_id.id));
  if ((status == REDIS_ERR) || context->err) {
    LOG_REDIS_DEBUG(context, "error in redis_task_table_test_and_update");
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
    remove_timer_callback(db->loop, callback_data);
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
  for (auto subscribe_context : db->subscribe_contexts) {
    int status;
    if (IS_NIL_ID(data->local_scheduler_id)) {
      /* TODO(swang): Implement the state_filter by translating the bitmask into
       * a Redis key-matching pattern. */
      status = redisAsyncCommand(
          subscribe_context, redis_task_table_subscribe_callback,
          (void *) callback_data->timer_id, "PSUBSCRIBE %s*:%d",
          TASK_CHANNEL_PREFIX, data->state_filter);
    } else {
      DBClientID local_scheduler_id = data->local_scheduler_id;
      status = redisAsyncCommand(
          subscribe_context, redis_task_table_subscribe_callback,
          (void *) callback_data->timer_id, "SUBSCRIBE %s%b:%d",
          TASK_CHANNEL_PREFIX, (char *) local_scheduler_id.id,
          sizeof(local_scheduler_id.id), data->state_filter);
    }
    if ((status == REDIS_ERR) || subscribe_context->err) {
      LOG_REDIS_DEBUG(subscribe_context, "error in redis_task_table_subscribe");
    }
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

void redis_db_client_table_scan(DBHandle *db,
                                std::vector<DBClient> &db_clients) {
  /* TODO(swang): Integrate this functionality with the Ray Redis module. To do
   * this, we need the KEYS or SCAN command in Redis modules. */
  /* Get all the database client keys. */
  redisReply *reply = (redisReply *) redisCommand(db->sync_context, "KEYS %s*",
                                                  DB_CLIENT_PREFIX);
  if (reply->type == REDIS_REPLY_NIL) {
    return;
  }
  /* Get all the database client information. */
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  for (int i = 0; i < reply->elements; ++i) {
    redisReply *client_reply = (redisReply *) redisCommand(
        db->sync_context, "HGETALL %b", reply->element[i]->str,
        reply->element[i]->len);
    CHECK(reply->type == REDIS_REPLY_ARRAY);
    CHECK(reply->elements > 0);
    DBClient db_client;
    memset(&db_client, 0, sizeof(db_client));
    int num_fields = 0;
    /* Parse the fields into a DBClient. */
    for (int j = 0; j < client_reply->elements; j = j + 2) {
      const char *key = client_reply->element[j]->str;
      const char *value = client_reply->element[j + 1]->str;
      if (strcmp(key, "ray_client_id") == 0) {
        memcpy(db_client.id.id, value, sizeof(db_client.id));
        num_fields++;
      } else if (strcmp(key, "client_type") == 0) {
        db_client.client_type = strdup(value);
        num_fields++;
      } else if (strcmp(key, "aux_address") == 0) {
        db_client.aux_address = strdup(value);
        num_fields++;
      } else if (strcmp(key, "deleted") == 0) {
        bool is_deleted = atoi(value);
        db_client.is_insertion = !is_deleted;
        num_fields++;
      }
    }
    freeReplyObject(client_reply);
    /* The client ID, type, and whether it is deleted are all mandatory fields.
     * Auxiliary address is optional. */
    CHECK(num_fields >= 3);
    db_clients.push_back(db_client);
  }
  freeReplyObject(reply);
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
    remove_timer_callback(db->loop, callback_data);

    /* Get the current db client table entries, in case we missed notifications
     * before the initial subscription. This must be done before we process any
     * notifications from the subscription channel, so that we don't readd an
     * entry that has already been deleted. */
    std::vector<DBClient> db_clients;
    redis_db_client_table_scan(db, db_clients);
    /* Call the subscription callback for all entries that we missed. */
    DBClientTableSubscribeData *data =
        (DBClientTableSubscribeData *) callback_data->data;
    for (auto db_client : db_clients) {
      data->subscribe_callback(&db_client, data->subscribe_context);
      if (db_client.client_type != NULL) {
        free((void *) db_client.client_type);
      }
      if (db_client.aux_address != NULL) {
        free((void *) db_client.aux_address);
      }
    }
    return;
  }
  /* Otherwise, parse the payload and call the callback. */
  auto message =
      flatbuffers::GetRoot<SubscribeToDBClientTableReply>(payload->str);

  /* Parse the client type and auxiliary address from the response. If there is
   * only client type, then the update was a delete. */
  DBClient db_client;
  db_client.id = from_flatbuf(message->db_client_id());
  db_client.client_type = (char *) message->client_type()->data();
  db_client.aux_address = message->aux_address()->data();
  db_client.is_insertion = message->is_insertion();

  /* Call the subscription callback. */
  DBClientTableSubscribeData *data =
      (DBClientTableSubscribeData *) callback_data->data;
  if (data->subscribe_callback) {
    data->subscribe_callback(&db_client, data->subscribe_context);
  }
}

void redis_db_client_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->subscribe_context, redis_db_client_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE db_clients");
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context,
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
    memset(&info, 0, sizeof(info));
    if (message->is_dead()) {
      /* If the local scheduler is dead, then ignore all other fields in the
       * message. */
      info.is_dead = true;
    } else {
      /* If the local scheduler is alive, collect load information. */
      info.total_num_workers = message->total_num_workers();
      info.task_queue_length = message->task_queue_length();
      info.available_workers = message->available_workers();
      for (int i = 0; i < ResourceIndex_MAX; ++i) {
        info.static_resources[i] = message->static_resources()->Get(i);
      }
      for (int i = 0; i < ResourceIndex_MAX; ++i) {
        info.dynamic_resources[i] = message->dynamic_resources()->Get(i);
      }
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
    remove_timer_callback(db->loop, callback_data);

  } else {
    LOG_FATAL("Unexpected reply type from local scheduler subscribe.");
  }
}

void redis_local_scheduler_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->subscribe_context, redis_local_scheduler_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE local_schedulers");
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context,
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
      fbb.CreateVector(info.dynamic_resources, ResourceIndex_MAX), false);
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

void redis_local_scheduler_table_disconnect(DBHandle *db) {
  flatbuffers::FlatBufferBuilder fbb;
  /* Create the flatbuffers message. */
  double empty_array[] = {};
  /* Most of the flatbuffer message fields don't matter here. Only the
   * db_client_id and the is_dead field matter. */
  auto message = CreateLocalSchedulerInfoMessage(
      fbb, to_flatbuf(fbb, db->client), 0, 0, 0,
      fbb.CreateVector(empty_array, 0), fbb.CreateVector(empty_array, 0), true);
  fbb.Finish(message);

  redisReply *reply = (redisReply *) redisCommand(
      db->sync_context, "PUBLISH local_schedulers %b", fbb.GetBufferPointer(),
      fbb.GetSize());
  CHECK(reply->type != REDIS_REPLY_ERROR);
  CHECK(reply->type == REDIS_REPLY_INTEGER);
  LOG_DEBUG("%" PRId64 " subscribers received this publish.\n", reply->integer);
  freeReplyObject(reply);
}

void redis_driver_table_subscribe_callback(redisAsyncContext *c,
                                           void *r,
                                           void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_ARRAY);
  CHECK(reply->elements == 3);
  redisReply *message_type = reply->element[0];
  LOG_DEBUG("Driver table subscribe callback, message %s", message_type->str);

  if (strcmp(message_type->str, "message") == 0) {
    /* Handle a driver heartbeat. Parse the payload and call the subscribe
     * callback. */
    auto message =
        flatbuffers::GetRoot<DriverTableMessage>(reply->element[2]->str);
    /* Extract the client ID. */
    WorkerID driver_id = from_flatbuf(message->driver_id());

    /* Call the subscribe callback. */
    DriverTableSubscribeData *data =
        (DriverTableSubscribeData *) callback_data->data;
    if (data->subscribe_callback) {
      data->subscribe_callback(driver_id, data->subscribe_context);
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    CHECK(callback_data->done_callback == NULL);
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    remove_timer_callback(db->loop, callback_data);

  } else {
    LOG_FATAL("Unexpected reply type from driver subscribe.");
  }
}

void redis_driver_table_subscribe(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->subscribe_context, redis_driver_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE driver_deaths");
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context,
                    "error in redis_driver_table_subscribe");
  }
}

void redis_driver_table_send_driver_death_callback(redisAsyncContext *c,
                                                   void *r,
                                                   void *privdata) {
  REDIS_CALLBACK_HEADER(db, callback_data, r);

  redisReply *reply = (redisReply *) r;
  CHECK(reply->type == REDIS_REPLY_INTEGER);
  LOG_DEBUG("%" PRId64 " subscribers received this publish.\n", reply->integer);
  /* At the very least, the local scheduler that publishes this message should
   * also receive it. */
  CHECK(reply->integer >= 1);

  CHECK(callback_data->done_callback == NULL);
  /* Clean up the timer and callback. */
  destroy_timer_callback(db->loop, callback_data);
}

void redis_driver_table_send_driver_death(TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  WorkerID driver_id = callback_data->id;

  /* Create a flatbuffer object to serialize and publish. */
  flatbuffers::FlatBufferBuilder fbb;
  /* Create the flatbuffers message. */
  auto message = CreateDriverTableMessage(fbb, to_flatbuf(fbb, driver_id));
  fbb.Finish(message);

  int status = redisAsyncCommand(
      db->context, redis_driver_table_send_driver_death_callback,
      (void *) callback_data->timer_id, "PUBLISH driver_deaths %b",
      fbb.GetBufferPointer(), fbb.GetSize());
  if ((status == REDIS_ERR) || db->context->err) {
    LOG_REDIS_DEBUG(db->context,
                    "error in redis_driver_table_send_driver_death");
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
    /* The payload should be the concatenation of three IDs. */
    ActorID actor_id;
    WorkerID driver_id;
    DBClientID local_scheduler_id;
    bool reconstruct;
    CHECK(sizeof(actor_id) + sizeof(driver_id) + sizeof(local_scheduler_id) +
              1 ==
          payload->len);
    char *current_ptr = payload->str;
    /* Parse the actor ID. */
    memcpy(&actor_id, current_ptr, sizeof(actor_id));
    current_ptr += sizeof(actor_id);
    /* Parse the driver ID. */
    memcpy(&driver_id, current_ptr, sizeof(driver_id));
    current_ptr += sizeof(driver_id);
    /* Parse the local scheduler ID. */
    memcpy(&local_scheduler_id, current_ptr, sizeof(local_scheduler_id));
    current_ptr += sizeof(local_scheduler_id);
    /* Parse the reconstruct bit. */
    if (*current_ptr == '1') {
      reconstruct = true;
    } else if (*current_ptr == '0') {
      reconstruct = false;
    } else {
      LOG_FATAL("This code should be unreachable.");
    }
    current_ptr += 1;

    if (data->subscribe_callback) {
      data->subscribe_callback(actor_id, driver_id, local_scheduler_id,
                               reconstruct, data->subscribe_context);
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
    /* The reply for the initial SUBSCRIBE command. */
    CHECK(callback_data->done_callback == NULL);
    /* If the initial SUBSCRIBE was successful, clean up the timer, but don't
     * destroy the callback data. */
    remove_timer_callback(db->loop, callback_data);

  } else {
    LOG_FATAL("Unexpected reply type from actor notification subscribe.");
  }
}

void redis_actor_notification_table_subscribe(
    TableCallbackData *callback_data) {
  DBHandle *db = callback_data->db_handle;
  int status = redisAsyncCommand(
      db->subscribe_context, redis_actor_notification_table_subscribe_callback,
      (void *) callback_data->timer_id, "SUBSCRIBE actor_notifications");
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context,
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
    remove_timer_callback(db->loop, callback_data);
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
      db->subscribe_context, redis_object_info_subscribe_callback,
      (void *) callback_data->timer_id, "PSUBSCRIBE obj:info");
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context,
                    "error in object_info_register_callback");
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
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context, "error in redis_push_error rpush");
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
  if ((status == REDIS_ERR) || db->subscribe_context->err) {
    LOG_REDIS_DEBUG(db->subscribe_context, "error in redis_push_error hmset");
  }
}

DBClientID get_db_client_id(DBHandle *db) {
  CHECK(db != NULL);
  return db->client;
}
