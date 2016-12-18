#include "redismodule.h"

#include <string.h>

/**
 * Various tables are maintained in redis:
 *
 * == OBJECT TABLE ==
 *
 * This consists of two parts:
 * - The object location table, indexed by OL:object_id, which is the set of
 *   plasma manager indices that have access to the object.
 * - The object info table, indexed by OI:object_id, which is a hashmap with key
 *   "hash" for the hash of the object and key "data_size" for the size of the
 *   object in bytes.
 *
 * == TASK TABLE ==
 *
 * TODO(pcm): Fill this out.
 */

#define DB_CLIENT_PREFIX "CL:"
#define OBJECT_INFO_PREFIX "OI:"
#define OBJECT_LOCATION_PREFIX "OL:"
#define OBJECT_SUBSCRIBE_PREFIX "OS:"
#define TASK_PREFIX "TT:"

#define CHECK_ERROR(STATUS, MESSAGE)                   \
  if ((STATUS) == REDISMODULE_ERR) {                   \
    return RedisModule_ReplyWithError(ctx, (MESSAGE)); \
  }

RedisModuleString *CreatePrefixedString(RedisModuleCtx *ctx,
                                        const char *prefix,
                                        RedisModuleString *keyname) {
  size_t length;
  const char *value = RedisModule_StringPtrLen(keyname, &length);
  RedisModuleString *prefixed_keyname =
      RedisModule_CreateString(ctx, prefix, strlen(prefix));
  /* Using RedisModule_CreateStringPrintf in the past did not handle NULL
   * characters properly. */
  RedisModule_StringAppendBuffer(ctx, prefixed_keyname, value, length);
  return prefixed_keyname;
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode) {
  RedisModuleString *prefixed_keyname =
      CreatePrefixedString(ctx, prefix, keyname);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  RedisModule_FreeString(ctx, prefixed_keyname);
  return key;
}

/**
 * Register a client with Redis. This is called from a client with the command:
 *
 *     RAY.CONNECT <client type> <address> <ray client id> <aux address>
 *
 * @param client_type The type of the client (e.g., plasma_manager).
 * @param address The address of the client.
 * @param ray_client_id The db client ID of the client.
 * @param aux_address An auxiliary address. This is currently just used by the
 *        local scheduler to record the address of the plasma manager that it is
 *        connected to.
 * @return OK if the operation was successful.
 */
int Connect_RedisCommand(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *client_type = argv[1];
  RedisModuleString *address = argv[2];
  RedisModuleString *ray_client_id = argv[3];
  RedisModuleString *aux_address = argv[4];

  /* Add this client to the Ray db client table. */
  RedisModuleKey *db_client_table_key =
      OpenPrefixedKey(ctx, DB_CLIENT_PREFIX, ray_client_id, REDISMODULE_WRITE);
  RedisModule_HashSet(db_client_table_key, REDISMODULE_HASH_CFIELDS,
                      "client_type", client_type, "address", address,
                      "aux_address", aux_address, NULL);
  /* Clean up. */
  RedisModule_CloseKey(db_client_table_key);

  /* Construct strings to publish on the db client channel. */
  RedisModuleString *channel_name =
      RedisModule_CreateString(ctx, "db_clients", strlen("db_clients"));
  RedisModuleString *client_info =
      RedisModule_CreateStringFromString(ctx, ray_client_id);
  RedisModule_StringAppendBuffer(ctx, client_info, ":", strlen(":"));
  /* Append the client type. */
  size_t client_type_size;
  const char *client_type_str =
      RedisModule_StringPtrLen(client_type, &client_type_size);
  RedisModule_StringAppendBuffer(ctx, client_info, client_type_str,
                                 client_type_size);
  /* Append a space. */
  RedisModule_StringAppendBuffer(ctx, client_info, " ", strlen(" "));
  /* Append the aux address. */
  size_t aux_address_size;
  const char *aux_address_str =
      RedisModule_StringPtrLen(aux_address, &aux_address_size);
  RedisModule_StringAppendBuffer(ctx, client_info, aux_address_str,
                                 aux_address_size);
  /* Publish the client info on the db client channel. */
  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel_name, client_info);
  RedisModule_FreeString(ctx, channel_name);
  RedisModule_FreeString(ctx, client_info);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

/**
 * Get the address of a client from its db client ID. This is called from a
 * client with the command:
 *
 *     RAY.GET_CLIENT_ADDRESS <ray client id>
 *
 * @param ray_client_id The db client ID of the client.
 * @return The address of the client if the operation was successful.
 */
int GetClientAddress_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *ray_client_id = argv[1];
  /* Get the request client address from the db client table. */
  RedisModuleKey *db_client_table_key =
      OpenPrefixedKey(ctx, DB_CLIENT_PREFIX, ray_client_id, REDISMODULE_READ);
  if (db_client_table_key == NULL) {
    /* There is no client with this ID. */
    RedisModule_CloseKey(db_client_table_key);
    return RedisModule_ReplyWithError(ctx, "invalid client ID");
  }
  RedisModuleString *address;
  RedisModule_HashGet(db_client_table_key, REDISMODULE_HASH_CFIELDS, "address",
                      &address, NULL);
  if (address == NULL) {
    /* The key did not exist. This should not happen. */
    RedisModule_CloseKey(db_client_table_key);
    return RedisModule_ReplyWithError(
        ctx, "Client does not have an address field. This shouldn't happen.");
  }

  RedisModule_ReplyWithString(ctx, address);

  /* Cleanup. */
  RedisModule_CloseKey(db_client_table_key);
  RedisModule_FreeString(ctx, address);

  return REDISMODULE_OK;
}

/**
 * Lookup an entry in the object table.
 *
 * This is called from a client with the command:
 *
 *     RAY.OBJECT_TABLE_LOOKUP <object id>
 *
 * @param object_id A string representing the object ID.
 * @return A list of plasma manager IDs that are listed in the object table as
 *         having the object.
 */
int ObjectTableLookup_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, argv[1], REDISMODULE_READ);

  int keytype = RedisModule_KeyType(key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithArray(ctx, 0);
  }

  CHECK_ERROR(
      RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
                                        REDISMODULE_POSITIVE_INFINITE, 1, 1),
      "Unable to initialize zset iterator");

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int num_results = 0;
  do {
    RedisModuleString *curr = RedisModule_ZsetRangeCurrentElement(key, NULL);
    RedisModule_ReplyWithString(ctx, curr);
    num_results += 1;
  } while (RedisModule_ZsetRangeNext(key));
  RedisModule_ReplySetArrayLength(ctx, num_results);

  /* Clean up. */
  RedisModule_CloseKey(key);

  return REDISMODULE_OK;
}

/**
 * Add a new entry to the object table or update an existing one.
 *
 * This is called from a client with the command:
 *
 *     RAY.OBJECT_TABLE_ADD <object id> <data size> <hash string> <manager id>
 *
 * @param object_id A string representing the object ID.
 * @param data_size An integer which is the object size in bytes.
 * @param hash_string A string which is a hash of the object.
 * @param manager A string which represents the manager ID of the plasma manager
 *        that has the object.
 * @return OK if the operation was successful and an error with string
 *         "hash mismatch" if the same object_id is already present with a
 *         different hash value.
 */
int ObjectTableAdd_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString **argv,
                                int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *object_id = argv[1];
  RedisModuleString *data_size = argv[2];
  RedisModuleString *new_hash = argv[3];
  RedisModuleString *manager = argv[4];

  long long data_size_value;
  if (RedisModule_StringToLongLong(data_size, &data_size_value) !=
      REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "data_size must be integer");
  }

  /* Set the fields in the object info table. */
  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id,
                        REDISMODULE_READ | REDISMODULE_WRITE);

  int keytype = RedisModule_KeyType(key);
  /* Check if this object was already registered and if the hashes agree. */
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModuleString *existing_hash;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "hash", &existing_hash,
                        NULL);
    if (RedisModule_StringCompare(existing_hash, new_hash) != 0) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(ctx, "hash mismatch");
    }
  }

  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "hash", new_hash, NULL);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "data_size", data_size,
                      NULL);
  RedisModule_CloseKey(key);

  /* Add the location in the object location table. */
  RedisModuleKey *table_key;
  table_key = OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, object_id,
                              REDISMODULE_READ | REDISMODULE_WRITE);

  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(table_key, 0.0, manager, NULL);

  /* Build the PUBLISH topic and message for object table subscribers. The
   * topic is a string in the format "OBJECT_LOCATION_PREFIX:<object ID>". The
   * message is a string in the format: "<manager ID> <manager ID> ... <manager
   * ID>". */
  RedisModuleString *publish_topic =
      CreatePrefixedString(ctx, OBJECT_LOCATION_PREFIX, object_id);
  const char *MANAGERS = "MANAGERS";
  RedisModuleString *publish =
      RedisModule_CreateString(ctx, MANAGERS, strlen(MANAGERS));
  CHECK_ERROR(RedisModule_ZsetFirstInScoreRange(
                  table_key, REDISMODULE_NEGATIVE_INFINITE,
                  REDISMODULE_POSITIVE_INFINITE, 1, 1),
              "Unable to initialize zset iterator");
  do {
    RedisModuleString *curr =
        RedisModule_ZsetRangeCurrentElement(table_key, NULL);
    RedisModule_StringAppendBuffer(ctx, publish, " ", 1);
    size_t size;
    const char *val = RedisModule_StringPtrLen(curr, &size);
    RedisModule_StringAppendBuffer(ctx, publish, val, size);
  } while (RedisModule_ZsetRangeNext(table_key));

  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish);
  RedisModule_FreeString(ctx, publish);
  RedisModule_FreeString(ctx, publish_topic);
  RedisModule_CloseKey(table_key);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

int ObjectTableSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                      RedisModuleString **argv,
                                      int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

int ObjectInfoSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                     RedisModuleString **argv,
                                     int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

/**
 * Add a new entry to the result table or update an existing one.
 *
 * This is called from a client with the command:
 *
 *     RAY.RESULT_TABLE_ADD <object id> <task id>
 *
 * @param object_id A string representing the object ID.
 * @param task_id A string representing the task ID of the task that produced
 *        the object.
 * @return OK if the operation was successful.
 */
int ResultTableAdd_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString **argv,
                                int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  /* Set the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];
  RedisModuleString *task_id = argv[2];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_WRITE);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "task", task_id, NULL);

  /* Clean up. */
  RedisModule_CloseKey(key);
  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/**
 * Lookup an entry in the result table.
 *
 * This is called from a client with the command:
 *
 *     RAY.RESULT_TABLE_LOOKUP <object id>
 *
 * @param object_id A string representing the object ID.
 * @return An empty string if the object ID is not in the result table and the
 *         task ID of the task that created the object ID otherwise.
 */
int ResultTableLookup_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  /* Get the task ID under field "task" in the object info table. */
  RedisModuleString *object_id = argv[1];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ);

  int keytype = RedisModule_KeyType(key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }

  RedisModuleString *task_id;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task", &task_id, NULL);
  if (task_id == NULL) {
    return RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }
  RedisModule_ReplyWithString(ctx, task_id);

  /* Clean up. */
  RedisModule_FreeString(ctx, task_id);
  RedisModule_CloseKey(key);

  return REDISMODULE_OK;
}

int TaskTableWrite(RedisModuleCtx *ctx,
                   RedisModuleString *task_id,
                   RedisModuleString *state,
                   RedisModuleString *node_id,
                   RedisModuleString *task_spec) {
  /* Pad the state integer to a fixed-width integer, and make sure it has width
   * less than or equal to 2. */
  long long state_integer;
  int status = RedisModule_StringToLongLong(state, &state_integer);
  if (status != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "Invalid scheduling state (must be an integer)");
  }
  state = RedisModule_CreateStringPrintf(ctx, "%2d", state_integer);
  size_t length;
  RedisModule_StringPtrLen(state, &length);
  if (length != 2) {
    return RedisModule_ReplyWithError(
        ctx, "Invalid scheduling state width (must have width 2)");
  }

  /* Add the task to the task table. If no spec was provided, get the existing
   * spec out of the task table so we can publish it. */
  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, task_id, REDISMODULE_WRITE);
  if (task_spec == NULL) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state, "node",
                        node_id, NULL);
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "task_spec", &task_spec,
                        NULL);
    if (task_spec == NULL) {
      return RedisModule_ReplyWithError(
          ctx, "Cannot update a task that doesn't exist yet");
    }
  } else {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state, "node",
                        node_id, "task_spec", task_spec, NULL);
  }
  RedisModule_CloseKey(key);

  /* Build the PUBLISH topic and message for task table subscribers. The topic
   * is a string in the format "TASK_PREFIX:<node ID>:<state>". The
   * message is a string in the format: "<task ID> <state> <node ID> <task
   * specification>". */
  RedisModuleString *publish_topic =
      CreatePrefixedString(ctx, TASK_PREFIX, node_id);
  RedisModule_StringAppendBuffer(ctx, publish_topic, ":", strlen(":"));
  const char *state_string = RedisModule_StringPtrLen(state, &length);
  RedisModule_StringAppendBuffer(ctx, publish_topic, state_string, length);
  /* Append the fields to the PUBLISH message. */
  RedisModuleString *publish_message =
      RedisModule_CreateStringFromString(ctx, task_id);
  const char *publish_field;
  /* Append the scheduling state. */
  publish_field = state_string;
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);
  /* Append the node ID. */
  publish_field = RedisModule_StringPtrLen(node_id, &length);
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);
  /* Append the task specification. */
  publish_field = RedisModule_StringPtrLen(task_spec, &length);
  RedisModule_StringAppendBuffer(ctx, publish_message, " ", strlen(" "));
  RedisModule_StringAppendBuffer(ctx, publish_message, publish_field, length);

  RedisModuleCallReply *reply =
      RedisModule_Call(ctx, "PUBLISH", "ss", publish_topic, publish_message);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  RedisModule_FreeString(ctx, publish_message);
  RedisModule_FreeString(ctx, publish_topic);
  RedisModule_ReplyWithSimpleString(ctx, "ok");

  return REDISMODULE_OK;
}

/**
 * Add a new entry to the task table. This will overwrite any existing entry
 * with the same task ID.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_add <task ID> <state> <node ID> <task spec>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance). The string's value must be a
 *        nonnegative integer less than 100, so that it has width at most 2. If
 *        less than 2, the value will be left-padded with spaces to a width of
 *        2.
 * @param node_id A string that is the ID of the associated node, if any.
 * @param task_spec A string that is the specification of the task, which can
 *        be cast to a `task_spec`.
 * @return OK if the operation was successful.
 */
int TaskTableAddTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], argv[4]);
}

/**
 * Update an entry in the task table. This does not update the task
 * specification in the table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_update_task <task ID> <state> <node ID>
 *
 * @param task_id A string that is the ID of the task.
 * @param state A string that is the current scheduling state (a
 *        scheduling_state enum instance). The string's value must be a
 *        nonnegative integer less than 100, so that it has width at most 2. If
 *        less than 2, the value will be left-padded with spaces to a width of
 *        2.
 * @param node_id A string that is the ID of the associated node, if any.
 * @return OK if the operation was successful.
 */
int TaskTableUpdate_RedisCommand(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  return TaskTableWrite(ctx, argv[1], argv[2], argv[3], NULL);
}

/**
 * Get an entry from the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_get <task ID>
 *
 * @param task_id A string of the task ID to look up.
 * @return An array of strings representing the task fields in the following
 *         order: 1) (integer) scheduling state 2) (string) associated node ID,
 *         if any 3) (string) the task specification, which can be casted to a
 *         task_spec. If the task ID is not in the table, returns nil.
 */
int TaskTableGetTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleKey *key =
      OpenPrefixedKey(ctx, TASK_PREFIX, argv[1], REDISMODULE_READ);

  int keytype = RedisModule_KeyType(key);
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    /* If the key exists, look up the fields and return them in an array. */
    RedisModuleString *state = NULL, *node = NULL, *task_spec = NULL;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &state, "node",
                        &node, "task_spec", &task_spec, NULL);
    if (state == NULL || node == NULL || task_spec == NULL) {
      /* We must have either all fields or no fields. */
      return RedisModule_ReplyWithError(
          ctx, "Missing fields in the task table entry");
    }

    size_t state_length;
    const char *state_string = RedisModule_StringPtrLen(state, &state_length);
    int state_integer;
    int scanned = sscanf(state_string, "%2d", &state_integer);
    if (scanned != 1 || state_length != 2) {
      return RedisModule_ReplyWithError(ctx,
                                        "Found invalid scheduling state (must "
                                        "be an integer of width 2");
    }

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithLongLong(ctx, state_integer);
    RedisModule_ReplyWithString(ctx, node);
    RedisModule_ReplyWithString(ctx, task_spec);

    RedisModule_FreeString(ctx, task_spec);
    RedisModule_FreeString(ctx, node);
    RedisModule_FreeString(ctx, state);
  } else {
    /* If the key does not exist, return nil. */
    RedisModule_ReplyWithNull(ctx);
  }

  RedisModule_CloseKey(key);

  return REDISMODULE_OK;
}

int TaskTableSubscribe_RedisCommand(RedisModuleCtx *ctx,
                                    RedisModuleString **argv,
                                    int argc) {
  /* TODO(swang): Implement this. */
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "ray", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.connect", Connect_RedisCommand,
                                "write", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.get_client_address",
                                GetClientAddress_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_lookup",
                                ObjectTableLookup_RedisCommand, "readonly", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_add",
                                ObjectTableAdd_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_subscribe",
                                ObjectTableSubscribe_RedisCommand, "pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_info_subscribe",
                                ObjectInfoSubscribe_RedisCommand, "pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_add",
                                ResultTableAdd_RedisCommand, "write", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_lookup",
                                ResultTableLookup_RedisCommand, "readonly", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_add",
                                TaskTableAddTask_RedisCommand, "write pubsub",
                                0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_update",
                                TaskTableUpdate_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_get",
                                TaskTableGetTask_RedisCommand, "readonly", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_subscribe",
                                TaskTableSubscribe_RedisCommand, "pubsub", 0, 0,
                                0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
