#include "redismodule.h"

#include <string.h>

#include "task.h"

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

#define OBJECT_INFO_PREFIX "OI:"
#define OBJECT_LOCATION_PREFIX "OL:"
#define OBJECT_SUBSCRIBE_PREFIX "OS:"
#define TASK_PREFIX "T:"

#define CHECK_ERROR(STATUS, MESSAGE)                   \
  if ((STATUS) == REDISMODULE_ERR) {                   \
    return RedisModule_ReplyWithError(ctx, (MESSAGE)); \
  }

RedisModuleKey *OpenPrefixedString(RedisModuleCtx *ctx,
                                   const char *prefix,
                                   const char *keyname,
                                   size_t keylength,
                                   int mode) {
  RedisModuleString *prefixed_keyname = RedisModule_CreateStringPrintf(
      ctx, "%s%*.*s", prefix, keylength, keylength, keyname);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  RedisModule_FreeString(ctx, prefixed_keyname);
  return key;
}

RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString *keyname,
                                int mode) {
  size_t length;
  const char *value = RedisModule_StringPtrLen(keyname, &length);
  return OpenPrefixedString(ctx, prefix, value, length, mode);
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

  RedisModuleKey *table_key;
  table_key = OpenPrefixedKey(ctx, OBJECT_LOCATION_PREFIX, object_id,
                              REDISMODULE_READ | REDISMODULE_WRITE);

  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(table_key, 0.0, manager, NULL);

  /* Inform subscribers. */
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

  RedisModuleCallReply *reply;
  reply = RedisModule_Call(ctx, "PUBLISH", "ss", object_id, publish);
  RedisModule_FreeString(ctx, publish);
  if (reply == NULL) {
    return RedisModule_ReplyWithError(ctx, "PUBLISH unsuccessful");
  }

  /* Clean up. */
  RedisModule_CloseKey(key);
  RedisModule_CloseKey(table_key);

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

int TaskTableWrite_RedisCommand(RedisModuleCtx *ctx,
                                RedisModuleString *task_arg,
                                bool task_added) {
  size_t length;
  const char *task_string = RedisModule_StringPtrLen(task_arg, &length);
  task *task_instance = (task *) task_string;
  CHECK((size_t) task_size(task_instance) == length);
  task_id id = task_task_id(task_instance);
  scheduling_state state = task_state(task_instance);
  node_id node = task_node(task_instance);
  task_spec *spec = task_task_spec(task_instance);

  RedisModuleKey *key = OpenPrefixedString(ctx, TASK_PREFIX, (const char *) &id,
                                           sizeof(task_id), REDISMODULE_WRITE);

  RedisModuleString *state_string =
      RedisModule_CreateStringPrintf(ctx, "%d", state);
  RedisModuleString *node_string = RedisModule_CreateStringPrintf(
      ctx, "%*.*s", sizeof(node_id), sizeof(node_id), &node);

  if (task_added) {
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state_string,
                        "node", node_string, NULL);
  } else {
    RedisModuleString *spec_string = RedisModule_CreateStringPrintf(
        ctx, "%*.*s", task_spec_size(spec), task_spec_size(spec), spec);
    RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "state", state_string,
                        "node", node_string, "spec", spec_string, NULL);
    RedisModule_FreeString(ctx, spec_string);
  }

  RedisModule_FreeString(ctx, node_string);
  RedisModule_FreeString(ctx, state_string);
  RedisModule_CloseKey(key);

  RedisModule_ReplyWithSimpleString(ctx, "ok");

  return REDISMODULE_OK;
}

/**
 * Add a new entry to the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_add_task <task string>
 *
 * @param task_string A string which is the task instance.
 * @return OK if the operation was successful.
 */
int TaskTableAddTask_RedisCommand(RedisModuleCtx *ctx,
                                  RedisModuleString **argv,
                                  int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *task_arg = argv[1];
  return TaskTableWrite_RedisCommand(ctx, task_arg, false);
}

/**
 * Update an entry in the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_add_task <task string>
 *
 * @param task_string A string which is the task instance.
 * @return OK if the operation was successful.
 */
int TaskTableUpdate_RedisCommand(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleString *task_arg = argv[1];
  return TaskTableWrite_RedisCommand(ctx, task_arg, true);
}

/**
 * Get an entry from the task table.
 *
 * This is called from a client with the command:
 *
 *     RAY.task_table_get_task <task ID>
 *
 * @param task_id A string of the task ID to look up.
 * @return An array of strings representing the task fields in the following
 *         order: 1) scheduling state 2) associated node ID, if any 3) the task
 *         specification, which can be casted to a task_spec. If the task ID is
 *         not in the table, returns nil.
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
    RedisModuleString *state, *node, *task_spec;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "state", &state, "node",
                        &node, "task_spec", &task_spec, NULL);
    if (state == NULL || node == NULL || task_spec == NULL) {
      /* We must have either all fields or no fields. */
      return RedisModule_ReplyWithError(
          ctx, "Missing fields in the task table entry");
    }

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithString(ctx, state);
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

  if (RedisModule_CreateCommand(ctx, "ray.task_table_add_task",
                                TaskTableAddTask_RedisCommand, "write pubsub",
                                0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_update",
                                TaskTableUpdate_RedisCommand, "write pubsub", 0,
                                0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.task_table_get_task",
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
